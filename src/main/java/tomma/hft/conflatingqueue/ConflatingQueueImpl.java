package tomma.hft.conflatingqueue;

import sun.misc.Contended;
import util.Logger;

import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ConflatingQueueImpl<K, V> implements ConflatingQueue<K, V> {

    @Contended
    private final Map<K, Entry<K, QueueValue<V>>> entryKeyMap;
    private final Deque<Entry<K, QueueValue<V>>> deque;
    private final int capacity;
    private final int keyCnt;

    @Contended
    private volatile QueueValue<V> priceValueOffer = new QueueValue<>();

    @Contended
    private volatile QueueValue<V> priceValueTake = new QueueValue<>();

    // Pool for reusable entries and QueueValues
    private final EntryPool<K, V> entryPool;

    // Atomic counter for size handling
    private final AtomicInteger size = new AtomicInteger(0);

    ConflatingQueueImpl(int keyCnt, int capacity) {
        this.capacity = capacity;
        this.keyCnt = keyCnt;
        entryKeyMap = new ConcurrentHashMap<>(keyCnt);
        deque = new ConcurrentLinkedDeque<>();
        // Initialize pools
        this.entryPool = new EntryPool<>(keyCnt * 5);
    }

    @Override
    public boolean offer(final KeyValue<K, V> keyValue) {
        try {
            Objects.requireNonNull(keyValue);
            Objects.requireNonNull(keyValue.getKey());
            Objects.requireNonNull(keyValue.getValue());

            K conflationKey = keyValue.getKey();
            V value = keyValue.getValue();

            final Entry<K, QueueValue<V>> entry = entryKeyMap.computeIfAbsent(conflationKey, k -> this.entryPool.getOrCreateEntry(k));
            final QueueValue<V> newValue = priceValueOffer.initializeWithUnconfirmed(value);
            final QueueValue<V> oldValue = entry.priceValue.getAndSet(newValue);

            if (oldValue.isNotInQueue()) {
                newValue.confirm();
                addToDeque(entry);
            } else {
                newValue.confirmWith(value);
            }

            priceValueOffer = oldValue; // Reuse old value
        } catch (NullPointerException npe) {
            Logger.error("NullPointerException in offer method", npe);
            throw npe;
        }
        return true;
    }

    @Override
    public KeyValue<K, V> take() throws InterruptedException {
        try {
            Entry<K, QueueValue<V>> entry;
            do {
                if (Thread.interrupted())
                    throw new InterruptedException();
                entry = pollFromDeque();
            } while (entry == null);

            final QueueValue<V> polledValue = entry.priceValue.get();
            final V value = polledValue.awaitAndRelease();
            final K key = entry.getKey();

            return new QueueKeyValue<>(key, value);
        } catch (RuntimeException e) {
            throw e;
        }
    }

    @Override
    public boolean isEmpty() {
        return deque.isEmpty();
    }

    private void addToDeque(Entry<K, QueueValue<V>> entry) {
        deque.add(entry);
        size.incrementAndGet();
    }

    private Entry<K, QueueValue<V>> pollFromDeque() {
        if (isEmpty()) {
            return null;
        }
        Entry<K, QueueValue<V>> entry = deque.poll();
        size.decrementAndGet();
        return entry;
    }

    public Map<K, Entry<K, QueueValue<V>>> getEntryKeyMap() {
        return entryKeyMap;
    }

    public Deque<Entry<K, QueueValue<V>>> getDeque() {
        return deque;
    }

    public int getSize() {
        return size.get();
    }

    static class QueueValue<V> {
        enum State { UNUSED, UNCONFIRMED, CONFIRMED }

        private V value;
        private final AtomicReference<State> state;

        public QueueValue() {
            this.state = new AtomicReference<>(State.UNUSED);
        }

        QueueValue<V> initializeWithUnconfirmed(final V value) {
            this.value = Objects.requireNonNull(value);
            state.set(State.UNCONFIRMED);  // Set state atomically
            return this;
        }

        QueueValue<V> initializeWithUnused(final V value) {
            this.value = value;  // nulls allowed here
            state.set(State.UNUSED);  // Set state atomically
            return this;
        }

        void confirmWith(final V value) {
            this.value = value;
            state.set(State.CONFIRMED);  // Set state atomically
        }

        boolean isNotInQueue() {
            return state.get() == State.UNUSED;  // Atomic state check
        }

        void confirm() {
            state.set(State.CONFIRMED);  // Set state atomically
        }

        V awaitAndRelease() {
            awaitFinalState();
            return release();
        }

        State awaitFinalState() {
            // Busy-wait until the state transitions to CONFIRMED
            while (state.get() == State.UNCONFIRMED) {
                // Optionally, add a small sleep to reduce CPU load if necessary
            }
            return state.get();
        }

        V release() {
            final V released = value;
            state.set(State.UNUSED);  // Transition to UNUSED atomically
            this.value = null;
            return released;
        }
    }
    // Pool for reusable Entry objects to avoid GC
    static class EntryPool<K, V> {
        private final Entry<K, QueueValue<V>>[] pool;
        private AtomicInteger index = new AtomicInteger(0);

        @SuppressWarnings("unchecked")
        EntryPool(int size) {
            pool = new Entry[size];
            for (int i = 0; i < size; i++) {
                pool[i] = new Entry<>((K) new Object(), new QueueValue<>());
            }
        }

        Entry<K, QueueValue<V>> getOrCreateEntry(K key) {
            int poolIndex = getNextIndex() % pool.length;
            Entry<K, QueueValue<V>> entry = pool[poolIndex];
            if (entry == null) {
                entry = new Entry<>(key, new QueueValue<V>());
                pool[poolIndex] = entry;
            } else {
                entry.setKey(key);  // Reset the key for reuse
            }
            return entry;
        }

        private int getNextIndex() {
            while (true) {
                int current = index.get();
                int next = current >= Integer.MAX_VALUE - pool.length ? 0 : current + 1;
                if (index.compareAndSet(current, next)) {
                    return current;
                }
            }
        }
    }
}