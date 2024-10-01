package tomma.hft.conflatingqueue;


import sun.misc.Contended;
import util.Logger;

import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class ConflatingQueueImplAi<K, V> implements ConflatingQueue<K, V> {

    @Contended
    private final Map<K, Entry<K, QueueValue<V>>> entryKeyMap; // Stores entries with their queue values
    private final Map<K, Boolean> inDequeMap; // Tracks if a key is in the deque
    private final Map<K, QueueValue<V>> valueMap; // Stores the values separately
    private final Deque<Entry<K, QueueValue<V>>> deque; // Keeps the order of keys

    private final int capacity;
    private final EntryPool<K, V> entryPool;

    @Contended
    private QueueValue<V> priceValueOffer = new QueueValue<>();

    ConflatingQueueImplAi(int keyCnt, int capacity) {
        this.capacity = capacity;
        entryKeyMap = new ConcurrentHashMap<>(keyCnt);
        valueMap = new ConcurrentHashMap<>(keyCnt);
        inDequeMap = new ConcurrentHashMap<>(keyCnt);
        deque = new ConcurrentLinkedDeque<>();
        this.entryPool = new EntryPool<>(keyCnt * 2); // Entry pool to avoid GC
    }

    @Override
    public boolean offer(final KeyValue<K, V> keyValue) {
        Objects.requireNonNull(keyValue);
        Objects.requireNonNull(keyValue.getKey());
        Objects.requireNonNull(keyValue.getValue());

        K key = keyValue.getKey();
        V value = keyValue.getValue();

        // Get or create an entry from the pool
        final Entry<K, QueueValue<V>> entry = entryKeyMap.computeIfAbsent(key, k -> this.entryPool.getOrCreateEntry(k));

        final QueueValue<V> newValue = priceValueOffer.initializeWithUnconfirmed(value);
        valueMap.put(key, newValue);
        try {
            if (!inDequeMap.getOrDefault(key, false)) { // If key is not in the deque
                inDequeMap.compute(key, (k, v) -> {
                    addToDeque(entry);
                    return true;
                }); // Mark key as in deque
            }
        } finally {
            priceValueOffer = entry.priceValue.getAndSet(newValue); // Pool management
//            priceValueOffer = newValue ; // Pool management
        }

        return true;
    }

    @Override
    public KeyValue<K, V> take() throws InterruptedException {
        Entry<K, QueueValue<V>> entry;
        final QueueValue<V> entryValue;
        while (true) {
            entry = pollFromDeque();
            if (entry != null && valueMap.get(entry.key) != null){
                entryValue = valueMap.get(entry.key);
                break;
            }
            if (Thread.interrupted()) throw new InterruptedException();
        }
        inDequeMap.put(entry.key, false); // Mark key as not in the deque
//        final QueueValue<V> polledValue = valueMap.get(entry.key);
//        Logger.info("get 1 "+ entryValue.value);

        final V value = entryValue.awaitAndRelease();
        final QueueKeyValue result =  new QueueKeyValue<>(entry.key, value);
//        Logger.info("get 2 "+ result.toString());
        return result;
    }

    @Override
    public boolean isEmpty() {
        return deque.isEmpty();
    }

    private void addToDeque(Entry<K, QueueValue<V>> entry) {
        deque.add(entry); // Maintain order in deque
    }

    private Entry<K, QueueValue<V>> pollFromDeque() {
        return deque.poll(); // Poll from deque
    }

    // Returns the entry map (for testing purposes)
    public Map<K, Entry<K, QueueValue<V>>> getEntryKeyMap() {
        return entryKeyMap;
    }

    // Returns the deque (for testing purposes)
    public Deque<Entry<K, QueueValue<V>>> getDeque() {
        return deque;
    }

    static class QueueValue<V> {
        private V value;

        public QueueValue() {
            // No explicit state handling
        }

        QueueValue<V> initializeWithUnconfirmed(final V value) {
            this.value = Objects.requireNonNull(value);
            return this;
        }

        void confirmWith(final V value) {
            this.value = value;
        }

        void confirm() {
            // No explicit state change
        }

        V awaitAndRelease() {
            return release();
        }

        V release() {
            V released = value;
            return released;
        }
    }

    // Pool for reusable Entry objects to avoid GC
    static class EntryPool<K, V> {
        private final Entry<K, QueueValue<V>>[] pool;
        private final AtomicInteger index = new AtomicInteger(0);

        @SuppressWarnings("unchecked")
        EntryPool(int size) {
            pool = new Entry[size];
            for (int i = 0; i < size; i++) {
                pool[i] = (Entry<K, QueueValue<V>>) new Entry<>(new Object(), new QueueValue<V>());
            }
        }

        Entry<K, QueueValue<V>> getOrCreateEntry(K key) {
            int poolIndex = getNextIndex() % pool.length;
            Entry<K, QueueValue<V>> entry = pool[poolIndex];
            entry.setKey(key);
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

