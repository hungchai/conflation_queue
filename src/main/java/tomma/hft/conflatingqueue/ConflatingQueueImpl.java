package tomma.hft.conflatingqueue;


import sun.misc.Contended;

import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import util.Logger;

public class ConflatingQueueImpl<K, V> implements ConflatingQueue<K, V> {

    @Contended
    private final Map<K, Entry<K, QueueValue<V>>> entryKeyMap;

//    private final AtomicReferenceArray<Entry<K, QueueValue<V>>> deque;
    private final Deque<Entry<K, QueueValue<V>>> deque;
    private final int capacity;
    private final int keyCnt;
    private volatile int head = 0;
    private volatile int tail = 0;

    @Contended
    QueueValue<V> priceValueOffer = new QueueValue<>();

    @Contended
    QueueValue<V> priceValueTake = new QueueValue<>();

    // Pool for reusable entries and QueueValues
    private final EntryPool<K, V> entryPool;
//    private final QueueValuePool<V> queueValuePool;

    ConflatingQueueImpl(int keyCnt, int capacity) {
        this.capacity = capacity;
        this.keyCnt = keyCnt;
        entryKeyMap = new ConcurrentHashMap<>(keyCnt);
        deque = new ConcurrentLinkedDeque<>();
        // Initialize pools
        this.entryPool = new EntryPool<>(capacity);
//        this.queueValuePool = new QueueValuePool<>(capacity);
    }

    @Override
    public boolean offer(final KeyValue<K, V> keyValue) {
        try {
            Objects.requireNonNull(keyValue);
            Objects.requireNonNull(keyValue.getKey());
            Objects.requireNonNull(keyValue.getValue());
            K conflationKey = keyValue.getKey();
            V value = keyValue.getValue();
//            final Entry<K, QueueValue<V>> entry = entryKeyMap.computeIfAbsent(conflationKey, k -> this.entryPool.getOrCreateEntry(k));
            final Entry<K, QueueValue<V>> entry = entryKeyMap.computeIfAbsent(conflationKey, k -> new Entry<>(k, new QueueValue<V>()));
            final QueueValue<V> newValue = priceValueOffer.initializeWithUnconfirmed(value);
            final QueueValue<V> oldValue = entry.priceValue.getAndSet(newValue);
            final V add;
            final V old;
            try {
                if (oldValue.isNotInQueue()) {
                    old = oldValue.awaitAndRelease();
                    newValue.confirm();
                    addToDeque(entry);
                } else {
                    old = oldValue.awaitAndRelease();
                    try {
                        add = value;
                        newValue.confirmWith(add);
                    } catch (final Throwable t) {
                        newValue.confirmWith(old);
                        throw t;
                    }
//                    newValue.confirmWith(add);
                }
            } finally {
                priceValueOffer = oldValue;
            }
//            Logger.info(entry.toString()+ " key:" + conflationKey + " old:" + old +" add:" + add);

        } catch (NullPointerException npe) {
            Logger.error("error nullpointerException");
            Logger.error(npe.getStackTrace().toString());
            throw npe;
        }
        return true;
    }

    @Override
    public KeyValue<K, V> take() throws InterruptedException {
        try {
            Entry<K, QueueValue<V>> entry = null;
            do {
                try {
                    if (Thread.interrupted())
                        throw new InterruptedException();
                    entry = pollFromDeque();
                }catch(Exception ex) {
                    Logger.error("unknown exception",ex);
                }
            }
            while (entry == null || entry.priceValue.get().state != QueueValue.State.CONFIRMED);

//            final QueueValue<V> exchangeValue = priceValueTake.initalizeWithUnused(null);
//            final QueueValue<V> polledValue = entry.priceValue.getAndSet(exchangeValue);
            final QueueValue<V> polledValue = entry.priceValue.get();
            V value = polledValue.awaitAndRelease();
            K key = entry.key;
//            priceValueTake = polledValue;
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
        // Set the entry in the AtomicReferenceArray at the tail position
        deque.add(entry);
//        tail = (tail + 1) % keyCnt;
    }

    private Entry<K, QueueValue<V>> pollFromDeque() {
        if (isEmpty()) {
            return null;
        }
        Entry<K, QueueValue<V>> entry = deque.poll();
//        head = (head + 1) % keyCnt;
        return entry;
    }

    public Map<K, Entry<K, QueueValue<V>>> getEntryKeyMap() {
        return entryKeyMap;
    }

    public Deque<Entry<K, QueueValue<V>>> getDeque() {
        return deque;
    }
    public int getHead() {
        return head;
    }

    public int getTail() {
        return tail;
    }
    static class QueueValue<V> {
        enum State {UNUSED, UNCONFIRMED, CONFIRMED}

        private V value;
        volatile State state;

        public QueueValue() {
            this.state = State.UNUSED;
        }

        QueueValue<V> initializeWithUnconfirmed(final V value) {
            this.value = Objects.requireNonNull(value);
            this.state = State.UNCONFIRMED;
            return this;
        }

        QueueValue<V> initalizeWithUnused(final V value) {
            this.value = value;//nulls allowed here
            this.state = State.UNUSED;
            return this;
        }

        void confirmWith(final V value) {
            this.value = value;
            this.state = State.CONFIRMED;
        }

        boolean isNotInQueue() {
            return state == State.UNUSED;
        }

        void confirm() {
            this.state = State.CONFIRMED;
        }

        V awaitAndRelease() {
            awaitFinalState();
            return release();
        }

        State awaitFinalState() {
            State s;
            do {
                s = state;
            } while (s == State.UNCONFIRMED);
            return s;
        }

        V release() {
            final V released = value;
            state = State.UNUSED;
            this.value = null;
            return released;
        }

    }

//    static class QueueValuePool<V> {
//        private final QueueValue<V>[] pool;
//        private AtomicInteger index = new AtomicInteger(0);
//
//        QueueValuePool(int size) {
//            pool = new QueueValue[size];
////            for (int i = 0; i < size; i++) {
////                pool[i] = new QueueValue<>();
////            }
//        }
//
//        QueueValue<V> getOrCreateQueueValue() {
//            int poolIndex = index.incrementAndGet() % pool.length;
//            if (pool[poolIndex] == null) {
//                pool[poolIndex] = new QueueValue<>();
//            }
//            return pool[poolIndex];
//        }
//    }

    // Pool for reusable Entry objects to avoid GC
    static class EntryPool<K,V> {
        private final Entry<K, QueueValue<V>>[] pool;
        private AtomicInteger index = new AtomicInteger(0);

        @SuppressWarnings("unchecked")
        EntryPool(int size) {
            pool = new Entry[size];
            for (int i = 0; i < size; i++) {
                pool[i] = (Entry<K, QueueValue<V>>) new Entry<>(new Object(), new QueueValue<V>());
            }
        }

        Entry<K, QueueValue<V>> getOrCreateEntry(K key) {
            int poolIndex = getNextIndex() % pool.length;
//            if (pool[poolIndex] == null) {
//                pool[poolIndex] = new Entry<>(key, new QueueValue<V>());
//            }
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
