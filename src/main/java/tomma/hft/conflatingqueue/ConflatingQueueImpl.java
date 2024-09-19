package tomma.hft.conflatingqueue;


import sun.misc.Contended;

import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import util.Logger;

public class ConflatingQueueImpl<K, V> implements ConflatingQueue<K, V> {

    @Contended
    private final Map<K, Entry<K, QueueValue<V>>> entryKeyMap;

    private final Deque<Entry<K, QueueValue<V>>> deque;

    @Contended
    QueueValue<V> priceValueOffer = new QueueValue<>();

    @Contended
    QueueValue<V> priceValueTake = new QueueValue<>();

    ConflatingQueueImpl(int keyCnt) {
        entryKeyMap = new ConcurrentHashMap<>(keyCnt);
        deque = new ConcurrentLinkedDeque<>();
    }

    @Override
    public boolean offer(final KeyValue<K, V> keyValue) {
        try {
            Objects.requireNonNull(keyValue);
            Objects.requireNonNull(keyValue.getKey());
            Objects.requireNonNull(keyValue.getValue());
            K conflationKey = keyValue.getKey();
            V value = keyValue.getValue();
            final Entry<K, QueueValue<V>> entry = entryKeyMap.computeIfAbsent(conflationKey, k -> new Entry<>(k, new QueueValue<>()));
            final QueueValue<V> newValue = priceValueOffer.initializeWithUnconfirmed(value);
            final QueueValue<V> oldValue = entry.priceValue.getAndSet(newValue);
            final V add;
            final V old;
            try {
                if (oldValue.isNotInQueue()) {
                    old = oldValue.awaitAndRelease();
                    newValue.confirm();
                    deque.add(entry);

                } else {
                    old = oldValue.awaitAndRelease();
                    try {
                        add = value;
                        newValue.confirmWith(add);
                    } catch (final Throwable t) {
                        newValue.confirmWith(old);
                        throw t;
                    }
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
                    entry = deque.poll();
                }catch(Exception ex) {
                    Logger.error("unknown exception",ex);
                }
            }
            while (entry == null);

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
        return false;
    }

    public Map<K, Entry<K, QueueValue<V>>> getEntryKeyMap() {
        return entryKeyMap;
    }

    public Deque<Entry<K, QueueValue<V>>> getDeque() {
        return deque;
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
}
