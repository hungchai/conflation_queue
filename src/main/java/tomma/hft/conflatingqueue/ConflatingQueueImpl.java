package tomma.hft.conflatingqueue;


import sun.misc.Contended;

import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import util.Logger;

public class ConflatingQueueImpl<K, V> implements ConflatingQueue<K, V> {

    private final Map<K, Entry<K, QueueValue<V>>> instrumentPriceMap;

    private final Deque<Entry<K, QueueValue<V>>> deque;

    @Contended
    QueueValue<V> priceValueOffer = new QueueValue<>();
//    @Contended
    QueueValue<V> priceValueTake = new QueueValue<>();

    ConflatingQueueImpl(int keyCnt) {
        instrumentPriceMap = new ConcurrentHashMap<>(keyCnt);
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
            final Entry<K, QueueValue<V>> entry = instrumentPriceMap.computeIfAbsent(conflationKey, k -> new Entry<>(k, new QueueValue<>()));
            final QueueValue<V> newValue = priceValueOffer.initializeWithUnconfirmed(value);
            final QueueValue<V> oldValue = entry.priceValue.getAndSet(newValue);
            final V add;
            final V old;
            try {
                if (oldValue.isNotInQueue()) {
                    old = oldValue.release();
                    add = value;
                    newValue.confirm();
                    deque.add(entry);

                } else {
                    old = oldValue.awaitAndRelease();
                    try {
                        add = value;
                    } catch (final Throwable t) {
                        newValue.confirmWith(old);
                        throw t;
                    }
                    newValue.confirmWith(add);
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
        } catch (RuntimeException e) {
            throw e;
        } finally {
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    public Map<K, Entry<K, QueueValue<V>>> getInstrumentPriceMap() {
        return instrumentPriceMap;
    }

    public Deque<Entry<K, QueueValue<V>>> getDeque() {
        return deque;
    }

    static class QueueValue<V> {
        enum State {UNUSED, UNCONFIRMED, CONFIRMED}
        private V value;
        volatile QueueValue.State state;

        public QueueValue() {
            this.state = QueueValue.State.UNUSED;
        }

        QueueValue<V> initializeWithUnconfirmed(final V value) {
            this.value = Objects.requireNonNull(value);
            this.state = QueueValue.State.UNCONFIRMED;
            return this;
        }
        QueueValue<V> initalizeWithUnused(final V value) {
            this.value = value;//nulls allowed here
            this.state = QueueValue.State.UNUSED;
            return this;
        }

        void confirmWith(final V value) {
            this.value = value;
            this.state = QueueValue.State.CONFIRMED;
        }

        boolean isNotInQueue() {
            return state == QueueValue.State.UNUSED;
        }

        void confirm() {
            this.state = QueueValue.State.CONFIRMED;
        }
        V awaitAndRelease() {
            awaitFinalState();
            return release();
        }

        QueueValue.State awaitFinalState() {
            QueueValue.State s;
            do {
                s = state;
            } while (s == QueueValue.State.UNCONFIRMED);
            return s;
        }
        V release() {
            final V released = value;
            state = QueueValue.State.UNUSED;
            this.value = null;
            return released;
        }

    }
}
