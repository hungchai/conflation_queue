package tomma.hft.conflatingqueue;


//import sun.misc.Contended;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import tomma.hft.conflatingqueue.InstrumentPrice.PriceValue;
import util.Logger;

public class ConflatingQueueImpl<K, V> implements ConflatingQueue<K, V> {

    private final Map<K, Entry<K, PriceValue<V>>> instrumentPriceMap = new ConcurrentHashMap<>();

    private final AtomicBoolean lock = new AtomicBoolean(false);
    private final ConcurrentLinkedDeque<Entry<K, PriceValue<V>>> deque = new ConcurrentLinkedDeque<>();
//    @Contended
    PriceValue<V> priceValueOffer = new PriceValue<>();
//    @Contended
    PriceValue<V> priceValueTake = new PriceValue<>();

    @Override
    public boolean offer(final KeyValue<K, V> keyValue) {
        try {
            Objects.requireNonNull(keyValue);
            Objects.requireNonNull(keyValue.getKey());
            Objects.requireNonNull(keyValue.getValue());
            K conflationKey = keyValue.getKey();
            V value = keyValue.getValue();
            final Entry<K, PriceValue<V>> entry = instrumentPriceMap.computeIfAbsent(conflationKey, k -> new Entry<>(k, new PriceValue<>()));
            final PriceValue<V> newValue = priceValueOffer.initializeWithUnconfirmed(value);
            final PriceValue<V> oldValue = entry.priceValue.getAndSet(newValue);
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
            Logger.info(entry.toString()+ " key:" + conflationKey + " old:" + old +" add:" + add);

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
            lock.compareAndSet(false, true);
        } catch (RuntimeException e) {
            throw e;
        } finally {
            lock.set(false);
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
