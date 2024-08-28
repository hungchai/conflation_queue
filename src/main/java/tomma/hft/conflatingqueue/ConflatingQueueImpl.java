package tomma.hft.conflatingqueue;


import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConflatingQueueImpl<K,V> implements ConflatingQueue<K, V>  {

    private final Map<K, KeyValue<K,V>> instrumentPrice = new ConcurrentHashMap<>();
    private final AtomicBoolean lock = new AtomicBoolean(false);
    private final ConcurrentLinkedDeque<K> deque = new ConcurrentLinkedDeque<>();

    @Override
    public boolean offer(final KeyValue<K, V> keyValue) {
        try {
            Objects.requireNonNull(keyValue);
            Objects.requireNonNull(keyValue.getKey());
            Objects.requireNonNull(keyValue.getValue());
            String conflationKey = keyValue.getKey();
            final Entry<K,MarkedValue<V>> entry = instrumentPrice.computeIfAbsent(conflationKey, k -> new Entry<>(k, new MarkedValue<>()));
            final MarkedValue<V> newValue = markedValue.initializeWithUnconfirmed(value);
            final MarkedValue<V> oldValue = entry.value.getAndSet(newValue);
            final V add;
            final V old;
        }
        catch(NullPointerException npe){
            Log.error("error nullpointerException");
            Log.error(npe.getStackTrace().toString());
                throw npe;

        } finally {

        }
    }

    @Override
    public KeyValue<K, V> take() throws InterruptedException {
        try {
            lock.compareAndSet(false, true);
        }
        catch (RuntimeException e) {
            throw e;
        }
        finally {
            lock.set(false);
        }
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
