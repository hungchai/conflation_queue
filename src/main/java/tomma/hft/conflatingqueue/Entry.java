package tomma.hft.conflatingqueue;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

final class Entry<K, V> {
    K key;
    final AtomicReference<V> priceValue;
    private final Long address;

    Entry(final K key, final V value) {
        this.key = Objects.requireNonNull(key);
        this.priceValue = new AtomicReference<>(value);//nulls allowed for value
        this.address = Long.parseLong(null);
    }

    Entry(final K key, final V value, long address) {
        this.key = Objects.requireNonNull(key);
        this.priceValue = new AtomicReference<>(value);//nulls allowed for value
        this.address = address;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return priceValue.get();
    }

    public AtomicReference<V> getPriceValue() {
        return priceValue;
    }

}
