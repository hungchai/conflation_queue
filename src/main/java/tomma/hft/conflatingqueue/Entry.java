package tomma.hft.conflatingqueue;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

final class Entry<K,V> implements KeyValue<K, AtomicReference<V>> {
    final K key;
    final AtomicReference<V> value;

    Entry(final K key, final V value) {
        this.key = Objects.requireNonNull(key);
        this.value = new AtomicReference<>(value);//nulls allowed for value
    }

    @Override
    public K getKey() {
        return null;
    }

    @Override
    public AtomicReference<V> getValue() {
        return value);
    }
}
