package tomma.hft.conflatingqueue;

public class QueueKeyValue<K, V> implements KeyValue<K,V> {
    private K key;
    private V value;

    public QueueKeyValue(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "QueueKeyValue{" +
                "key=" + this.getKey() +
                ", value=" + this.getValue() +
                '}';
    }
}
