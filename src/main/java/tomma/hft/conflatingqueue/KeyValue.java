package tomma.hft.conflatingqueue;

public interface KeyValue<K, V> {

	/**
	 * Returns the key
	 * @return the key
	 */
	K getKey();

	V getValue();
}
