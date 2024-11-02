package tomma.hft.conflatingqueue;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

public class OffHeapEntryPool<K, V> {
    private static final Unsafe unsafe = getUnsafe();
    private static final int ENTRY_SIZE = 16; // 8 bytes for key reference, 8 bytes for value reference
    private final long baseAddress;
    private final int poolSize;
    private final AtomicInteger index = new AtomicInteger(0);

    public OffHeapEntryPool(int size) {
        this.poolSize = size;
        // Allocate off-heap memory for the entire pool
        baseAddress = unsafe.allocateMemory(size * ENTRY_SIZE);
        for (int i = 0; i < size; i++) {
            long entryAddress = getAddress(i);
            unsafe.putAddress(entryAddress, 0L);  // Initialize key to null
            unsafe.putAddress(entryAddress + 8, 0L);  // Initialize value to null
        }
    }

    public long getOrCreateEntryAddress(K key, V initialValue) {
        int poolIndex = getNextIndex() % poolSize;
        long entryAddress = getAddress(poolIndex);

        // Set the key if not already present
        K currentKey = (K) unsafe.getObject(null, entryAddress);
        if (currentKey == null || !currentKey.equals(key)) {
            unsafe.putObject(null, entryAddress, key);
        }

        // Initialize or retrieve the QueueValue
        QueueValueWrapper.QueueValue<V> value = (QueueValueWrapper.QueueValue<V>) unsafe.getObject(null, entryAddress + 8);
        if (value == null) {
            value = new QueueValueWrapper.QueueValue<>();
            value.initializeWithUnconfirmed(initialValue);
            unsafe.putObject(null, entryAddress + 8, value);
        }

        return entryAddress;
    }

    public K getKey(long entryAddress) {
        return (K) unsafe.getObject(null, entryAddress);
    }

    public QueueValueWrapper.QueueValue<V> getValue(long entryAddress) {
        return (QueueValueWrapper.QueueValue<V>) unsafe.getObject(null, entryAddress + 8);
    }

    public void setKey(long entryAddress, K key) {
        unsafe.putObject(null, entryAddress, key);
    }

    private long getAddress(int index) {
        return baseAddress + (index * ENTRY_SIZE);
    }

    private int getNextIndex() {
        while (true) {
            int current = index.get();
            int next = current >= Integer.MAX_VALUE - poolSize ? 0 : current + 1;
            if (index.compareAndSet(current, next)) {
                return current;
            }
        }
    }

    public void freeMemory() {
        unsafe.freeMemory(baseAddress);  // Release off-heap memory
    }

    private static Unsafe getUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to access Unsafe", e);
        }
    }
}
