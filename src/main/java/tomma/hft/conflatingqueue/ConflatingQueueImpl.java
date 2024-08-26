package tomma.hft.conflatingqueue;

import sun.security.krb5.internal.crypto.KeyUsage;

import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConflatingQueueImpl<K,V> implements ConflatingQueue<K, V>  {

    private final Map<K, KeyValue<K,V>> instrumentPrice = new ConcurrentHashMap<>();
    private final AtomicBoolean lock = new AtomicBoolean(false);
    private final Stack incomeStack = new Stack();

    @Override
    public boolean offer(KeyValue<K, V> keyValue) {
        return false;
    }

    @Override
    public KeyValue<K, V> take() throws InterruptedException {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
