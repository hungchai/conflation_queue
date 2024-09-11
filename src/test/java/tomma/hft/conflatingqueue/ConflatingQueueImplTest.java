package tomma.hft.conflatingqueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import util.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConflatingQueueImplTest {
    private ConflatingQueueImpl<String, Long> conflationQueue;
    private static final long TOTAL = 10_000_000;
    final int keyCount = 2 * 5000;
    final String END_KEY = "KEY_END";

    @BeforeEach
    public void init() {

    }

    @Test
    void offer() {
        conflationQueue = new ConflatingQueueImpl<>(keyCount);

        final List<String> keys = new ArrayList<>(keyCount + 1);
        for (int i = 0; i < keyCount; i++) keys.add("KEY_" + i);
        keys.add(END_KEY);
        final Random rnd = new Random();
        QueueKeyValue<String, Long> kv = new QueueKeyValue<>("NaN", 0L);

        Map<String, Long> assertMap = new HashMap<>();
        String assertFirstKey = "Nan";
        String assertLastKey = "Nan";

        for (long i = 0; i < TOTAL; i++) {
            final int keyIndex = rnd.nextInt(keyCount);
            final String key = keys.get(keyIndex);
            kv.setKey(key);
            kv.setValue(i);
            conflationQueue.offer(kv);

            if (i == 0) assertFirstKey = key;
            if (!assertMap.containsKey(key)) {
                assertLastKey = kv.getKey();
            }
            assertMap.put(kv.getKey(), kv.getValue());
        }
        Map<String, Entry<String, ConflatingQueueImpl.QueueValue<Long>>> k = conflationQueue.getEntryKeyMap();
        Deque<Entry<String, ConflatingQueueImpl.QueueValue<Long>>> d = conflationQueue.getDeque();

        assertEquals(assertMap.size(), k.size());
        assertEquals(assertMap.size(), d.size());
        assert d.peek() != null;
        assertEquals(assertFirstKey,  ((Entry<?, ?>)d.peek()).getKey());
        assert d.peekLast() != null;
        assertEquals(assertLastKey,  ((Entry<?, ?>)d.peekLast()).getKey());

        for (int j = 0; j < k.size(); j++) {
            try {
                QueueKeyValue queueValue = (QueueKeyValue) conflationQueue.take();
                assertEquals(assertMap.get(queueValue.getKey()),  queueValue.getValue());

                if (j == k.size() - 1) {
                    assertEquals(assertLastKey,  queueValue.getKey());
                }
            }catch(Exception e) {
                Logger.error(e.getMessage(), e);
            }
        }
        Assertions.assertTrue(d.isEmpty());
    }


    @Test
    void offerTakeConcurrent() throws InterruptedException {
        conflationQueue = new ConflatingQueueImpl<>(keyCount);
        Map<String, Entry<String, ConflatingQueueImpl.QueueValue<Long>>> k = conflationQueue.getEntryKeyMap();
        Deque<Entry<String, ConflatingQueueImpl.QueueValue<Long>>> d = conflationQueue.getDeque();

        final List<String> keys = new ArrayList<>(keyCount + 1);
        for (int i = 0; i < keyCount; i++) keys.add("KEY_" + i);
        final Random rnd = new Random();
        QueueKeyValue<String, Long> kv = new QueueKeyValue<>("NaN", 0L);

        Map<String, Long> assertMap = new ConcurrentHashMap<>();
        AtomicReference<String> assertFirstKey = new AtomicReference<>("Nan");
        AtomicReference<String> assertLastKey = new AtomicReference<>("Nan");
        final Thread producer = new Thread(() -> {
            for (long i = 0; i < TOTAL; i++) {
                final int keyIndex = rnd.nextInt(keyCount);
                final String key = keys.get(keyIndex);
                kv.setKey(key);
                kv.setValue(i);
                assertMap.put(kv.getKey(), kv.getValue());
                conflationQueue.offer(kv);
                if (i % 100_000_000 == 0) {
                    Logger.info("p: " + kv);
                }
                if (i == 0) assertFirstKey.set(key);
                if (!assertMap.containsKey(key)) {
                    assertLastKey.set(kv.getKey());
                }
            }
            kv.setKey(END_KEY);
            kv.setValue(-1L);
            conflationQueue.offer(kv);
        });
        producer.start();


        final Thread consumer = new Thread(() -> {
            KeyValue<String, Long>  queueValue = null;
            long i = 0;
            do{
                try {
                    queueValue = conflationQueue.take();
//                    assertEquals(assertMap.get(queueValue.getKey()), queueValue.getValue());
//                    Logger.info("d " + d.size());
                    if (i % 100_000_000 == 0) {
                        Logger.info("c: " + kv);
                    }
                    if (queueValue.getKey().equals(END_KEY)) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    i++;
                }catch(Exception e) {
                    Logger.error(e.getMessage(), e);
                }
            }while (true);
        });
        consumer.start();
        consumer.join();

    }

}