package tomma.hft.conflatingqueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import util.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConflatingQueueImplTest {
    private ConflatingQueueImpl<String, Long> conflationQueue;
    private static final long TOTAL = 10_000_000;
    final int keyCount = (20 * 30) + 1;
    final String END_KEY = "KEY_END";

    @BeforeEach
    public void init() {
        conflationQueue = new ConflatingQueueImpl<>(keyCount);
    }

    @Test
    void offerThenTake() {
        final List<String> keys = new ArrayList<>(keyCount);
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
        kv.setKey(END_KEY);
        kv.setValue(-1L);
        conflationQueue.offer(kv);

        while (true) {
            try {
                QueueKeyValue queueValue = (QueueKeyValue) conflationQueue.take();
                if (queueValue.getKey().equals(END_KEY)) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }catch(Exception e) {
                Logger.error(e.getMessage(), e);
            }
        }
    }


    @Test
    void offerTakeConcurrent() throws InterruptedException {
        Map<String, Entry<String, ConflatingQueueImpl.QueueValue<Long>>> k = conflationQueue.getEntryKeyMap();
        Deque<Entry<String, ConflatingQueueImpl.QueueValue<Long>>> d = conflationQueue.getDeque();

        final List<String> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) keys.add("KEY_" + i);
        final Random rnd = new Random();
        QueueKeyValue<String, Long> kv = new QueueKeyValue<>("NaN", 0L);

        Map<String, Long> assertPublishMap = new ConcurrentHashMap<>();
        Map<String, Long> assertConsumerMap = new ConcurrentHashMap<>();
        AtomicReference<String> assertFirstKey = new AtomicReference<>("Nan");
        AtomicReference<String> assertLastKey = new AtomicReference<>("Nan");
        final Thread producer = new Thread(() -> {
            for (long i = 0; i < TOTAL; i++) {
                final int keyIndex = rnd.nextInt(keyCount);
                final String key = keys.get(keyIndex);
                kv.setKey(key);
                kv.setValue(i);
                assertPublishMap.put(kv.getKey(), kv.getValue());
                conflationQueue.offer(kv);
                if (i == 0) assertFirstKey.set(key);
                if (!assertPublishMap.containsKey(key)) {
                    assertLastKey.set(kv.getKey());
                }
            }
            kv.setKey(END_KEY);
            kv.setValue(-1L);
            assertPublishMap.put(kv.getKey(), kv.getValue());
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
                    if (queueValue.getValue() == null)
                        Logger.info("p null: " + queueValue);

                    assertConsumerMap.put(
                            queueValue.getKey(),
                            queueValue.getValue());
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
        producer.join();
        assertEquals(assertConsumerMap.size(), assertPublishMap.size());
        assertEquals(assertPublishMap.keySet(), assertConsumerMap.keySet());


        for (Map.Entry<String, Long> entry : assertPublishMap.entrySet()) {
            String key = entry.getKey();
            Long publishValue = entry.getValue();
            Long consumerValue = assertConsumerMap.get(key);

            // Assert that both maps contain the same key-value pairs
            assertNotNull("Key " + key + " is missing in consumer map", String.valueOf(consumerValue));
            if (!publishValue.equals(consumerValue)) {
                Logger.info("key "+ key + " is not same publishValue " +publishValue +  "consumerValue " + consumerValue);
            }
        }
        assertEquals(assertPublishMap, assertConsumerMap);
    }

}