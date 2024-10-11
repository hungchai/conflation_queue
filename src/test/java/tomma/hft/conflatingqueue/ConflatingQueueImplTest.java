package tomma.hft.conflatingqueue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import util.Logger;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConflatingQueueImplTest {
    private ConflatingQueue<String, Long> conflationQueue;
    private static long TOTAL = 1_000_000 * 1;
    int keyCount = (20 * 50) + 1;
    final String END_KEY = "KEY_END";
    private PerformanceAnalyzer performanceAnalyzerProd;
    private PerformanceAnalyzer performanceAnalyzerConsum;

    @BeforeEach
    public void init() {
        conflationQueue = new ConflatingQueueImplv1<>(keyCount, 1_000_000);
        performanceAnalyzerProd = new PerformanceAnalyzer();
        performanceAnalyzerConsum = new PerformanceAnalyzer();
    }

    @Test
    void offerThenTake() {
        final List<String> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) keys.add("KEY_" + i);
        keys.add(END_KEY);
        final Random rnd = new Random();
        QueueKeyValue<String, Long> kv = new QueueKeyValue<>("NaN", 0L);

        Map<String, Long> assertPublishMap = new ConcurrentHashMap<>();
        Map<String, Long> assertConsumerMap = new ConcurrentHashMap<>();        String assertFirstKey = "Nan";
        String assertLastKey = "Nan";

        for (long i = 0; i < TOTAL; i++) {
            final int keyIndex = rnd.nextInt(keyCount);
            final String key = keys.get(keyIndex);
            kv.setKey(key);
            kv.setValue(i);
            long startTime = System.nanoTime();
            conflationQueue.offer(kv);
            long endTime = System.nanoTime();
            performanceAnalyzerProd.recordExecutionTime(endTime - startTime);
//            if (i == 0) assertFirstKey = key;
//            if (!assertPublishMap.containsKey(key)) {
//                assertLastKey = kv.getKey();
//            }
//            assertPublishMap.put(kv.getKey(), kv.getValue());
        }
        kv.setKey(END_KEY);
        kv.setValue(-1L);
        conflationQueue.offer(kv);

//        Map<String, Entry<String, ConflatingQueueImplAi.QueueValue<Long>>> k = conflationQueue.getEntryKeyMap();
//        Deque<Entry<String, ConflatingQueueImplAi.QueueValue<Long>>> d = conflationQueue.getDeque();
//
//        assertEquals(assertPublishMap.size(), k.size());
//        assertEquals(assertPublishMap.size(), d.size());
//        assert d.peek() != null;
//        assertEquals(assertFirstKey,  ((Entry<?, ?>)d.peek()).getKey());
//        assert d.peekLast() != null;
//        assertEquals(assertLastKey,  ((Entry<?, ?>)d.peekLast()).getKey());

        while (true) {
            try {
                long startTime = System.nanoTime();
                QueueKeyValue<String, Long> queueValue = (QueueKeyValue<String, Long>) conflationQueue.take();
                long endTime = System.nanoTime();
                performanceAnalyzerConsum.recordExecutionTime(endTime - startTime);

//                assertConsumerMap.put(queueValue.getKey(), queueValue.getValue());
//                assertEquals(assertPublishMap.get(queueValue.getKey()),  queueValue.getValue());
//
//                if (j == k.size() - 1) {
//                    assertEquals(assertLastKey,  queueValue.getKey());
//                }
                if (queueValue.getKey().equals(END_KEY)) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }catch(Exception e) {
                Logger.error(e.getMessage(), e);
            }
        }

        performanceAnalyzerProd.printHistogram();
        performanceAnalyzerConsum.printHistogram();

//        Assertions.assertTrue(d.isEmpty());
//        assertEquals(assertPublishMap, assertConsumerMap);
    }

//     * 		1. The consumer calls take(), and blocks as the queue is empty.
//            * 		2. The producer calls offer(@{@link KeyValue}) with key = BTCUSD, value = 7000.  The consumer unblocks and receives
// * 		   the @{@link KeyValue}.
//            * 		3. The producer calls offer(@{@link KeyValue}) with key = BTCUSD, value = 7001
//            * 		4. The producer calls offer(@{@link KeyValue}) with key = ETHUSD, value = 250
//            * 		5. The producer calls offer(@{@link KeyValue}) with key = BTCUSD, value = 7002, which replaces the queued price 7001
//            * 		6. The consumer calls take() three times.  The consumer first receives key = BTCUSD, value = 7002, then receives
// * 		   key = ETHUSD, value = 250, then blocks as the queue is empty.
    @Test
    void bitmexTest() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(1);

        AtomicReference<KeyValue<String, Long>> queueValueF = new AtomicReference();
        Runnable consumerTask = () -> {
            try {
                queueValueF.set(conflationQueue.take());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        executor.submit(consumerTask);
        LockSupport.parkUntil(System.currentTimeMillis() + 5000);
        conflationQueue.offer(new QueueKeyValue("BTCUSD", 7000L));
        LockSupport.parkNanos(1_000_000);
        assertEquals("BTCUSD", queueValueF.get().getKey() );
        assertEquals(7000L,  queueValueF.get().getValue());

        conflationQueue.offer(new QueueKeyValue("BTCUSD", 7001L));
        conflationQueue.offer(new QueueKeyValue("ETHUSD", 250L));
        conflationQueue.offer(new QueueKeyValue("BTCUSD", 7002L));


        KeyValue<String, Long> queueValue = conflationQueue.take();
        assertEquals("BTCUSD", queueValue.getKey());
        assertEquals(7002,queueValue.getValue());

        queueValue = conflationQueue.take();
        assertEquals("ETHUSD", queueValue.getKey());
        assertEquals(250, queueValue.getValue());
        executor.submit(consumerTask);
        LockSupport.parkUntil(System.currentTimeMillis() + 5000);

    }

    @Test
    void offerTakeConcurrent5MItems() throws InterruptedException {
        TOTAL = 1_000_000 * 5;
        offerTakeConcurrent(null );
    }

    @Test
    void offerTakeConcurrent5min() throws InterruptedException {
        TOTAL = 1_000_000 * 5;
        offerTakeConcurrent(60 * 10);
    }

    void offerTakeConcurrent(Integer endAfterSec) throws InterruptedException {
        Instant endDt;
        if (endAfterSec != null) {
            endDt = Instant.now().plusSeconds(endAfterSec);
        } else {
            endDt = null;
        }
        final List<String> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) keys.add("KEY_" + i);
        final Random rnd = new Random();
        QueueKeyValue<String, Long> kv = new QueueKeyValue<>("NaN", 0L);

        Map<String, Long> assertPublishMap = new ConcurrentHashMap<>();
        Map<String, Long> assertConsumerMap = new ConcurrentHashMap<>();
        AtomicReference<String> assertFirstKey = new AtomicReference<>("Nan");
        AtomicReference<String> assertLastKey = new AtomicReference<>("Nan");
        Thread producer = null;
        if (endDt != null) {
            producer = new Thread(() -> {
                long i = 0;
                while (Instant.now().isBefore(endDt)) {
                    final int keyIndex = rnd.nextInt(keyCount);
                    final String key = keys.get(keyIndex);
                    kv.setKey(key);
                    kv.setValue(i);
                    assertPublishMap.put(kv.getKey(), kv.getValue());
                    long startTime = System.nanoTime();
                    conflationQueue.offer(kv);
                    long endTime = System.nanoTime();
                    performanceAnalyzerProd.recordExecutionTime(endTime - startTime);
                    if (i == 0) assertFirstKey.set(key);
                    if (!assertPublishMap.containsKey(key)) {
                        assertLastKey.set(kv.getKey());
                    }
                    i++;
                    if (i > TOTAL) {
                        i = 0;
                    }
                }
                    kv.setKey(END_KEY);
                    kv.setValue(-1L);
                    assertPublishMap.put(kv.getKey(), kv.getValue());
                    conflationQueue.offer(kv);
                });

        }
        else {
            producer = new Thread(() -> {
                for (long i = 0; i < TOTAL; i++) {
                    final int keyIndex = rnd.nextInt(keyCount);
                    final String key = keys.get(keyIndex);
                    kv.setKey(key);
                    kv.setValue(i);
                    assertPublishMap.put(kv.getKey(), kv.getValue());
                    long startTime = System.nanoTime();
                    conflationQueue.offer(kv);
                    long endTime = System.nanoTime();
                    performanceAnalyzerProd.recordExecutionTime(endTime - startTime);
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
        }

        final Thread consumer = new Thread(() -> {
            KeyValue<String, Long>  queueValue = null;
            long i = 0;
            do{
                try {
                    long startTime = System.nanoTime();
                    queueValue = conflationQueue.take();
                    long endTime = System.nanoTime();
                    performanceAnalyzerConsum.recordExecutionTime(endTime - startTime);

//                    assertEquals(assertMap.get(queueValue.getKey()), queueValue.getValue());
//                    Logger.info("d " + d.size());

                    assertConsumerMap.put(
                            queueValue.getKey(),
                            queueValue.getValue());
//                    if (i % 10_000 == 0) {
//                        Logger.info("c: " + queueValue);
//                    }
                    if (queueValue.getKey().equals(END_KEY)) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    i++;
                }catch(Exception e) {
                    Logger.error(queueValue.getKey()+ " "  + e.getMessage(), e);
                }
            }while (true);
        });
        consumer.start();
        sleep(500);
        producer.start();

        consumer.join();
        producer.join();

        performanceAnalyzerProd.printHistogram();
        performanceAnalyzerConsum.printHistogram();

        assertEquals(assertConsumerMap.size(), assertPublishMap.size());
        assertEquals(assertPublishMap.keySet(), assertConsumerMap.keySet());


        for (Map.Entry<String, Long> entry : assertPublishMap.entrySet()) {
            String key = entry.getKey();
            Long publishValue = entry.getValue();
            Long consumerValue = assertConsumerMap.get(key);

            // Assert that both maps contain the same key-value pairs
            assertNotNull("Key " + key + " is missing in consumer map", String.valueOf(consumerValue));
            if (!publishValue.equals(consumerValue)) {
                Logger.info("key "+ key + " is not same publishValue " +publishValue +  " consumerValue " + consumerValue);
            }
        }
        assertEquals(assertPublishMap, assertConsumerMap);
    }

}