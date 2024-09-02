package tomma.hft.conflatingqueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.*;
import util.Logger;

import java.util.*;

class ConflatingQueueImplTest {
    private ConflatingQueueImpl<String, Long> conflationQueue;
    private static final int TOTAL = 100_000_000;
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
        Long assertLastValue= 0L;

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
        Map k = conflationQueue.getEntryKeyMap();
        Deque d = conflationQueue.getDeque();

        Assertions.assertEquals(assertMap.size(), k.size());
        Assertions.assertEquals(assertMap.size(), d.size());
        Assertions.assertEquals(assertFirstKey,  ((Entry)d.peek()).getKey());
        Assertions.assertEquals(assertLastKey,  ((Entry)d.peekLast()).getKey());

        for (int j = 0; j < k.size(); j++) {
            try {
                QueueKeyValue<String, Long> queueValue = (QueueKeyValue) conflationQueue.take();
                Assertions.assertEquals(assertMap.get(queueValue.getKey()),  queueValue.getValue());

                if (j == k.size() - 1) {
                    Assertions.assertEquals(assertLastKey,  queueValue.getKey());
                }
            }catch(Exception e) {
                Logger.error(e.getMessage(), e);
            }
        }
        Assertions.assertTrue(d.isEmpty());
    }
}