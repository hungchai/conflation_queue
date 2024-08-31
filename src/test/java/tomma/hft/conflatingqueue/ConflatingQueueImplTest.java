package tomma.hft.conflatingqueue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import util.Logger;

import java.util.*;

class ConflatingQueueImplTest {
    private ConflatingQueueImpl<String, Long> conflationQueue;
    private static final int TOTAL = 2_000_000;
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
        for (int i = 0; i < TOTAL; i++) {
            final int keyIndex = rnd.nextInt(keyCount);
            final String key = keys.get(keyIndex);
            kv.setKey(key);
            kv.setValue((long) i);
            conflationQueue.offer(kv);
        }
        Map t = conflationQueue.getInstrumentPriceMap();
        Deque d = conflationQueue.getDeque();


    }
}