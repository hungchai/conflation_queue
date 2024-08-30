package tomma.hft.conflatingqueue;

import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

class ConflatingQueueImplTest {
    private ConflatingQueueImpl<String, Long> conflationQueue;

    @BeforeEach
    public void init() {
        conflationQueue = new ConflatingQueueImpl<>();
    }

    @org.junit.jupiter.api.Test
    void offer() {

    }
}