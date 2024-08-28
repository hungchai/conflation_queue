package tomma.hft.conflatingqueue;

import sun.misc.Contended;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static tomma.hft.conflatingqueue.PriceEntry.InstrumentPrice.State.NOT_INQUEUE;

class PriceEntry {
    @Contended
    static class InstrumentPrice {
        enum State {INQUEUE, NOT_INQUEUE}
        private long price;
        volatile State state;

        public InstrumentPrice(long price) {
            this.price = price;
            state = NOT_INQUEUE;
        }

    }
}