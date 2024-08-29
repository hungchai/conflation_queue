package tomma.hft.conflatingqueue;

import sun.misc.Contended;

class InstrumentPrice {
    @Contended
    static class PriceEntry<V> {
        enum State {NOTINQUEUE, UNCONFIRMED, CONFIRMED}
        private V value;
        volatile State state;

        public PriceEntry(V price) {
            this.value = price;
            state = State.UNCONFIRMED;
        }

        boolean isInQueue() {
            return state == State.INQUEUE;
        }

        State awaitFinalState() {
            State s;
            do {
                s = state;
            } while (s == State.UNCONFIRMED);
            return s;
        }

        V release() {
            final V released = value;
            state = State.;
            this.value = null;
            return released;
        }

    }
}