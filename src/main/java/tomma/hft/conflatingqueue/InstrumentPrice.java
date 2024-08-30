package tomma.hft.conflatingqueue;

//import sun.misc.Contended;

import java.util.Objects;

class InstrumentPrice {
//    @Contended
    static class PriceValue<V> {
        enum State {UNUSED, UNCONFIRMED, CONFIRMED}
        private V value;
        volatile State state;

        public PriceValue() {
            this.state = State.UNUSED;
        }

        PriceValue<V> initializeWithUnconfirmed(final V value) {
            this.value = Objects.requireNonNull(value);
            this.state = State.UNCONFIRMED;
            return this;
        }
        PriceValue<V> initalizeWithUnused(final V value) {
            this.value = value;//nulls allowed here
            this.state = State.UNUSED;
            return this;
        }

        void confirmWith(final V value) {
            this.value = value;
            this.state = State.CONFIRMED;
        }

        boolean isNotInQueue() {
            return state == State.UNUSED;
        }

        void confirm() {
            this.state = State.CONFIRMED;
        }
        V awaitAndRelease() {
            awaitFinalState();
            return release();
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
            state = State.UNUSED;
            this.value = null;
            return released;
        }

    }
}