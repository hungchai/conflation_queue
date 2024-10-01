package tomma.hft.conflatingqueue;

import java.util.Objects;

public class QueueValueWrapper {
    static class QueueValue<V> {
        private V value;

        public QueueValue() {
            // No explicit state handling
        }

        QueueValueWrapper.QueueValue<V> initializeWithUnconfirmed(final V value) {
            this.value = Objects.requireNonNull(value);
            return this;
        }

        void confirmWith(final V value) {
            this.value = value;
        }

        void confirm() {
            // No explicit state change
        }

        V awaitAndRelease() {
            return release();
        }

        V release() {
            V released = value;
            return released;
        }
    }

}
