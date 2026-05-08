package io.zmux.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SessionOutboundQueueBookkeepingTest {
    @Test
    void replacingPendingControlWithSmallerBucketWakesMemoryWaiters() {
        TestOwner owner = new TestOwner(10L);
        SessionOutboundQueueBookkeeping bookkeeping = new SessionOutboundQueueBookkeeping(owner);
        owner.bookkeeping = bookkeeping;

        assertTrue(bookkeeping.replacePendingControlBytesLocked(0L, 10L));
        assertEquals(0, owner.notifications);

        assertTrue(bookkeeping.replacePendingControlBytesLocked(10L, 5L));

        assertEquals(1, owner.notifications);
    }

    @Test
    void replacingPendingPriorityWithSmallerBucketWakesMemoryWaiters() {
        TestOwner owner = new TestOwner(10L);
        SessionOutboundQueueBookkeeping bookkeeping = new SessionOutboundQueueBookkeeping(owner);
        owner.bookkeeping = bookkeeping;

        assertEquals(
                SessionOutboundQueueBookkeeping.PendingPriorityReplaceResult.ACCEPTED,
                bookkeeping.replacePendingPriorityBytesLocked(0L, 10L)
        );
        assertEquals(0, owner.notifications);

        assertEquals(
                SessionOutboundQueueBookkeeping.PendingPriorityReplaceResult.ACCEPTED,
                bookkeeping.replacePendingPriorityBytesLocked(10L, 5L)
        );

        assertEquals(1, owner.notifications);
    }

    private static final class TestOwner implements SessionOutboundQueueBookkeeping.Owner {
        private final long threshold;
        private SessionOutboundQueueBookkeeping bookkeeping;
        private int notifications;

        private TestOwner(long threshold) {
            this.threshold = threshold;
        }

        @Override
        public long trackedSessionMemoryLocked() {
            if (bookkeeping == null) {
                return 0L;
            }
            return bookkeeping.pendingControlBytesLocked() + bookkeeping.pendingPriorityBytesLocked();
        }

        @Override
        public boolean sessionMemoryWakeNeededLocked(long previousTracked) {
            return RuntimeFlow.memoryWakeNeeded(previousTracked, trackedSessionMemoryLocked(), threshold);
        }

        @Override
        public long sessionMemoryHardCapLocked() {
            return Long.MAX_VALUE;
        }

        @Override
        public long pendingControlBytesBudgetLocked() {
            return Long.MAX_VALUE;
        }

        @Override
        public long pendingPriorityBytesBudgetLocked() {
            return Long.MAX_VALUE;
        }

        @Override
        public void notifyLockWaiters() {
            notifications++;
        }
    }
}
