package io.zmux.internal;

import java.io.IOException;
import java.util.*;

final class SessionTerminalBookkeeping {
    private final Owner owner;
    private final long compactTerminalStateUnit;
    private final long hiddenControlRetainedMaxAgeNanos;
    private Map<Long, Tombstone> tombstones = new HashMap<>();
    private Deque<Long> tombstoneOrder = new ArrayDeque<>();
    private Map<Long, TerminalDataDisposition> markerOnlyUsedStreams = new HashMap<>();
    private ArrayList<MarkerRange> markerOnlyRanges = new ArrayList<>();
    private Deque<Long> hiddenTombstones = new ArrayDeque<>();
    private boolean markerOnlyRangeMode;
    private boolean asyncFailureScheduled;

    SessionTerminalBookkeeping(Owner owner, long compactTerminalStateUnit, long hiddenControlRetainedMaxAgeNanos) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.compactTerminalStateUnit = compactTerminalStateUnit;
        this.hiddenControlRetainedMaxAgeNanos = hiddenControlRetainedMaxAgeNanos;
    }

    private static TerminalDataDisposition tombstoneLateDataDisposition(Tombstone tombstone) {
        if (tombstone == null) {
            return null;
        }
        if (!tombstone.hasReceiveHalf()) {
            return new TerminalDataDisposition(LateDataAction.IGNORE, tombstone.lateDataCause());
        }
        return new TerminalDataDisposition(
                tombstone.gracefulReceiveClosed() ? LateDataAction.ABORT_CLOSED : LateDataAction.IGNORE,
                tombstone.lateDataCause()
        );
    }

    private static long saturatingAdd(long left, long right) {
        long sum = left + right;
        if (((left ^ sum) & (right ^ sum)) < 0) {
            return Long.MAX_VALUE;
        }
        return sum;
    }

    private static long saturatingMultiply(long value, long factor) {
        if (value <= 0L || factor <= 0L) {
            return 0L;
        }
        try {
            return Math.multiplyExact(value, factor);
        } catch (ArithmeticException arithmeticException) {
            return Long.MAX_VALUE;
        }
    }

    int hiddenControlStateRetainedLocked() {
        return this.hiddenTombstones.size();
    }

    int visibleTombstoneRetainedLocked() {
        return Math.max(0, this.tombstones.size() - this.hiddenControlStateRetainedLocked());
    }

    int markerOnlyRetainedLocked() {
        return this.markerOnlyMapCountLocked() + this.markerOnlyRanges.size();
    }

    int markerOnlyRangeCountLocked() {
        return this.markerOnlyRanges.size();
    }

    long trackedRetainedStateMemoryLocked() {
        long retainedUnit = this.owner.retainedStateUnitLocked();
        long total = saturatingMultiply(this.hiddenControlStateRetainedLocked(), retainedUnit);
        total = saturatingAdd(
                total,
                saturatingMultiply(this.visibleTombstoneRetainedLocked(), this.compactTerminalStateUnit)
        );
        total = saturatingAdd(
                total,
                saturatingMultiply(this.markerOnlyRetainedLocked(), this.compactTerminalStateUnit)
        );
        return total;
    }

    void putTombstoneLocked(long streamId, Tombstone tombstone) {
        Tombstone previous = this.tombstones.get(streamId);
        if (previous == null) {
            this.tombstoneOrder.addLast(streamId);
        }
        this.tombstones.put(streamId, tombstone);
        if (tombstone.hidden()) {
            if (previous == null || !previous.hidden()) {
                this.hiddenTombstones.addLast(streamId);
            }
        } else if (previous != null && previous.hidden()) {
            this.hiddenTombstones.removeFirstOccurrence(streamId);
        }
        this.reapExcessTombstonesLocked();
        this.enforceHiddenControlStateBudgetLocked(tombstone.createdAtNanos());
        this.enforceTerminalBookkeepingMemoryCapLocked();
    }

    void retainHiddenAbortTombstoneLocked(long streamId, long code, String reason, long nowNanos) {
        Tombstone tombstone = this.tombstones.get(streamId);
        if (tombstone != null && tombstone.hidden()) {
            return;
        }
        this.putTombstoneLocked(streamId, Tombstone.hidden(code, reason, nowNanos, LateDataCause.ABORT));
    }

    TerminalDataDisposition terminalDataDispositionForLocked(long streamId) {
        Tombstone tombstone = this.tombstones.get(streamId);
        if (tombstone != null) {
            return tombstoneLateDataDisposition(tombstone);
        }
        TerminalDataDisposition disposition = this.markerOnlyUsedStreams.get(streamId);
        if (disposition != null) {
            return disposition;
        }
        return this.markerRangeDispositionForLocked(streamId);
    }

    boolean hasTerminalMarkerLocked(long streamId) {
        return this.terminalDataDispositionForLocked(streamId) != null;
    }

    void clear() {
        this.tombstones = new HashMap<>();
        this.tombstoneOrder = new ArrayDeque<>();
        this.markerOnlyUsedStreams = new HashMap<>();
        this.markerOnlyRanges = new ArrayList<>();
        this.hiddenTombstones = new ArrayDeque<>();
        this.markerOnlyRangeMode = false;
        this.asyncFailureScheduled = false;
    }

    private void retainMarkerOnlyUsedStreamLocked(long streamId, Tombstone tombstone) {
        TerminalDataDisposition disposition = tombstoneLateDataDisposition(tombstone);
        if (disposition == null) {
            return;
        }
        if (this.markerOnlyRangeMode) {
            this.upsertMarkerRangeLocked(streamId, disposition);
            this.enforceMarkerOnlyUsedStreamLimitLocked();
            return;
        }
        this.markerOnlyUsedStreams.put(streamId, disposition);
        this.compactMarkerOnlyRangesLocked();
        this.enforceMarkerOnlyUsedStreamLimitLocked();
    }

    private int markerOnlyMapCountLocked() {
        if (this.markerOnlyUsedStreams.isEmpty() || this.tombstones.isEmpty()) {
            return this.markerOnlyUsedStreams.size();
        }
        int count = 0;
        for (Long streamId : this.markerOnlyUsedStreams.keySet()) {
            if (!this.tombstones.containsKey(streamId)) {
                count++;
            }
        }
        return count;
    }

    private void compactMarkerOnlyRangesLocked() {
        int markerOnlyCount = this.markerOnlyMapCountLocked();
        if (markerOnlyCount <= this.owner.markerOnlyUsedStreamHardCapLocked() && markerOnlyCount < 64) {
            return;
        }
        if (markerOnlyCount == 0) {
            return;
        }
        ArrayList<Long> streamIds = new ArrayList<>(markerOnlyCount);
        for (Long streamId : this.markerOnlyUsedStreams.keySet()) {
            if (!this.tombstones.containsKey(streamId)) {
                streamIds.add(streamId);
            }
        }
        if (streamIds.isEmpty()) {
            return;
        }
        Collections.sort(streamIds);
        for (Long streamId : streamIds) {
            TerminalDataDisposition disposition = this.markerOnlyUsedStreams.remove(streamId);
            if (disposition != null) {
                this.upsertMarkerRangeLocked(streamId, disposition);
            }
        }
        this.markerOnlyRangeMode = true;
    }

    private void enforceMarkerOnlyUsedStreamLimitLocked() {
        int cap = this.owner.markerOnlyUsedStreamHardCapLocked();
        int count = this.markerOnlyRetainedLocked();
        if (count <= cap) {
            return;
        }
        this.requestAsyncSessionFailureLocked(this.owner.sessionInternalError(
                "compact terminal state",
                "marker-only used-stream cap exceeded: count=" + count + " cap=" + cap
        ));
    }

    private TerminalDataDisposition markerRangeDispositionForLocked(long streamId) {
        int index = firstMarkerRangeStartingAfter(streamId);
        if (index <= 0) {
            return null;
        }
        MarkerRange range = this.markerOnlyRanges.get(index - 1);
        return range.contains(streamId) ? range.disposition : null;
    }

    private int firstMarkerRangeStartingAfter(long streamId) {
        int low = 0;
        int high = this.markerOnlyRanges.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (this.markerOnlyRanges.get(mid).start > streamId) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    private void upsertMarkerRangeLocked(long streamId, TerminalDataDisposition disposition) {
        int index = firstMarkerRangeStartingAfter(streamId);
        if (index > 0 && this.markerOnlyRanges.get(index - 1).contains(streamId)) {
            this.setContainedMarkerRangeLocked(index - 1, streamId, disposition);
            return;
        }
        this.markerOnlyRanges.add(index, new MarkerRange(streamId, streamId, disposition));
        this.mergeMarkerRangesAroundLocked(index);
    }

    private void setContainedMarkerRangeLocked(int index, long streamId, TerminalDataDisposition disposition) {
        MarkerRange current = this.markerOnlyRanges.get(index);
        if (current.sameDisposition(disposition)) {
            return;
        }
        this.markerOnlyRanges.remove(index);
        int insert = index;
        if (current.start < streamId) {
            this.markerOnlyRanges.add(insert++, new MarkerRange(current.start, streamId - 4L, current.disposition));
        }
        int inserted = insert;
        this.markerOnlyRanges.add(insert++, new MarkerRange(streamId, streamId, disposition));
        if (streamId < current.end) {
            this.markerOnlyRanges.add(insert, new MarkerRange(streamId + 4L, current.end, current.disposition));
        }
        this.mergeMarkerRangesAroundLocked(inserted);
    }

    private void mergeMarkerRangesAroundLocked(int index) {
        while (index > 0 && MarkerRange.mergeable(this.markerOnlyRanges.get(index - 1), this.markerOnlyRanges.get(index))) {
            MarkerRange previous = this.markerOnlyRanges.get(index - 1);
            MarkerRange current = this.markerOnlyRanges.remove(index);
            previous.end = Math.max(previous.end, current.end);
            --index;
        }
        while (index + 1 < this.markerOnlyRanges.size()
                && MarkerRange.mergeable(this.markerOnlyRanges.get(index), this.markerOnlyRanges.get(index + 1))) {
            MarkerRange current = this.markerOnlyRanges.get(index);
            MarkerRange next = this.markerOnlyRanges.remove(index + 1);
            current.end = Math.max(current.end, next.end);
        }
    }

    private void enforceTerminalBookkeepingMemoryCapLocked() {
        if (this.owner.sessionTerminalLocked()) {
            return;
        }
        this.enforceMarkerOnlyUsedStreamLimitLocked();
        IOException memoryErr = this.owner.sessionMemoryCapErrorLocked("compact terminal state");
        if (memoryErr != null) {
            this.requestAsyncSessionFailureLocked(memoryErr);
        }
    }

    void reapExcessTombstonesLocked() {
        int limit = this.owner.tombstoneLimitLocked();
        while (this.tombstones.size() > limit) {
            if (!this.reapOldestTombstoneLocked(false)) {
                return;
            }
        }
        while (!this.tombstones.isEmpty()
                && this.owner.trackedSessionMemoryLocked() > this.owner.sessionMemoryHardCapLocked()) {
            if (!this.reapOldestTombstoneLocked(true)) {
                return;
            }
        }
    }

    private boolean reapOldestTombstoneLocked(boolean memoryPressure) {
        Long oldest = this.oldestTombstoneIdLocked();
        if (oldest == null) {
            return false;
        }
        Tombstone tombstone = this.tombstones.get(oldest);
        if (tombstone == null) {
            return false;
        }
        if (memoryPressure && !tombstone.hidden() && this.visibleTombstoneRetainedLocked() <= 1) {
            return false;
        }
        return this.reapTombstoneLocked(oldest);
    }

    private boolean reapNewestHiddenControlStateLocked() {
        int beforeSize = this.hiddenTombstones.size();
        Long newest = this.newestHiddenTombstoneIdLocked();
        if (newest == null) {
            return this.hiddenTombstones.size() < beforeSize;
        }
        return this.reapTombstoneLocked(newest);
    }

    private Long oldestTombstoneIdLocked() {
        while (true) {
            Long streamId = this.tombstoneOrder.peekFirst();
            if (streamId == null) {
                return null;
            }
            if (this.tombstones.containsKey(streamId)) {
                return streamId;
            }
            this.tombstoneOrder.pollFirst();
        }
    }

    private Long newestHiddenTombstoneIdLocked() {
        boolean cleanedStaleEntry = false;
        while (true) {
            Long streamId = this.hiddenTombstones.peekLast();
            if (streamId == null) {
                return null;
            }
            Tombstone tombstone = this.tombstones.get(streamId);
            if (tombstone != null && tombstone.hidden()) {
                return cleanedStaleEntry ? null : streamId;
            }
            this.hiddenTombstones.pollLast();
            cleanedStaleEntry = true;
        }
    }

    private boolean reapTombstoneLocked(long streamId) {
        Tombstone removed = this.tombstones.remove(streamId);
        if (removed == null) {
            this.tombstoneOrder.removeFirstOccurrence(streamId);
            this.hiddenTombstones.removeFirstOccurrence(streamId);
            return false;
        }
        this.tombstoneOrder.removeFirstOccurrence(streamId);
        this.hiddenTombstones.removeFirstOccurrence(streamId);
        this.retainMarkerOnlyUsedStreamLocked(streamId, removed);
        return true;
    }

    void reapExpiredHiddenControlStateLocked(long nowNanos) {
        if (this.hiddenTombstones.isEmpty()) {
            return;
        }
        long effectiveNow = nowNanos > 0L ? nowNanos : System.nanoTime();
        Long streamId;
        while ((streamId = this.hiddenTombstones.peekFirst()) != null) {
            Tombstone tombstone = this.tombstones.get(streamId);
            if (tombstone == null || !tombstone.hidden()) {
                this.hiddenTombstones.removeFirst();
                continue;
            }
            if (tombstone.createdAtNanos() == 0L
                    || !RuntimeFlow.elapsedExceeds(
                    effectiveNow,
                    tombstone.createdAtNanos(),
                    this.hiddenControlRetainedMaxAgeNanos
            )) {
                return;
            }
            this.reapTombstoneLocked(streamId);
        }
    }

    private void enforceHiddenControlStateBudgetLocked(long nowNanos) {
        this.reapExpiredHiddenControlStateLocked(nowNanos);
        int cap = this.owner.hiddenControlStateHardCapLocked();
        while (this.hiddenTombstones.size() > cap) {
            if (!this.reapNewestHiddenControlStateLocked()) {
                return;
            }
        }
        while (saturatingMultiply(this.hiddenControlStateRetainedLocked(), this.owner.retainedStateUnitLocked())
                > this.owner.sessionMemoryHardCapLocked()) {
            if (!this.reapNewestHiddenControlStateLocked()) {
                return;
            }
        }
    }

    private void requestAsyncSessionFailureLocked(IOException error) {
        if (error == null || this.owner.sessionTerminalLocked() || this.asyncFailureScheduled) {
            return;
        }
        this.asyncFailureScheduled = true;
        this.owner.failSessionAsync(error);
    }

    enum LateDataAction {
        IGNORE,
        ABORT_CLOSED,
        ABORT_STATE
    }

    interface Owner {
        int tombstoneLimitLocked();

        int markerOnlyUsedStreamHardCapLocked();

        int hiddenControlStateHardCapLocked();

        long trackedSessionMemoryLocked();

        long sessionMemoryHardCapLocked();

        long retainedStateUnitLocked();

        boolean sessionTerminalLocked();

        IOException sessionInternalError(String operation, String message);

        IOException sessionMemoryCapErrorLocked(String operation);

        void failSession(IOException error);

        void failSessionAsync(IOException error);
    }

    static final class TerminalDataDisposition {
        private final LateDataAction action;
        private final LateDataCause cause;

        TerminalDataDisposition(LateDataAction action, LateDataCause cause) {
            this.action = action;
            this.cause = cause;
        }

        LateDataAction action() {
            return action;
        }

        LateDataCause cause() {
            return cause;
        }

        boolean sameAs(TerminalDataDisposition other) {
            return other != null && action == other.action && cause == other.cause;
        }
    }

    private static final class MarkerRange {
        private final long start;
        private final TerminalDataDisposition disposition;
        private long end;

        MarkerRange(long start, long end, TerminalDataDisposition disposition) {
            this.start = start;
            this.end = end;
            this.disposition = disposition;
        }

        private static boolean mergeable(MarkerRange left, MarkerRange right) {
            if (left == null || right == null || !left.sameDisposition(right.disposition)) {
                return false;
            }
            if (left.start % 4L != right.start % 4L) {
                return false;
            }
            return left.end + 4L >= right.start && right.end + 4L >= left.start;
        }

        boolean contains(long streamId) {
            return streamId >= start && streamId <= end && (streamId - start) % 4L == 0L;
        }

        boolean sameDisposition(TerminalDataDisposition other) {
            return disposition != null && disposition.sameAs(other);
        }
    }

    static final class Tombstone {
        private final boolean hasReceiveHalf;
        private final boolean gracefulReceiveClosed;
        private final long terminalCode;
        private final String terminalReason;
        private final LateDataCause lateDataCause;
        private final boolean hidden;
        private final long createdAtNanos;

        Tombstone(boolean hasReceiveHalf,
                  boolean gracefulReceiveClosed,
                  long terminalCode,
                  String terminalReason,
                  LateDataCause lateDataCause,
                  boolean hidden,
                  long createdAtNanos) {
            this.hasReceiveHalf = hasReceiveHalf;
            this.gracefulReceiveClosed = gracefulReceiveClosed;
            this.terminalCode = terminalCode;
            this.terminalReason = terminalReason;
            this.lateDataCause = lateDataCause;
            this.hidden = hidden;
            this.createdAtNanos = createdAtNanos;
        }

        Tombstone(boolean hasReceiveHalf,
                  boolean gracefulReceiveClosed,
                  long terminalCode,
                  String terminalReason,
                  LateDataCause lateDataCause) {
            this(hasReceiveHalf, gracefulReceiveClosed, terminalCode, terminalReason, lateDataCause, false, 0L);
        }

        Tombstone(boolean hasReceiveHalf,
                  boolean gracefulReceiveClosed,
                  long terminalCode,
                  String terminalReason,
                  boolean hidden,
                  long createdAtNanos) {
            this(
                    hasReceiveHalf,
                    gracefulReceiveClosed,
                    terminalCode,
                    terminalReason,
                    LateDataCause.NONE,
                    hidden,
                    createdAtNanos
            );
        }

        static Tombstone hidden(long terminalCode, String terminalReason, long createdAtNanos, LateDataCause lateDataCause) {
            return new Tombstone(
                    false,
                    false,
                    terminalCode,
                    terminalReason == null ? "" : terminalReason,
                    lateDataCause == null ? LateDataCause.NONE : lateDataCause,
                    true,
                    createdAtNanos
            );
        }

        boolean hasReceiveHalf() {
            return hasReceiveHalf;
        }

        boolean gracefulReceiveClosed() {
            return gracefulReceiveClosed;
        }

        long terminalCode() {
            return terminalCode;
        }

        String terminalReason() {
            return terminalReason;
        }

        LateDataCause lateDataCause() {
            return lateDataCause;
        }

        boolean hidden() {
            return hidden;
        }

        long createdAtNanos() {
            return createdAtNanos;
        }
    }
}
