package io.zmux;

/** Direction-wide local carrier accounting. A receipt remains readable after the direction finishes. */
public final class SpliceSettlement {
    /** The disposition of accepted bytes at the time of observation. */
    public record Snapshot(long acceptedBytes, long firstCarrierBytes, long unsentBytes, long pendingBytes,
                           long unsentRefundedBytes) {}

    private long nativeCount, recordCount, encoded;
    synchronized void encoded(int bytes) { encoded += bytes; }
    synchronized long encodeBytes() { return encoded; }
    synchronized void dispatched(boolean nativeChannel) {
        if (nativeChannel) nativeCount++; else recordCount++;
    }
    synchronized long nativeCount() { return nativeCount; }
    synchronized long recordCount() { return recordCount; }

    private long accepted, handedOff, unsent, unusedRefunds, unsentRefunds;

    SpliceSettlement() {}

    synchronized void enqueue(long count) { accepted += count; }
    synchronized void handoff(long count) { handedOff += count; }
    synchronized long preAdmissionRefundedBytes() { return unusedRefunds; }
    synchronized void unused(long count) { unusedRefunds += count; }
    synchronized void finish(long unaccepted, long missing, boolean funded) {
        unsent += missing;
        if (funded) {
            unusedRefunds += unaccepted;
            unsentRefunds += missing;
        }
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(accepted, handedOff, unsent, accepted - handedOff - unsent, unsentRefunds);
    }
}
