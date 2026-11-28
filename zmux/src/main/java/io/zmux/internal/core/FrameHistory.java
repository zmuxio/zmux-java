package io.zmux.internal.core;

import io.zmux.internal.buffer.WriteWorkspace;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;

/** Prepaid retransmission descriptors for one outbound lane. Payload lives in the send queue. */
final class FrameHistory implements Iterable<Retained> {
    static final int SLOTS = Session.DEFAULT_BATCH / 256;
    /**
     * Per slot: 192 bytes for a frame entry/descriptor and its indexes.
     * Another 224 covers bounded ACK tuples, activity marks, samples and array growth.
     */
    static final long WORKSPACE = SLOTS * (192L + 224) + 4096;

    private ArrayDeque<Retained> entries;
    private WriteWorkspace backing;

    void attach(WriteWorkspace workspace) {
        if (backing != null) return;
        if (workspace.tryTake()) backing = workspace;
    }

    int room() { return SLOTS - (entries == null ? 0 : entries.size()); }

    void add(Retained entry) {
        if (room() == 0) throw new IllegalStateException("frame history space was not reserved");
        if (entries == null) entries = new ArrayDeque<>(SLOTS);
        entries.addLast(entry);
    }

    Retained first() { return entries == null ? null : entries.peekFirst(); }

    void removeFirst() { entries.removeFirst().forgetPrefix(); }

    void retire() {
        if (entries != null) for (Retained entry : entries) entry.forgetPrefix();
        entries = null;
        if (backing != null) {
            backing.giveBack(null, false);
            backing = null;
        }
    }

    @Override public Iterator<Retained> iterator() {
        return entries == null ? Collections.emptyIterator() : entries.iterator();
    }
}
