package io.zmux.internal.core;

import io.zmux.internal.buffer.ReadBuf;

/** The backing a batch keeps across carrier handoff. */
record Pin(ReadBuf buf) {
    static final Pin NONE = new Pin(null);

    boolean none() { return buf == null; }

    void ref() { if (buf != null) buf.ref(); }
    void unref() { if (buf != null) buf.unref(); }
}
