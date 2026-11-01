package io.zmux.internal.core;

/** What keeps a frame's data alive while a batch references it: a send block or a read buffer. */
record Pin(SendBlock block, ReadBuf buf) {
    static final Pin NONE = new Pin(null, null);

    boolean none() { return block == null && buf == null; }

    void ref() {
        if (block != null) block.ref();
        else if (buf != null) buf.ref();
    }

    void unref() {
        if (block != null) block.unref();
        else if (buf != null) buf.unref();
    }
}
