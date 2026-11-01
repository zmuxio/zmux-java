package io.zmux.carrier;

import java.nio.ByteBuffer;

/** {@link OutboundBatch#of}: caller buffers, one record each. */
final class Buffers implements OutboundBatch {
    private static final Retained NOTHING = () -> {};
    private final ByteBuffer[] data;
    private final int bytes;
    private boolean copied;

    Buffers(ByteBuffer[] messages) {
        data = messages.clone();
        int n = 0;
        for (ByteBuffer b : data) n = Math.addExact(n, b.remaining());
        bytes = n;
    }

    @Override public int bytes() { return bytes; }
    @Override public int segmentCount() { return data.length; }
    @Override public ByteBuffer segment(int i) { return data[i].asReadOnlyBuffer(); }

    @Override
    public ByteBuffer[] segments(ByteBuffer[] reuse) {
        ByteBuffer[] out = reuse != null && reuse.length >= data.length ? reuse : new ByteBuffer[data.length];
        for (int i = 0; i < data.length; i++) out[i] = segment(i);
        return out;
    }

    @Override public int messageCount() { return data.length; }
    @Override public int messageEnd(int m) { return m + 1; }

    @Override
    public synchronized Retained retain() {
        if (!copied) {
            copied = true;
            for (int i = 0; i < data.length; i++) {
                ByteBuffer c = ByteBuffer.allocate(data[i].remaining());
                c.put(data[i].duplicate()).flip();
                data[i] = c;
            }
        }
        return NOTHING;
    }
}
