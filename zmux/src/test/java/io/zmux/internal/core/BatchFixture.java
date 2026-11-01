package io.zmux.internal.core;

/** A real referenced payload for driver lifetime tests, and flat views of a batch. */
public final class BatchFixture {
    private BatchFixture() {}
    public static void pin(Batch batch, ReadBuf payload) {
        Pin pin = new Pin(null, payload);
        batch.pin(pin);
        batch.refSeg(payload.data().slice(payload.data().position(), 16));
    }

    /** The batch's bytes as one array. */
    public static byte[] flat(Batch batch) {
        java.nio.ByteBuffer out = java.nio.ByteBuffer.allocate(batch.bytes());
        for (int i = 0; i < batch.segmentCount(); i++) out.put(batch.segment(i));
        return out.array();
    }

    /** One buffer per record (message framing). */
    public static java.nio.ByteBuffer[] messages(Batch batch) {
        var out = new java.nio.ByteBuffer[batch.messageCount()];
        for (int m = 0, from = 0; m < out.length; m++) {
            int to = batch.messageEnd(m), n = 0;
            for (int i = from; i < to; i++) n += batch.segment(i).remaining();
            var b = java.nio.ByteBuffer.allocate(n);
            for (int i = from; i < to; i++) b.put(batch.segment(i));
            out[m] = b.flip();
            from = to;
        }
        return out;
    }
}
