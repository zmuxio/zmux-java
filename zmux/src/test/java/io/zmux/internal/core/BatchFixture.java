package io.zmux.internal.core;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.testutil.Json;
import io.zmux.testutil.SpecFixtures;
import java.nio.file.Files;

/** A real referenced payload for driver lifetime tests, and flat views of a batch. */
public final class BatchFixture {
    private BatchFixture() {}

    public record Failover(Session session, Stream stream, long now) {}

    /** Two usable paths with unacknowledged stream bytes on the primary. */
    public static Failover beforeReinjection() throws Exception {
        var root = SpecFixtures.root().resolve("fixtures");
        var profiles = Json.object(Files.readString(root.resolve("profiles.json")));
        var lines = TraceTest.traceLines(root.resolve("traces/multipath/failover/suspect_reinjects_and_probes.ndjson"));
        var trace = new TraceRunner(Json.object(lines.getFirst()), profiles);
        for (int step = 1; step <= 15; step++) trace.step(Json.object(lines.get(step)));
        return new Failover(trace.s, trace.streams.get("a"), trace.now);
    }

    public static void pin(Batch batch, ReadBuf payload) {
        Pin pin = new Pin(payload);
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
