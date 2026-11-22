package io.zmux.http;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.*;

import io.zmux.Admission;
import io.zmux.PipeOptions;
import io.zmux.Takeover;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A streaming request carrier decided on the start of its body with a fallback is taken over by the listener's
 * fallback as the full-duplex byte stream of its request and response bodies, the response head written by the
 * adapter, over HTTP/1.1 and HTTP/2 alike; closing the takeover's sending half ends the response while the request
 * body reads on, and the request ends once both halves are closed. A request decided on its head stays a request.
 */
@Timeout(60)
class RequestTakeoverTest {
    /** What r reads: each chunk as text, then "end" or "error". */
    static BlockingQueue<String> reading(PipeReader r) {
        var got = new LinkedBlockingQueue<String>();
        r.start(new PipeReader.Sink() {
            @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) {
                ByteBuffer b = chunk.buffer();
                byte[] m = new byte[b.remaining()];
                b.get(m);
                chunk.close();
                consumed.run();
                got.add(new String(m, US_ASCII));
            }
            @Override public void onEnd() { got.add("end"); }
            @Override public void onError(Throwable cause) { got.add("error " + cause); }
        });
        return got;
    }

    static void write(PipeWriter w, String s) {
        var done = new CompletableFuture<Void>();
        w.write(OutboundBatch.of(ByteBuffer.wrap(s.getBytes(US_ASCII))), new PipeWriter.Completion() {
            @Override public void completed() { done.complete(null); }
            @Override public void failed(Throwable cause) { done.completeExceptionally(cause); }
        });
        done.orTimeout(5, TimeUnit.SECONDS).join();
    }

    /** Reads q until the concatenated text has n characters (TCP may split it). */
    static String take(BlockingQueue<String> q, int n) throws InterruptedException {
        var b = new StringBuilder();
        while (b.length() < n) {
            String s = q.poll(5, TimeUnit.SECONDS);
            assertNotNull(s, "only " + b);
            b.append(s);
        }
        return b.toString();
    }

    @Test void aRequestCarrierDecidedOnItsBodyIsTakenOverFullDuplex() throws Exception {
        // HS-15.
        var stages = new LinkedBlockingQueue<Admission.Stage>();
        Admission.Decider decider = Admission.Decider.immediate((info, prefix) -> {
            stages.add(info.stage());
            return Admission.Decision.takeover("tunnel");
        });
        var taken = new LinkedBlockingQueue<Takeover>();
        try (var f = new Fixture(b -> b, b -> b.admission(Admission.builder(4, decider).peekTimeout(Duration.ofSeconds(10)).build())
                .fallback(t -> { taken.add(t); assertTrue(t.detach()); return null; }), b -> b.streaming(), false, null)) {
            for (boolean h1 : new boolean[] {true, false}) {
                try (var c = f.client(b -> h1 ? b.http1() : b.http2())) {
                    String v = h1 ? "HTTP/1.1" : "HTTP/2";
                    PipeIo client = c.pipe(f.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null);
                    var down = reading(client.reader());
                    write(client.writer(), "TUN1hello");
                    Takeover t = taken.poll(5, TimeUnit.SECONDS);
                    assertNotNull(t, v);
                    assertEquals(Admission.Stage.CARRIER, stages.poll(5, TimeUnit.SECONDS), v + ": decided on its body");
                    assertEquals("tunnel", t.value());
                    assertTrue(t.messages().isEmpty());
                    // Served after the fallback returned (detached).
                    var up = reading(t.pipe().reader());
                    assertEquals("TUN1hello", take(up, 9), v + ": the body bytes read replayed");
                    write(t.pipe().writer(), "echo:TUN1hello");
                    assertEquals("echo:TUN1hello", take(down, 14), v + ": the response body");
                    t.pipe().writer().close();
                    assertEquals("end", down.poll(5, TimeUnit.SECONDS), v + ": the response ended");
                    write(client.writer(), "more");
                    assertEquals("more", take(up, 4), v + ": the request body reads on after the half-close");
                    client.writer().close();
                    assertEquals("end", up.poll(5, TimeUnit.SECONDS), v + ": the request body ended");
                    t.pipe().reader().close();
                    client.reader().close();
                }
            }
            var st = f.listener.stats().admission();
            assertEquals(0, st.fallback());
            assertEquals(2, st.takeover());
        }
    }
}
