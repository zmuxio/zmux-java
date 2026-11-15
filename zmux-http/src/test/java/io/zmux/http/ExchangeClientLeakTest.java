package io.zmux.http;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.zmux.Chunk;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The client's downlink buffers have one owner, whichever way the carrier ends. */
class ExchangeClientLeakTest {
    private static final int PIECES = 4;

    /** Answers every request of a client itself: four full DOWN pieces, the last with FIN. */
    private static byte[] respond(byte[] body, int size) {
        if (body.length < Exchange.HDR) return null;
        var q = Exchange.parseHeader(ByteBuffer.wrap(body));
        var key = Exchange.key(ExchangeTest.PSK, q.cid());
        byte[] data = q.kind() == Exchange.KIND_DOWN && q.seq() < PIECES ? new byte[size] : new byte[0];
        int flags = q.kind() == Exchange.KIND_DOWN && q.seq() >= PIECES - 1 ? Exchange.FLAG_FIN : 0;
        if (q.kind() == Exchange.KIND_DOWN && q.seq() >= PIECES) data = new byte[0];
        ByteBuffer h = Exchange.responseHeader(key, q, flags, ByteBuffer.wrap(data));
        byte[] out = new byte[h.remaining() + data.length];
        h.get(out, 0, h.remaining());
        System.arraycopy(data, 0, out, out.length - data.length, data.length);
        return out;
    }

    /**
     * A completed carrier keeps the pieces its reader has no room for yet; when the reader closes
     * only after the client's event loop terminated, they are released all the same.
     */
    @Test void heldPiecesAfterTheLoopTerminated() throws Exception {
        var c = ExchangeTest.cfg().holdTime(Duration.ofMillis(200)).build();
        int size = c.downLimit();
        try (var f = Fixture.plain(b -> b.exchange(c));
             var px = new TestProxy(f.base, TestProxy.Profile.bufferingCdn(0),
                     new TestProxy.Faults(0, 0, 0, 0, 0, 0, body -> respond(body, size)), 1)) {
            HttpCarrierClient hc = HttpCarrierClient.builder().timeout(Duration.ofSeconds(5)).build();
            PipeIo p = hc.exchangePipe(px.base.toString(), c);
            ExchangeClient x = hc.exchanges.iterator().next();
            List<Chunk> taken = new CopyOnWriteArrayList<>();
            p.reader().start(new PipeReader.Sink() {
                @Override public void onData(ByteBuffer data, Runnable release) { throw new AssertionError("chunks expected"); }
                // Never consumed: the receive memory fills, and the rest of the pieces stay with the carrier.
                @Override public void onChunk(Chunk chunk, Runnable consumed) { taken.add(chunk); }
                @Override public void onEnd() {}
                @Override public void onError(Throwable cause) {}
            });
            p.writer().close();
            ExchangeTest.waitFor(() -> !hc.exchanges.contains(x), "the carrier completes");
            List<ByteBuf> held = x.heldPieces().get(5, TimeUnit.SECONDS);
            assertEquals(PIECES - taken.size(), held.size());
            assertTrue(!held.isEmpty(), "pieces beyond the receive memory are held");
            hc.close();
            assertTrue(hc.group().terminationFuture().await(5, TimeUnit.SECONDS));
            try {
                assertDoesNotThrow(() -> p.reader().close());
                // Released by the loop's termination listener, which runs on Netty's global executor.
                ExchangeTest.waitFor(() -> held.stream().allMatch(b -> b.refCnt() == 0), "the held pieces are released");
            } finally {
                for (Chunk ch : taken) ch.close();
            }
        }
    }

    /**
     * A response cut halfway leaves its partial body in the connection's pipeline; when the
     * client's group terminates meanwhile, the termination releases it.
     */
    @Test void partialBodyWhenTheGroupTerminates() throws Exception {
        List<java.net.Socket> conns = new CopyOnWriteArrayList<>();
        try (var server = new java.net.ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            Thread t = new Thread(() -> {
                try {
                    while (true) {
                        java.net.Socket s = server.accept();
                        conns.add(s);
                        // Half of the declared body, then nothing: the request itself is never read.
                        s.getOutputStream().write("HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                        s.getOutputStream().write(new byte[100]);
                        s.getOutputStream().flush();
                    }
                } catch (java.io.IOException closed) {
                    // The server socket closed.
                }
            }, "half-response");
            t.setDaemon(true);
            t.start();
            var c = ExchangeTest.cfg().build();
            HttpCarrierClient hc = HttpCarrierClient.builder().timeout(Duration.ofSeconds(5)).build();
            PipeIo p = hc.exchangePipe("http://127.0.0.1:" + server.getLocalPort() + "/x", c);
            p.reader().start(new PipeReader.Sink() {
                @Override public void onData(ByteBuffer data, Runnable release) { release.run(); }
                @Override public void onEnd() {}
                @Override public void onError(Throwable cause) {}
            });
            ExchangeTest.waitFor(() -> !hc.partials.isEmpty(), "a partial body is held");
            List<ByteBuf> partial = List.copyOf(hc.partials.values());
            hc.close();
            assertTrue(hc.group().terminationFuture().await(5, TimeUnit.SECONDS));
            ExchangeTest.waitFor(() -> partial.stream().allMatch(b -> b.refCnt() == 0) && hc.partials.isEmpty(), "the partial bodies are released");
        } finally {
            for (java.net.Socket s : conns) s.close();
        }
    }
}
