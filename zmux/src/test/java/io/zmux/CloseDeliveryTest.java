package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A coded close over TCP and TLS delivers the data written before it, then the code. */
@Timeout(60)
class CloseDeliveryTest {

    /**
     * The client writes 1 MiB on one stream and a few bytes on a second it leaves open, then
     * closes with code and reason and waits for the whole end: the server reads the 1 MiB in
     * order, then ends with ApplicationClosed(code, reason, remote).
     */
    static void closeDelivers(ZmuxSession client, ZmuxSession server, long code, String reason) throws Exception {
        byte[] data = new byte[1 << 20];
        new java.util.Random(code).nextBytes(data);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var st = client.openStream();
            st.write(data);
            var second = client.openStream();
            second.write(new byte[] {1, 2, 3});
            var in = server.acceptStream(Duration.ofSeconds(5));
            assertNotNull(in);
            Future<byte[]> read = workers.submit(() -> {
                var got = new java.io.ByteArrayOutputStream();
                byte[] b = new byte[1 << 16];
                try {
                    for (int n; (n = in.read(b, 0, b.length)) > 0; ) got.write(b, 0, n);
                } catch (IOException ended) { /* The session's end. */ }
                return got.toByteArray();
            });
            // With a linger of an hour, an end within the guard means the peer read the close and ended its side.
            io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ofHours(1));
            SessionError.ApplicationClosed mine;
            try {
                if (code == 0 && reason.isEmpty()) client.close(); else client.close(code, reason);
                mine = assertInstanceOf(SessionError.ApplicationClosed.class, client.closed().get(30, TimeUnit.SECONDS));
                assertEquals(new SessionError.ApplicationClosed(code, reason, true), server.closed().get(30, TimeUnit.SECONDS));
            } finally {
                io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ZERO);
            }
            assertEquals(code, mine.code());
            assertFalse(mine.remote());
            var e = assertThrows(SessionClosedException.class, () -> second.write(new byte[] {4}));
            assertEquals(mine, e.error());
            var why = assertInstanceOf(SessionError.ApplicationClosed.class, server.closed().get(5, TimeUnit.SECONDS));
            assertEquals(new SessionError.ApplicationClosed(code, reason, true), why);
            assertTrue(Arrays.equals(data, read.get(5, TimeUnit.SECONDS)), "the peer read the whole 1 MiB in order");
        }
    }

    @Test void peerCloseSurvivesALocalWriteFailure() throws Exception {
        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
        var armed = new AtomicBoolean();
        var firstFailure = new CompletableFuture<Void>();
        var blockedWrite = new CompletableFuture<PipeWriter.Completion>();
        var inboundHeld = new CompletableFuture<Void>();
        PipeReader reader = new PipeReader() {
            @Override public void start(Sink sink) {
                pair[0].reader().start(new Sink() {
                    private CompletableFuture<Void> reads = firstFailure;

                    private void deliver(Runnable read) {
                        if (armed.get()) {
                            reads = reads.thenRun(read);
                            inboundHeld.complete(null);
                        } else {
                            read.run();
                        }
                    }

                    @Override public void onChunk(Chunk chunk, Runnable consumed) {
                        deliver(() -> sink.onChunk(chunk, consumed));
                    }
                    @Override public void onEnd() { deliver(sink::onEnd); }
                    @Override public void onError(Throwable cause) { deliver(() -> sink.onError(cause)); }
                });
            }
            @Override public void close() { pair[0].reader().close(); }
        };
        PipeWriter writer = new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) {
                if (armed.get()) {
                    blockedWrite.complete(done);
                } else {
                    pair[0].writer().write(batch, done);
                }
            }
            @Override public void close() { pair[0].writer().close(); }
        };

        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var dial = workers.submit(() -> Zmux.open(pair[0].withHalves(reader, writer), Role.CLIENT, cfg));
            try (var peer = Zmux.open(pair[1], Role.SERVER, cfg);
                 var local = dial.get(5, TimeUnit.SECONDS)) {
                var stream = local.openStream();
                armed.set(true);
                stream.write(new byte[] {1});
                var write = blockedWrite.get(20, TimeUnit.SECONDS);
                var closing = workers.submit(() -> peer.close(9, ""));
                try {
                    inboundHeld.get(20, TimeUnit.SECONDS);
                    write.failed(new IOException("armed write failure"));
                    firstFailure.complete(null);
                    assertEquals(new SessionError.ApplicationClosed(9, "", true), local.closed().get(5, TimeUnit.SECONDS));
                    closing.get(5, TimeUnit.SECONDS);
                } finally {
                    firstFailure.complete(null);
                }
            }
        }
    }

    @Test void overTcp() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg))) {
                for (long code : new long[] {0x2a, 0}) {
                    var c = Zmux.connect(l.localAddress(), cfg);
                    closeDelivers(c, l.accept(Duration.ofSeconds(5)), code, code == 0 ? "" : "bye");
                }
            }
        }
    }

    @Test void overTls() throws Exception {
        TestTls.init();
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var lc = ListenerConfig.builder().session(cfg).tls(TlsOptions.server(TestTls.server())).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                var tls = TlsOptions.client(TestTls.client(), "localhost");
                for (long code : new long[] {0x2a, 0}) {
                    var c = Zmux.connectTls(l.localAddress(), tls, cfg);
                    closeDelivers(c, l.accept(Duration.ofSeconds(5)), code, code == 0 ? "" : "bye");
                }
            }
        }
    }
}
