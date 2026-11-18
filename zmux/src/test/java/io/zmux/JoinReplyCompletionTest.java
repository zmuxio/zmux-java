package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A Reply whose write reports after the joiner's Confirm and first records: none of them is lost. */
@Timeout(60)
class JoinReplyCompletionTest {
    private static final long DELAY = 300;

    private static void join(RecordMode mode, Framing framing) throws Exception {
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        try (EventLoops loops = EventLoops.create(1); var readers = Executors.newVirtualThreadPerTaskExecutor()) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops)
                    .maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation()
                    .allowRecordMode((c, x, p) -> true).build();
            try (var l = Zmux.listen(NetworkTest.any(), ListenerConfig.builder().session(cfg).build());
                 var c = Zmux.connect(l.localAddress(), cfg); var s = l.accept(Duration.ofSeconds(5))) {
                PipeIo[] p = MemoryPipe.pair(framing);
                PipeWriter w = p[1].writer();
                AtomicBoolean first = new AtomicBoolean(true);
                PipeWriter late = new PipeWriter() {
                    @Override public void write(OutboundBatch b, Completion d) {
                        if (!first.getAndSet(false)) { w.write(b, d); return; }
                        w.write(b, new Completion() {
                            @Override public void completed() { timer.schedule(d::completed, DELAY, TimeUnit.MILLISECONDS); }
                            @Override public void failed(Throwable t) { d.failed(t); }
                        });
                    }
                    @Override public void close() { w.close(); }
                    @Override public void abort(Throwable t) { w.abort(t); }
                };
                PipeIo server = new PipeIo(p[1].reader(), late, framing, "memory", null);
                boolean bound = mode == RecordMode.BOUND;
                l.adapter().handle(bound ? server.withExporter((x, y, n) -> new byte[n]).withCarrierSecure() : server, JoinHint.inBand(new InetSocketAddress("127.0.0.1", 1)));
                ZmuxPipe pipe = c.addPipe(bound ? p[0].withExporter((x, y, n) -> new byte[n]).withCarrierSecure() : p[0], PipeOptions.builder().recordMode(mode).build());
                int streams = 30, size = 8000;
                var received = readers.submit(() -> {
                    long total = 0;
                    var done = new java.util.ArrayList<java.util.concurrent.Future<Integer>>();
                    for (int i = 0; i < streams; i++) {
                        var st = s.acceptStream(Duration.ofSeconds(10));
                        done.add(readers.submit(() -> st.readAll(1 << 20).length));
                    }
                    for (var f : done) total += f.get(20, TimeUnit.SECONDS);
                    return total;
                });
                // Most of it while the Reply's report is outstanding.
                for (int i = 0; i < streams; i++) {
                    var st = c.openStream();
                    st.write(new byte[size]);
                    st.finish();
                    Thread.sleep(DELAY * 2 / streams);
                }
                assertEquals((long) streams * size, received.get(30, TimeUnit.SECONDS));
                Thread.sleep(DELAY);
                assertEquals(0, s.stats().pipeFailures(), "acceptor");
                assertEquals(0, c.stats().pipeFailures(), "joiner");
                assertEquals(2, s.pipes().size(), "the join is attached");
                assertTrue(pipe.stats().orElseThrow().bytesSent() > 0, "the pipe carried records");
                // Nothing the joiner wrote on it went missing, what a later retransmission would hide.
                long sent = pipe.stats().orElseThrow().bytesSent();
                for (var ps : s.pipeStats()) if (ps.pipe().equals(pipe.id())) assertEquals(sent, ps.bytesReceived(), "bytes of the pipe");
            }
        } finally {
            timer.shutdownNow();
        }
    }

    @Test void sealedOnMessages() throws Exception { join(RecordMode.SEALED, Framing.MESSAGES); }
    @Test void boundOnMessages() throws Exception { join(RecordMode.BOUND, Framing.MESSAGES); }
    @Test void sealedOnAByteStream() throws Exception { join(RecordMode.SEALED, Framing.BYTE_STREAM); }
}
