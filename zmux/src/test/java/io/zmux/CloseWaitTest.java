package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.internal.core.SessionState;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The close wait of a multipath session (SPEC.md 6.5 step 1, MULTIPATH.md 13) over in-memory pipes with a fixed
 * one-way delay: while CONNECTION_CLOSE waits for PIPE_ACK coverage the session goes on reinjecting what a failed pipe
 * carried and acknowledging what it receives.
 */
@Timeout(60)
class CloseWaitTest {
    /**
     * The linger timeout and the bound of a public close, and so the bound of the close wait: far above what a
     * covered close takes here.
     */
    private static final Duration LINGER = Duration.ofSeconds(4);

    private static long bound() {
        long was = io.zmux.internal.core.Session.closeLimit;
        io.zmux.internal.diag.Diagnostics.lingerForTests(LINGER);
        io.zmux.internal.core.Session.closeLimit = LINGER.toNanos();
        return was;
    }

    private static void unbound(long was) {
        io.zmux.internal.diag.Diagnostics.lingerForTests(Duration.ZERO);
        io.zmux.internal.core.Session.closeLimit = was;
    }

    /** The two directions of one in-memory pipe, each delivering what was written DELAY later; kill loses both. */
    private static final class Link {
        final ScheduledExecutorService timer;
        final long delayMs;
        final List<Delayed> ends = new ArrayList<>();
        volatile boolean killed;
        /** Bytes toward the client written and not delivered yet. */
        final AtomicInteger towardClient = new AtomicInteger();
        /** Bytes lost in flight. */
        final AtomicInteger dropped = new AtomicInteger();

        Link(ScheduledExecutorService timer, long delayMs) {
            this.timer = timer;
            this.delayMs = delayMs;
        }

        /** The two halves of a new pipe: [0] for the client, [1] for the server. */
        PipeIo[] pipe() {
            PipeIo[] m = MemoryPipe.pair(Framing.BYTE_STREAM);
            return new PipeIo[] {wrap(m[0], true), wrap(m[1], false)};
        }

        private PipeIo wrap(PipeIo io, boolean client) {
            Delayed d = new Delayed(io.reader(), client);
            synchronized (ends) {
                ends.add(d);
            }
            return new PipeIo(d, io.writer(), Framing.BYTE_STREAM, "memory", null);
        }

        /** The pipe is lost: what is in flight never arrives and both ends read an error. */
        void kill() {
            killed = true;
            synchronized (ends) {
                for (Delayed d : ends) timer.execute(() -> d.fail(new IOException("pipe lost")));
            }
        }

        final class Delayed implements PipeReader {
            final PipeReader inner;
            final boolean client;
            final AtomicBoolean ended = new AtomicBoolean();
            volatile Sink sink;

            Delayed(PipeReader inner, boolean client) {
                this.inner = inner;
                this.client = client;
            }

            void fail(Throwable t) {
                if (sink != null && ended.compareAndSet(false, true)) sink.onError(t);
            }

            @Override
            public void start(Sink s) {
                sink = s;
                inner.start(new Sink() {
                    @Override
                    public void onChunk(Chunk c, Runnable consumed) {
                        int n = c.buffer().remaining();
                        if (client) towardClient.addAndGet(n);
                        timer.schedule(() -> {
                            if (client) towardClient.addAndGet(-n);
                            if (killed || ended.get()) {
                                dropped.addAndGet(n);
                                c.close();
                                consumed.run();
                                return;
                            }
                            s.onChunk(c, consumed);
                        }, delayMs, TimeUnit.MILLISECONDS);
                    }

                    @Override
                    public void onEnd() {
                        timer.schedule(() -> {
                            if (!killed && ended.compareAndSet(false, true)) s.onEnd();
                        }, delayMs, TimeUnit.MILLISECONDS);
                    }

                    @Override
                    public void onError(Throwable cause) {
                        timer.schedule(() -> fail(cause), delayMs, TimeUnit.MILLISECONDS);
                    }
                });
            }

            @Override
            public void close() {
                inner.close();
            }
        }
    }

    /** A client and a server joined over n delayed pipes. */
    private record Mesh(ZmuxListener listener, ZmuxSession client, ZmuxSession server, List<Link> links) {
        static Mesh open(EventLoops loops, ScheduledExecutorService timer, long delayMs, int n) throws Exception {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                    .noProbation().scheduler(Scheduler.SPREAD_ETA).build();
            ZmuxListener l = Zmux.listener(ListenerConfig.of(cfg));
            List<Link> links = new ArrayList<>();
            Link boot = new Link(timer, delayMs);
            links.add(boot);
            PipeIo[] b = boot.pipe();
            l.adapter().handle(b[1], JoinHint.inBand(TestAddr.peer("peer")));
            ZmuxSession c = Zmux.open(b[0], Role.CLIENT, cfg);
            ZmuxSession s = l.accept(Duration.ofSeconds(10));
            for (int i = 1; i < n; i++) {
                Link k = new Link(timer, delayMs);
                links.add(k);
                PipeIo[] m = k.pipe();
                l.adapter().handle(m[1], JoinHint.inBand(TestAddr.peer("peer")));
                c.addPipe(m[0], PipeOptions.defaults());
            }
            ListenerEntryTest.await(() -> s.pipes().size() == n && c.pipes().size() == n);
            return new Mesh(l, c, s, links);
        }
    }

    private static SessionState state(ZmuxSession s) {
        try {
            return (SessionState) s.run(op -> s.core.state());
        } catch (IOException e) {
            return SessionState.CLOSED;
        }
    }

    /** Reads st to its end or the session's; the bytes read. */
    private static int drain(ZmuxStream st) {
        byte[] buf = new byte[65536];
        int total = 0;
        try {
            for (int n; (n = st.read(buf, 0, buf.length)) >= 0; ) total += n;
        } catch (IOException e) {
            // the session's close ends the stream
        }
        return total;
    }

    /**
     * The server writes and closes; while its CONNECTION_CLOSE waits for coverage, the pipe carrying unacknowledged
     * data fails. What it carried is reinjected on the surviving pipes: the client reads all of it, and the close
     * completes long before its bound.
     */
    @Test void aPipeFailingDuringTheCloseWaitIsReinjected() throws Exception {
        int size = 96 << 10;
        long was = bound();
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        try (EventLoops loops = EventLoops.create(2); ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor()) {
            Mesh m = Mesh.open(loops, timer, 50, 3);
            try {
                ZmuxStream out = m.server().openStream();
                Future<Integer> got = ex.submit(() -> drain(m.client().acceptStream()));
                out.write(new byte[size]);
                long start = System.nanoTime();
                Future<?> closing = ex.submit(() -> {
                    m.server().close(0x10, "");
                    return null;
                });
                ListenerEntryTest.await(() -> state(m.server()) == SessionState.CLOSING);
                Link x = m.links().getFirst();
                for (Link k : m.links()) if (k.towardClient.get() > x.towardClient.get()) x = k;
                assertTrue(x.towardClient.get() > 8192, "no stream data in flight toward the client");
                x.kill();
                closing.get(10, TimeUnit.SECONDS);
                long took = System.nanoTime() - start;
                assertTrue(x.dropped.get() > 8192, "the failed pipe lost no stream data in flight");
                assertEquals(size, got.get(10, TimeUnit.SECONDS), "the client misses what the failed pipe carried");
                assertTrue(took < LINGER.toNanos() / 4, "the close took " + took / 1_000_000 + " ms (RTT 100 ms)");
                m.client().awaitClosed(Duration.ofSeconds(10));
                m.server().awaitClosed(Duration.ofSeconds(10));
                out.close();
            } finally {
                m.listener().close();
            }
            Leaks.none(loops, "read buffers leaked");
        } finally {
            unbound(was);
            timer.shutdownNow();
        }
    }

    /**
     * Both ends write and close at once: each one's CONNECTION_CLOSE waits for the acknowledgement of its data, which
     * the other end, waiting too, keeps sending. Both closes complete within a few RTTs, not at the bound.
     */
    @Test void aSimultaneousCloseCompletesWithinRoundTrips() throws Exception {
        int size = 64 << 10;
        long was = bound();
        ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
        try (EventLoops loops = EventLoops.create(2); ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor()) {
            Mesh m = Mesh.open(loops, timer, 50, 2);
            try {
                ZmuxStream a = m.client().openStream(), b = m.server().openStream();
                a.write(new byte[size]);
                b.write(new byte[size]);
                CompletableFuture<Void> go = new CompletableFuture<>();
                List<Future<Long>> took = new ArrayList<>();
                for (ZmuxSession s : List.of(m.client(), m.server())) {
                    took.add(ex.submit(() -> {
                        go.join();
                        long t = System.nanoTime();
                        s.close(0x10, "");
                        return System.nanoTime() - t;
                    }));
                }
                go.complete(null);
                for (Future<Long> t : took) {
                    long ns = t.get(10, TimeUnit.SECONDS);
                    assertTrue(ns < LINGER.toNanos() / 4, "a close took " + ns / 1_000_000 + " ms (RTT 100 ms)");
                }
                m.client().awaitClosed(Duration.ofSeconds(10));
                m.server().awaitClosed(Duration.ofSeconds(10));
                a.close();
                b.close();
            } finally {
                m.listener().close();
            }
            Leaks.none(loops, "read buffers leaked");
        } finally {
            unbound(was);
            timer.shutdownNow();
        }
    }
}
