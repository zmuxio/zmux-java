package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A slow bootstrap path does not hold a multipath session back once a fast pipe joined. */
@Timeout(60)
class SlowPathTest {
    /**
     * A sending half that delivers each write after a one-way delay, at a rate, with up to cap
     * bytes buffered before a write waits: one direction of a link.
     */
    static final class Link implements PipeWriter {
        private record Item(ByteBuffer data, long at) {}

        private final PipeWriter out;
        private final long cap;
        /** The one-way delay; a test may change it. */
        volatile long delayNanos;
        /** Nanoseconds per byte; a test may slow the link down. */
        volatile double nanosPerByte;
        private final LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>();
        private final Thread pump;
        private long last, held;
        private Completion waiting;
        private volatile boolean closed;

        void rate(double bytesPerSecond) { nanosPerByte = 1e9 / bytesPerSecond; }

        void delay(Duration d) { delayNanos = d.toNanos(); }

        Link(PipeWriter out, Duration delay, double bytesPerSecond, long cap) {
            this.out = out; delayNanos = delay.toNanos(); nanosPerByte = 1e9 / bytesPerSecond; this.cap = cap;
            pump = Thread.ofPlatform().daemon(true).name("link").start(this::run);
        }

        @Override public void write(OutboundBatch batch, Completion done) {
            ByteBuffer copy = ByteBuffer.allocate(batch.bytes());
            for (int i = 0; i < batch.segmentCount(); i++) copy.put(batch.segment(i).duplicate());
            copy.flip();
            boolean now;
            synchronized (this) {
                long t = Math.max(System.nanoTime(), last);
                last = t + (long) (copy.remaining() * nanosPerByte);
                held += copy.remaining();
                queue.add(new Item(copy, last + delayNanos));
                now = held < cap;
                if (!now) waiting = done;
            }
            if (now) done.completed();
        }

        private void run() {
            try {
                while (!closed) {
                    Item it = queue.take();
                    long wait = it.at - System.nanoTime();
                    if (wait > 0) Thread.sleep(Duration.ofNanos(wait));
                    var sent = new CompletableFuture<Void>();
                    int n = it.data.remaining();
                    out.write(OutboundBatch.of(it.data), new Completion() {
                        @Override public void completed() { sent.complete(null); }
                        @Override public void failed(Throwable cause) { sent.completeExceptionally(cause); }
                    });
                    sent.get();
                    Completion c;
                    synchronized (this) {
                        held -= n;
                        c = held < cap ? waiting : null;
                        if (c != null) waiting = null;
                    }
                    if (c != null) c.completed();
                }
            } catch (InterruptedException | ExecutionException e) {
                // Closed.
            }
        }

        @Override public void close() { closed = true; pump.interrupt(); out.close(); }
        @Override public void abort(Throwable cause) { closed = true; pump.interrupt(); out.abort(cause); }
    }

    /** Both ends of a pipe pair, each direction through its own Link. */
    static PipeIo[] linked(Duration delay, double rate, long cap) {
        PipeIo[] io = MemoryPipe.pair(Framing.BYTE_STREAM);
        return new PipeIo[] {
            new PipeIo(io[0].reader(), new Link(io[0].writer(), delay, rate, cap), Framing.BYTE_STREAM, null, null),
            new PipeIo(io[1].reader(), new Link(io[1].writer(), delay, rate, cap), Framing.BYTE_STREAM, null, null),
        };
    }

    /**
     * The slow bootstrap pipe (300 ms one way, 30 kB/s, deep buffer) takes the first window while
     * the joined fast pipe (20 ms, 5 MB/s) is in probation, and the stream's binding then waits
     * for that data. The stream is blocked by the peer's stream credit all along, so
     * opportunistic reinjection re-sends the blocking data on the fast pipe: the echo runs at
     * the offered ~3 MB/s instead of the slow path's rate.
     */
    @Test void aSlowBootstrapPathDoesNotHoldTheSession() throws Exception {
        long echoed = echoed(0).echoed();
        assertTrue(echoed >= 8 << 20, "echoed " + echoed + " bytes in 6 s next to a slow path, want at least 8 MiB of the ~19 MB offered");
    }

    /**
     * With windows wide enough that the peer's credit never blocks the stream, its binding still
     * waits for its data on the slow pipe once it moves: that wait triggers opportunistic
     * reinjection, the penalty moves the binding, and the direction's data left on the slow pipe
     * is re-sent once on the fast one. The copies stay within what the slow pipe had in flight.
     */
    @Test void aPenaltyReinjectsTheDataItsBindingLeftOnTheSlowPipe() throws Exception {
        Run r = echoed(16 << 20);
        assertTrue(r.stats().penalties() > 0 && r.stats().bytesOpportunistic() > 0, "no penalty: " + r.stats());
        assertTrue(r.echoed() >= 8 << 20, "echoed " + r.echoed() + " bytes in 6 s, want at least 8 MiB");
        // Each byte on the slow pipe is re-sent at most once for the penalty and once opportunistically.
        assertTrue(r.stats().bytesReinjected() <= r.slowSent() + r.stats().bytesOpportunistic(),
                r.stats().bytesReinjected() + " bytes re-sent, more than the " + r.slowSent() + " sent on the slow pipe plus "
                        + r.stats().bytesOpportunistic() + " opportunistic");
    }

    record Run(long echoed, SessionStats stats, long slowSent) {}

    /** Echoes at ~3 MB/s for 6 s over the slow bootstrap pipe and a fast joined one; window 0: the default. */
    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    private static Run echoed(long window) throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cb = ZmuxConfig.builder().eventLoops(loops).maxPipes(2).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd());
            if (window > 0) cb.initialStreamReceiveWindow(window).maxStreamReceiveWindow(window)
                    .initialConnectionReceiveWindow(window).maxConnectionReceiveWindow(window);
            ZmuxConfig cfg = cb.build();
            try (var listener = ZmuxListener.of(ListenerConfig.of(cfg))) {
                PipeIo[] slow = linked(Duration.ofMillis(300), 3e4, 16 << 20);
                listener.adapter().handle(slow[1], JoinHint.inBand(TestAddr.peer("slow")));
                try (var client = Zmux.open(slow[0], Role.CLIENT, cfg); var server = listener.accept(Duration.ofSeconds(10))) {
                    PipeIo[] fast = linked(Duration.ofMillis(20), 5e6, 256 << 10);
                    listener.adapter().handle(fast[1], JoinHint.inBand(TestAddr.peer("fast")));
                    client.addPipe(fast[0], PipeOptions.defaults());
                    var echo = Thread.ofVirtual().start(() -> {
                        try (var st = server.acceptStream(Duration.ofSeconds(10))) {
                            st.inputStream().transferTo(st.outputStream());
                        } catch (IOException ended) { /* The client closed. */ }
                    });
                    var st = client.openStream();
                    var got = new AtomicLong();
                    var writer = Thread.ofVirtual().start(() -> {
                        byte[] b = new byte[32 << 10];
                        try {
                            while (true) { st.write(b); Thread.sleep(10); }
                        } catch (IOException | InterruptedException ended) { /* The test ended. */ }
                    });
                    var reader = Thread.ofVirtual().start(() -> {
                        byte[] b = new byte[64 << 10];
                        try {
                            for (int n; (n = st.inputStream().read(b)) >= 0; ) got.addAndGet(n);
                        } catch (IOException ended) { /* The test ended. */ }
                    });
                    Thread.sleep(6_000);
                    long echoed = got.get();
                    SessionStats stats = client.stats();
                    long slowSent = client.pipeStats().stream().filter(p -> p.pipeId().value() == 0).findFirst().orElseThrow().bytesSent();
                    writer.interrupt();
                    st.reset(0);
                    client.close();
                    server.close();
                    writer.join(5_000); reader.join(5_000); echo.join(5_000);
                    return new Run(echoed, stats, slowSent);
                }
            }
        }
    }
}
