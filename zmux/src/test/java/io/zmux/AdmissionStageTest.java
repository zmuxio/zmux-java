package io.zmux;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import io.zmux.adapter.Handled;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The stages of an admission: what a decision is taken on (a carrier's start, a request head, an
 * upgrade's head and the first message a deferral waits for), the verdicts that apply to each,
 * the bound of a first message, and the one takeover that every carrier given back reaches.
 * Each outcome counts once, and the carrier's slots go back.
 */
@Timeout(60)
class AdmissionStageTest {
    private static final Duration LONG = Duration.ofSeconds(10);

    /** A listener whose decider is f, counted in calls; fallback may be null. */
    private static ZmuxListener listener(EventLoops loops, Admission.Builder adm, Admission.Fallback fallback) {
        return listener(loops, adm, fallback, false);
    }

    private static ZmuxListener listener(EventLoops loops, Admission.Builder adm, Admission.Fallback fallback,
                                         boolean refuseStalls) {
        var lc = ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build()).maxPendingPerSource(2)
                .admission(adm.build());
        if (refuseStalls) lc.maxPending(1);
        if (fallback != null) lc.fallback(fallback);
        return Zmux.listener(lc.build());
    }

    private static Admission.Builder deciding(int peek, AtomicInteger calls, BiFunction<Admission.CarrierInfo, ByteBuffer, Admission.Decision> f) {
        return Admission.builder(peek, Admission.Decider.immediate((info, prefix) -> {
            calls.incrementAndGet();
            return f.apply(info, prefix);
        })).peekTimeout(LONG);
    }

    private static JoinHint hint() { return JoinHint.inBand(TestAddr.peer("client")); }

    private static JoinHint request() { return JoinHint.of(RequestHead.of(java.util.Map.of()), false, TestAddr.peer("client")); }

    private static AdmissionVerdict verdict(Handled h) throws Exception {
        return h.verdict().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    /** Sends each message (or byte-stream write) on the peer end. */
    private static void send(PipeIo end, byte[] first, byte[]... more) {
        var buffers = new ByteBuffer[more.length + 1];
        buffers[0] = ByteBuffer.wrap(first);
        for (int i = 0; i < more.length; i++) buffers[i + 1] = ByteBuffer.wrap(more[i]);
        end.writer().write(OutboundBatch.of(buffers), new PipeWriter.Completion() {
            @Override public void completed() { }
            @Override public void failed(Throwable cause) { }
        });
    }

    /** Collects what a reader delivers, one entry per chunk (a message), and its end. */
    private record Reading(BlockingQueue<byte[]> chunks, CompletableFuture<Throwable> end) {
        static Reading of(PipeIo io) {
            var r = new Reading(new LinkedBlockingQueue<>(), new CompletableFuture<>());
            io.reader().start(new PipeReader.Sink() {
                @Override public void onChunk(Chunk chunk, Runnable consumed) {
                    byte[] copy = chunk.copy();
                    chunk.close();
                    consumed.run();
                    r.chunks.add(copy);
                }
                @Override public void onEnd() { r.end.complete(null); }
                @Override public void onError(Throwable cause) { r.end.complete(cause); }
            });
            return r;
        }

        byte[] next() throws InterruptedException {
            byte[] b = chunks.poll(10, TimeUnit.SECONDS);
            assertNotNull(b, "nothing delivered");
            return b;
        }
    }

    /** The fields of s add up to n carriers, each counted once. */
    private static void addsUp(AdmissionStats s, long n) {
        assertEquals(n, s.accepted() + s.fallback() + s.takeover() + s.stalled() + s.closed() + s.stallOverflow() + s.tooLarge()
                + s.peekTimedOut() + s.handshakeFailed(), s.toString());
    }

    /** The source's one slot went back. */
    private static void slotBack(ZmuxListener listener, Handled handled) throws Exception {
        handled.closed().get(30, TimeUnit.SECONDS);
        var slot = listener.adapter().reserve(TestAddr.peer("client"), null);
        assertNotNull(slot, "the completed admission returned its source slot");
        slot.release();
    }

    /** An upgrade under the test's control. */
    private static final class Up implements io.zmux.adapter.Upgrade {
        final CompletableFuture<Void> stage = new CompletableFuture<>();
        final AtomicInteger opened = new AtomicInteger();
        final CompletableFuture<Void> entered = new CompletableFuture<>();
        @Override public CompletableFuture<Void> open() {
            opened.incrementAndGet();
            entered.complete(null);
            return stage;
        }
    }

    @Test void maxMessageDefaultsToTheLargestRecordAndIsBounded() {
        var d = Admission.Decider.immediate((i, p) -> Admission.Decision.CLOSE);
        assertEquals(Admission.MAX_MESSAGE, Admission.builder(0, d).build().maxMessage());
        assertEquals(65535, Admission.MAX_MESSAGE);
        assertEquals(Admission.MAX_MESSAGE, Admission.builder(0, d).maxMessage(0).build().maxMessage());
        assertEquals(100, Admission.builder(0, d).maxMessage(100).build().maxMessage());
        assertThrows(IllegalArgumentException.class, () -> Admission.builder(0, d).maxMessage(Admission.MAX_MESSAGE + 1));
        assertThrows(IllegalArgumentException.class, () -> Admission.builder(0, d).maxMessage(-1));
        assertEquals(0, Admission.builder(4, d).maxMessage(10).build().maxPeek(), "maxMessage does not touch maxPeek");
    }

    /** DEFER decides again only on an upgrade's head: on a carrier's start and on a plain request head it closes. */
    @Test void deferAppliesOnlyToAnUpgradesHead() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            var stages = new LinkedBlockingQueue<String>();
            try (var l = listener(loops, deciding(4, calls, (info, prefix) -> {
                stages.add(info.stage() + " " + info.framing() + " " + info.upgrade() + " " + prefix.remaining());
                return new Admission.Decision(AdmissionVerdict.DEFER, 0, null, 0, "v");
            }), null)) {
                var bytes = MemoryPipe.pair(Framing.BYTE_STREAM);
                var h = l.adapter().handle(bytes[1], hint());
                send(bytes[0], new byte[] {1, 2, 3, 4});
                assertEquals(AdmissionVerdict.CLOSE, verdict(h));
                assertEquals("CARRIER BYTE_STREAM false 4", stages.poll(5, TimeUnit.SECONDS));
                slotBack(l, h);

                var head = MemoryPipe.pair(Framing.BYTE_STREAM);
                var headHandled = l.adapter().handleRequest(head[1], request(), true, null, () -> { throw new AssertionError("unexpected acceptance head"); });
                assertEquals(AdmissionVerdict.CLOSE, verdict(headHandled));
                assertEquals("HEAD BYTE_STREAM false 0", stages.poll(5, TimeUnit.SECONDS));
                slotBack(l, headHandled);

                // On an upgrade's head it defers: the upgrade runs, and the first message is decided on;
                // a second DEFER there closes.
                var up = new Up();
                var ws = MemoryPipe.pair(Framing.MESSAGES);
                var u = l.adapter().admitFirst(ws[1], request(), up, null);
                assertEquals("HEAD MESSAGES true 0", stages.poll(5, TimeUnit.SECONDS));
                up.entered.get(30, TimeUnit.SECONDS);
                assertEquals(1, up.opened.get());
                assertFalse(u.verdict().toCompletableFuture().isDone(), "a deferral is no verdict");
                up.stage.complete(null);
                send(ws[0], new byte[] {9, 9, 9});
                assertEquals(AdmissionVerdict.CLOSE, verdict(u));
                assertEquals("CARRIER MESSAGES true 3", stages.poll(5, TimeUnit.SECONDS));
                slotBack(l, u);
                assertEquals(4, calls.get());
                var st = l.stats().admission();
                assertEquals(3, st.closed());
                addsUp(st, 3);
                assertEquals(3, l.variantStats().getFirst().closed(), "each closing DEFER counts under its variant");
            }
        }
    }

    /** MORE asks for more of a byte stream only: on a head, an upgrade's head and a message carrier it closes. */
    @Test void moreWhereItDoesNotApplyCloses() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            try (var l = listener(loops, deciding(1, calls, (info, prefix) -> Admission.Decision.more(8)).maxPeek(16), null)) {
                var head = MemoryPipe.pair(Framing.BYTE_STREAM);
                var headHandled = l.adapter().handleRequest(head[1], request(), true, null, () -> { throw new AssertionError("unexpected acceptance head"); });
                assertEquals(AdmissionVerdict.CLOSE, verdict(headHandled));
                slotBack(l, headHandled);
                var up = new Up();
                var ws = MemoryPipe.pair(Framing.MESSAGES);
                var upgradeHandled = l.adapter().admitFirst(ws[1], request(), up, null);
                assertEquals(AdmissionVerdict.CLOSE, verdict(upgradeHandled));
                assertEquals(0, up.opened.get(), "nothing upgraded");
                slotBack(l, upgradeHandled);
                var messages = MemoryPipe.pair(Framing.MESSAGES);
                var h = l.adapter().handle(messages[1], hint());
                send(messages[0], new byte[] {1, 2});
                assertEquals(AdmissionVerdict.CLOSE, verdict(h));
                slotBack(l, h);
                assertEquals(3, calls.get(), "each decided once");
                addsUp(l.stats().admission(), 3);
                assertEquals(3, l.stats().admission().closed());
            }
        }
    }

    /** A first message above maxMessage closes the carrier without a decision, counted as too large. */
    @Test void aFirstMessageAboveTheBoundIsTooLarge() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            try (var l = listener(loops, deciding(1, calls, (info, prefix) -> Admission.Decision.accept(0)).maxPeek(16).maxMessage(8), null)) {
                var big = MemoryPipe.pair(Framing.MESSAGES);
                var h = l.adapter().handle(big[1], hint());
                send(big[0], new byte[9]);
                assertEquals(AdmissionVerdict.CLOSE, verdict(h));
                slotBack(l, h);
                // A reader that fails the message at the bound itself counts the same.
                var limited = MemoryPipe.pair(Framing.MESSAGES);
                var limit = new AtomicInteger(-1);
                PipeReader r = new PipeReader() {
                    @Override public void limitFirstMessage(int n) { limit.set(n); }
                    @Override public void start(Sink s) { s.onError(new io.zmux.carrier.MessageTooLargeException(limit.get())); }
                    @Override public void close() { }
                };
                var limitedHandled = l.adapter().handle(limited[1].withHalves(r, limited[1].writer()), hint());
                assertEquals(AdmissionVerdict.CLOSE, verdict(limitedHandled));
                assertEquals(8, limit.get(), "the reader is given maxMessage");
                slotBack(l, limitedHandled);
                assertEquals(0, calls.get(), "no decision on an oversized first message");
                var st = l.stats().admission();
                assertEquals(2, st.tooLarge());
                addsUp(st, 2);
                // The reader can also say how much it observed, which here exceeds both limits.
                var overBoth = MemoryPipe.pair(Framing.MESSAGES);
                PipeReader observed = new PipeReader() {
                    @Override public void start(Sink s) { s.onError(new io.zmux.carrier.MessageTooLargeException(8, 17)); }
                    @Override public void close() { }
                };
                var oversizedHandled = l.adapter().handle(overBoth[1].withHalves(observed, overBoth[1].writer()), hint());
                assertEquals(AdmissionVerdict.CLOSE, verdict(oversizedHandled));
                slotBack(l, oversizedHandled);
                assertEquals(new AdmissionStats(0, 0, 0, 0, 1, 0, 2, 0, 0), l.stats().admission());
                // A message of exactly the bound is decided.
                var fits = MemoryPipe.pair(Framing.MESSAGES);
                var f = l.adapter().handle(fits[1], hint());
                send(fits[0], new byte[8]);
                assertEquals(AdmissionVerdict.ACCEPT, verdict(f));
                assertEquals(1, calls.get());
                fits[0].writer().abort(new IOException("done"));
            }
        }
    }

    @Test void theSameProtocolLimitClosesOrdinaryAdmissionAndCountsTooLargeAfterDefer() throws Exception {
        try (var loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            var admission = deciding(1, calls, (info, prefix) ->
                    new Admission.Decision(AdmissionVerdict.DEFER, 0).withVariant("head")).maxPeek(8);
            try (var l = listener(loops, admission, null)) {
                var ordinary = MemoryPipe.pair(Framing.MESSAGES);
                var h = l.adapter().handle(ordinary[1], hint());
                send(ordinary[0], new byte[9]);
                assertEquals(AdmissionVerdict.CLOSE, verdict(h));
                slotBack(l, h);

                var deferred = MemoryPipe.pair(Framing.MESSAGES);
                var up = new Up();
                var d = l.adapter().admitFirst(deferred[1], request(), up, null);
                up.stage.complete(null);
                send(deferred[0], new byte[9]);
                assertEquals(AdmissionVerdict.CLOSE, verdict(d));
                slotBack(l, d);

                assertEquals(1, calls.get(), "only the upgrade head was decided");
                assertEquals(new AdmissionStats(0, 0, 0, 0, 1, 0, 1, 0, 0), l.stats().admission());
                assertTrue(l.variantStats().isEmpty());
                assertEquals(0, l.stats().rejected());
                assertEquals(0, l.stats().setupFailures());
            }
        }
    }

    @Test void deferredFirstInputFailureCountsAsHandshakeFailure() throws Exception {
        try (var loops = EventLoops.create(1)) {
            var admission = deciding(1, new AtomicInteger(), (info, prefix) ->
                    new Admission.Decision(AdmissionVerdict.DEFER, 0).withVariant("head"));
            try (var l = listener(loops, admission, null)) {
                var ws = MemoryPipe.pair(Framing.MESSAGES);
                var up = new Up();
                var h = l.adapter().admitFirst(ws[1], request(), up, null);
                up.stage.complete(null);
                ws[0].writer().abort(new IOException("first input failed"));
                assertEquals(AdmissionVerdict.CLOSE, verdict(h));
                slotBack(l, h);
                assertEquals(new AdmissionStats(0, 0, 0, 0, 0, 0, 0, 0, 1), l.stats().admission());
                assertEquals(0, l.stats().setupFailures());
                assertTrue(l.variantStats().isEmpty());
            }
        }
    }

    /** A message carrier that ends, fails or sends no binary message first is closed without a decision. */
    @Test void aFailedFirstMessageClosesWithoutADecision() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            try (var l = listener(loops, deciding(1, calls, (info, prefix) -> Admission.Decision.FALLBACK), null)) {
                var ended = MemoryPipe.pair(Framing.MESSAGES);
                var h = l.adapter().handle(ended[1], hint());
                ended[0].writer().close();
                assertEquals(AdmissionVerdict.CLOSE, verdict(h));
                slotBack(l, h);
                var failed = MemoryPipe.pair(Framing.MESSAGES);
                var g = l.adapter().handle(failed[1], hint());
                // What a WebSocket reader reports for a text message.
                failed[0].writer().abort(new java.net.ProtocolException("a text message on a zmux carrier"));
                assertEquals(AdmissionVerdict.CLOSE, verdict(g));
                slotBack(l, g);
                assertEquals(0, calls.get());
                var st = l.stats().admission();
                assertEquals(2, st.closed());
                addsUp(st, 2);
            }
        }
    }

    /** A deferral whose upgrade or first message does not come by the head's deadline times out; the upgrade is cancelled. */
    @Test void aDeferralThatDoesNotCompleteInTimeTimesOut() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            var adm = deciding(1, calls, (info, prefix) -> info.stage() == Admission.Stage.HEAD
                    ? new Admission.Decision(AdmissionVerdict.DEFER, 0) : Admission.Decision.accept(0)).peekTimeout(Duration.ofMillis(300));
            try (var l = listener(loops, adm, null)) {
                var up = new Up();
                var ws = MemoryPipe.pair(Framing.MESSAGES);
                var upgradeHandled = l.adapter().admitFirst(ws[1], request(), up, null);
                assertEquals(AdmissionVerdict.CLOSE, verdict(upgradeHandled));
                assertThrows(java.util.concurrent.CancellationException.class, () -> up.stage.get(3, TimeUnit.SECONDS));
                slotBack(l, upgradeHandled);
                // The upgrade completes, the first message never comes.
                var up2 = new Up();
                up2.stage.complete(null);
                var silent = MemoryPipe.pair(Framing.MESSAGES);
                var silentHandled = l.adapter().admitFirst(silent[1], request(), up2, null);
                assertEquals(AdmissionVerdict.CLOSE, verdict(silentHandled));
                slotBack(l, silentHandled);
                assertEquals(2, calls.get(), "only the heads were decided");
                var st = l.stats().admission();
                assertEquals(2, st.peekTimedOut());
                addsUp(st, 2);
            }
        }
    }

    /** The listener's close ends a deferral at once, counted closed; a failed upgrade counts as a failed handshake. */
    @Test void closeDuringTheWaitAndAFailedUpgrade() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            var adm = deciding(1, calls, (info, prefix) -> new Admission.Decision(AdmissionVerdict.DEFER, 0));
            var l = listener(loops, adm, null);
            try {
                var failing = new Up();
                var f = l.adapter().admitFirst(MemoryPipe.pair(Framing.MESSAGES)[1], request(), failing, null);
                failing.stage.completeExceptionally(new IOException("101 not written"));
                assertEquals(AdmissionVerdict.CLOSE, verdict(f));
                assertEquals(1, l.stats().admission().handshakeFailed());
                slotBack(l, f);

                var up = new Up();
                var ws = MemoryPipe.pair(Framing.MESSAGES);
                var h = l.adapter().admitFirst(ws[1], request(), up, null);
                up.stage.complete(null);
                up.entered.get(30, TimeUnit.SECONDS);
                assertEquals(2, calls.get());
                l.close();
                assertEquals(AdmissionVerdict.CLOSE, verdict(h));
                var st = l.stats().admission();
                assertEquals(1, st.closed());
                addsUp(st, 2);
            } finally { l.close(); }
        }
    }

    @SuppressWarnings("try") // The stop is the event under test.
    @Test void takeoverWaitsForHeadCommitAndLosesToFailureWithdrawalOrDeadline() throws Exception {
        // HS-16, HS-19.
        for (String ending : java.util.List.of("write", "withdraw", "deadline", "stop", "commit")) {
            var opened = new CompletableFuture<Void>();
            var flush = new CompletableFuture<Void>();
            var served = new AtomicInteger();
            var admission = deciding(0, new AtomicInteger(), (i,p) -> Admission.Decision.TAKEOVER)
                    .peekTimeout(ending.equals("deadline") ? Duration.ofMillis(400) : LONG);
            try (var loops = EventLoops.create(1); var l = listener(loops, admission, takeover -> {
                served.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            })) {
                var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                var h = l.adapter().handleRequest(pair[1], request(), true, null, () -> {
                    opened.complete(null);
                    return flush;
                });
                opened.get(3, TimeUnit.SECONDS);
                assertEquals(0, served.get());
                addsUp(l.stats().admission(), 0);
                switch (ending) {
                    case "write" -> {
                        var failed = new IOException("response head not flushed");
                        flush.completeExceptionally(failed);
                        assertSame(failed, assertThrows(IOException.class, h::awaitSetup));
                    }
                    case "withdraw" -> {
                        io.zmux.internal.spi.AdapterPorts.setup(h).interrupt();
                        assertThrows(java.io.InterruptedIOException.class, h::awaitSetup);
                    }
                    case "deadline" -> assertThrows(java.net.SocketTimeoutException.class, h::awaitSetup);
                    case "stop" -> { l.close(); assertThrows(ListenerClosedException.class, h::awaitSetup); }
                    default -> { flush.complete(null); h.awaitSetup(); }
                }
                h.closed().get(5, TimeUnit.SECONDS);
                if (ending.equals("deadline") || ending.equals("withdraw") || ending.equals("stop")) assertTrue(flush.isCancelled());
                assertEquals(ending.equals("commit") ? 1 : 0, served.get());
                if (!ending.equals("stop")) slotBack(l, h);
                var counts = l.stats().admission();
                assertEquals(ending.equals("write") ? 1 : 0, counts.handshakeFailed());
                assertEquals(ending.equals("deadline") ? 1 : 0, counts.peekTimedOut());
                assertEquals(ending.equals("commit") ? 1 : 0, counts.takeover());
                assertEquals(ending.equals("withdraw") || ending.equals("stop") ? 1 : 0, counts.closed());
                addsUp(counts, 1);
                pair[0].reader().close();
                pair[0].writer().close();
            }
        }
    }

    @Test void blockedHeadCancellationRetainsTheSourceUntilItExits() throws Exception {
        // HS-16.
        var opened = new CompletableFuture<Void>();
        var cancelling = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        var flush = new CompletableFuture<Void>() {
            @Override public boolean cancel(boolean interrupt) {
                cancelling.complete(null);
                release.join();
                return super.cancel(interrupt);
            }
        };
        try (var loops = EventLoops.create(1);
             var l = listener(loops, deciding(0, new AtomicInteger(), (i,p) -> Admission.Decision.TAKEOVER),
                     _ -> { throw new AssertionError("unflushed carrier escaped"); })) {
            var h = l.adapter().handleRequest(MemoryPipe.pair(Framing.BYTE_STREAM)[1], request(), true, null, () -> {
                opened.complete(null); return flush;
            });
            try {
                opened.get(3, TimeUnit.SECONDS);
                io.zmux.internal.spi.AdapterPorts.setup(h).interrupt();
                cancelling.get(3, TimeUnit.SECONDS);
                var turn = new CompletableFuture<Void>();
                loops.group().next().execute(() -> turn.complete(null));
                turn.get(3, TimeUnit.SECONDS);
                var held = l.adapter().reserve(request().source(), null);
                assertNotNull(held);
                try { assertNull(l.adapter().reserve(request().source(), null)); }
                finally { held.release(); }
                assertFalse(h.closed().isDone());
            } finally { release.complete(null); }
            assertThrows(java.io.InterruptedIOException.class, h::awaitSetup);
            h.closed().get(3, TimeUnit.SECONDS);
            assertTrue(flush.isCancelled());
            var returned = l.adapter().reserve(request().source(), null);
            assertNotNull(returned);
            returned.release();
            addsUp(l.stats().admission(), 1);
        } finally { release.complete(null); }
    }

    @Test void unacknowledgedHeadCannotHoldTheSlotPastLinger() throws Exception {
        // HS-16a. A legacy host never confirms release and even its cancel call remains blocked.
        var entered = new CompletableFuture<Void>();
        var cancelling = new CompletableFuture<Void>();
        var unblock = new CompletableFuture<Void>();
        var exited = new CompletableFuture<Void>();
        var stage = new CompletableFuture<Void>() {
            @Override public boolean cancel(boolean interrupt) {
                cancelling.complete(null);
                unblock.join();
                exited.complete(null);
                return false;
            }
        };
        try (var loops = EventLoops.create(1);
             var l = listener(loops, deciding(0, new AtomicInteger(), (i,p) -> Admission.Decision.TAKEOVER),
                     _ -> { throw new AssertionError("unflushed carrier escaped"); })) {
            var h = l.adapter().handleRequest(MemoryPipe.pair(Framing.BYTE_STREAM)[1], request(), true, null,
                    () -> { entered.complete(null); return stage; });
            entered.get(3, TimeUnit.SECONDS);
            io.zmux.internal.spi.AdapterPorts.setup(h).interrupt();
            cancelling.get(3, TimeUnit.SECONDS);
            assertThrows(java.io.InterruptedIOException.class, h::awaitSetup);
            io.zmux.internal.loop.CarrierActionsTest.advanceOpeningLinger(loops.group().next());
            h.closed().get(3, TimeUnit.SECONDS);
            assertFalse(exited.isDone());
            var again = l.adapter().reserve(request().source(), null);
            assertNotNull(again);
            again.release();
        } finally { unblock.complete(null); }
        exited.get(3, TimeUnit.SECONDS);
    }

    @Test void cancelledTakeoverRetainsTheSourceUntilTheHeadWriteReleases() throws Exception {
        // HS-16a. Cancel returns immediately. Only the adapter can release the actual write.
        var entered = new CompletableFuture<Void>();
        var cancelled = new CompletableFuture<Boolean>();
        var physical = new CompletableFuture<Void>();
        var waiting = new CompletableFuture<Void>() {
            @Override public boolean cancel(boolean interrupt) {
                boolean won = super.cancel(interrupt);
                cancelled.complete(io.zmux.internal.loop.SessionHost.onHostThread());
                return won;
            }
        };
        var head = new io.zmux.adapter.Upgrade() {
            @Override public CompletableFuture<Void> open() { entered.complete(null); return waiting; }
            @Override public CompletableFuture<Void> released() { return physical; }
        };
        try (var loops = EventLoops.create(1);
             var l = listener(loops, deciding(0, new AtomicInteger(), (i,p) -> Admission.Decision.TAKEOVER),
                     _ -> { throw new AssertionError("host received cancelled head"); })) {
            var h = l.adapter().handleRequest(MemoryPipe.pair(Framing.BYTE_STREAM)[1], request(), true, null, head);
            try {
                entered.get(3, TimeUnit.SECONDS);
                assertTrue(physical.getNumberOfDependents() > 0, "setup subscribed to actual write release");
                io.zmux.internal.spi.AdapterPorts.setup(h).interrupt();
                assertFalse(cancelled.get(3, TimeUnit.SECONDS), "owner called host cancel");
                assertThrows(java.io.InterruptedIOException.class, h::awaitSetup);
                var turn = new CompletableFuture<Void>();
                loops.group().next().execute(() -> turn.complete(null));
                turn.get(3, TimeUnit.SECONDS);
                assertFalse(h.closed().isDone());
                var held = l.adapter().reserve(request().source(), null);
                assertNotNull(held);
                try { assertNull(l.adapter().reserve(request().source(), null)); }
                finally { held.release(); }
            } finally { physical.complete(null); }
            h.closed().get(3, TimeUnit.SECONDS);
            var first = l.adapter().reserve(request().source(), null);
            assertNotNull(first);
            first.release();
            assertEquals(1, l.stats().admission().closed());
            addsUp(l.stats().admission(), 1);
        }
    }

    /** An exception of the decider closes the carrier; one of the fallback, or its failed stage, closes only its carrier. */
    @Test void failuresOfTheHostAreContained() throws Exception {
        // HS-07, HS-11.
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            var mode = new AtomicInteger();
            Admission.Fallback fallback = t -> {
                if (mode.get() == 0) throw new IllegalStateException("fallback failure");
                return CompletableFuture.failedFuture(new IOException("fallback stage failure"));
            };
            try (var l = listener(loops, deciding(1, calls, (info, prefix) -> {
                if (prefix.get(0) == 0) throw new IllegalStateException("decider failure");
                return Admission.Decision.FALLBACK;
            }), fallback)) {
                for (int m = 0; m < 3; m++) {
                    mode.set(m == 2 ? 1 : 0);
                    var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var peer = Reading.of(p[0]);
                    var h = l.adapter().handle(p[1], hint());
                    send(p[0], new byte[] {(byte) m});
                    assertEquals(m == 0 ? AdmissionVerdict.CLOSE : AdmissionVerdict.FALLBACK, verdict(h));
                    h.closed().toCompletableFuture().get(10, TimeUnit.SECONDS);
                    Throwable end = peer.end().get(10, TimeUnit.SECONDS);
                    if (m > 0) assertNotNull(end, "a failed fallback ends the carrier abortively");
                    slotBack(l, h);
                }
                var st = l.stats().admission();
                assertEquals(1, st.closed());
                assertEquals(2, st.fallback());
                addsUp(st, 3);
            }
        }
    }

    /**
     * A message carrier given back after a deferral: the takeover replays the first message
     * whole, then the messages that follow; its writes reach the peer, and the carrier is closed
     * once the fallback's stage completes.
     */
    @Test void aDeferredMessageCarrierIsTakenOverWithItsFirstMessage() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            var taken = new LinkedBlockingQueue<Takeover>();
            var done = new CompletableFuture<Void>();
            Admission.Fallback fallback = t -> { taken.add(t); return done; };
            try (var l = listener(loops, deciding(0, calls, (info, prefix) -> info.stage() == Admission.Stage.HEAD
                    ? new Admission.Decision(AdmissionVerdict.DEFER, 0) : Admission.Decision.FALLBACK), fallback)) {
                var up = new Up();
                up.stage.complete(null);
                var ws = MemoryPipe.pair(Framing.MESSAGES);
                var peer = Reading.of(ws[0]);
                var h = l.adapter().admitFirst(ws[1], request(), up, (t) -> { fail("the carrier is no request now"); return null; });
                send(ws[0], "first".getBytes(UTF_8), "second".getBytes(UTF_8));
                assertEquals(AdmissionVerdict.FALLBACK, verdict(h));
                Takeover t = taken.poll(10, TimeUnit.SECONDS);
                assertNotNull(t);
                assertEquals(Admission.Stage.CARRIER, t.info().stage());
                assertTrue(t.info().upgrade());
                assertEquals(Framing.MESSAGES, t.pipe().framing());
                assertTrue(t.socket().isEmpty());
                var got = Reading.of(t.pipe());
                assertEquals("first", new String(got.next(), UTF_8));
                assertEquals("second", new String(got.next(), UTF_8));
                send(ws[0], "third".getBytes(UTF_8));
                assertEquals("third", new String(got.next(), UTF_8));
                send(t.pipe(), "reply".getBytes(UTF_8));
                assertEquals("reply", new String(peer.next(), UTF_8));
                assertFalse(h.closed().toCompletableFuture().isDone(), "the host holds the carrier");
                done.complete(null);
                h.closed().toCompletableFuture().get(10, TimeUnit.SECONDS);
                peer.end().get(10, TimeUnit.SECONDS);
                slotBack(l, h);
                addsUp(l.stats().admission(), 1);
            }
        }
    }

    @Test void deferredHandbacksCountTheFinalVariantAndUseStallOverflowWithoutATarget() throws Exception {
        try (var loops = EventLoops.create(1)) {
            for (var verdict : java.util.List.of(AdmissionVerdict.FALLBACK, AdmissionVerdict.TAKEOVER)) {
                for (boolean hasTarget : new boolean[] { false, true }) {
                    var reached = new AtomicInteger();
                    var admission = deciding(0, new AtomicInteger(), (info, prefix) -> info.stage() == Admission.Stage.HEAD
                            ? new Admission.Decision(AdmissionVerdict.DEFER, 0).withVariant("head")
                            : new Admission.Decision(verdict, 0).withVariant("carrier"));
                    try (var l = listener(loops, admission, hasTarget ? takeover -> { reached.incrementAndGet(); return null; } : null, true)) {
                        var pair = MemoryPipe.pair(Framing.MESSAGES);
                        var up = new Up();
                        var h = l.adapter().admitFirst(pair[1], request(), up, null);
                        up.stage.complete(null);
                        send(pair[0], new byte[] {1});
                        assertEquals(hasTarget ? verdict : AdmissionVerdict.CLOSE, verdict(h));
                        h.closed().get(5, TimeUnit.SECONDS);
                        assertEquals(hasTarget ? 1 : 0, reached.get());
                        long fallback = hasTarget && verdict == AdmissionVerdict.FALLBACK ? 1 : 0;
                        long takeover = hasTarget && verdict == AdmissionVerdict.TAKEOVER ? 1 : 0;
                        long overflow = hasTarget ? 0 : 1;
                        assertEquals(new AdmissionStats(0, fallback, takeover, 0, 0, overflow, 0, 0, 0), l.stats().admission());
                        assertEquals(java.util.List.of(new Admission.VariantStat("carrier", false, 0, fallback, takeover, overflow, 0)),
                                l.variantStats());
                    }
                }
            }
        }
    }

    /** A head given back is a request: the entry's own fallback answers it, the listener's is not called. */
    @Test void aHeadGivenBackGoesToTheRequestFallback() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            var listenerTaken = new AtomicInteger();
            try (var l = listener(loops, deciding(0, calls, (info, prefix) -> Admission.Decision.FALLBACK),
                    t -> { listenerTaken.incrementAndGet(); return null; })) {
                var answered = new CompletableFuture<Admission.CarrierInfo>();
                var up = new Up();
                var h = l.adapter().admitFirst(MemoryPipe.pair(Framing.MESSAGES)[1], request(), up, t -> {
                    answered.complete(t.info());
                    return null;
                });
                assertEquals(AdmissionVerdict.FALLBACK, verdict(h));
                assertEquals(Admission.Stage.HEAD, answered.get(10, TimeUnit.SECONDS).stage());
                assertEquals(0, up.opened.get(), "nothing upgraded");
                h.closed().toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertEquals(0, listenerTaken.get());
            }
        }
    }

    @Test void withdrawingAnHttpFallbackBeforeItsDispatchCountsClose() throws Exception {
        try (var loops = EventLoops.create(1);
             var l = listener(loops, deciding(0, new AtomicInteger(), (i, p) -> Admission.Decision.FALLBACK.withVariant("request")), null)) {
            var dispatched = new CompletableFuture<java.util.function.BooleanSupplier>();
            var completion = new CompletableFuture<Void>();
            var fallback = new io.zmux.internal.spi.AdapterPorts.RequestFallback(false, (request, handoff) -> {
                dispatched.complete(handoff);
                return completion;
            });
            var h = l.adapter().handleRequest(MemoryPipe.pair(Framing.BYTE_STREAM)[1], request(), true, fallback,
                    () -> { throw new AssertionError("unexpected acceptance head"); });
            var handoff = dispatched.get(5, TimeUnit.SECONDS);
            addsUp(l.stats().admission(), 0);
            io.zmux.internal.spi.AdapterPorts.setup(h).interrupt();
            assertEquals(AdmissionVerdict.CLOSE, verdict(h));
            assertFalse(handoff.getAsBoolean());
            completion.complete(null);
            assertEquals(new AdmissionStats(0, 0, 0, 0, 1, 0, 0, 0, 0), l.stats().admission());
            assertEquals(java.util.List.of(new Admission.VariantStat("request", false, 0, 0, 0, 0, 1)), l.variantStats());
        }
    }

    @Test void aCompletedHttpDispatchThatDidNotHandOffClosesTheCarrier() throws Exception {
        try (var loops = EventLoops.create(1);
             var l = listener(loops, deciding(0, new AtomicInteger(), (i, p) -> Admission.Decision.FALLBACK.withVariant("request")), null)) {
            var fallback = new io.zmux.internal.spi.AdapterPorts.RequestFallback(false,
                    (request, handoff) -> CompletableFuture.completedFuture(null));
            var h = l.adapter().handleRequest(MemoryPipe.pair(Framing.BYTE_STREAM)[1], request(), true, fallback,
                    () -> { throw new AssertionError("unexpected acceptance head"); });
            assertEquals(AdmissionVerdict.CLOSE, verdict(h));
            h.closed().get(5, TimeUnit.SECONDS);
            assertEquals(new AdmissionStats(0, 0, 0, 0, 1, 0, 0, 0, 0), l.stats().admission());
            assertEquals(java.util.List.of(new Admission.VariantStat("request", false, 0, 0, 0, 0, 1)), l.variantStats());
        }
    }

    /** A plain TCP carrier offers its socket: blocking, off zmux, with the bytes read before the decision. */
    @Test void aTcpCarrierIsTakenOverAsItsSocket() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var calls = new AtomicInteger();
            var replayed = new CompletableFuture<byte[]>();
            var bothViews = new AtomicReference<String>();
            var entered = new CompletableFuture<Void>();
            Admission.Fallback fallback = t -> {
                var s = t.socket().orElseThrow();
                entered.complete(null);
                bothViews.set(t.socket().isPresent() + "");
                try {
                    assertTrue(s.channel().isBlocking());
                    byte[] r = new byte[s.replay().remaining()];
                    s.replay().get(r);
                    var more = ByteBuffer.allocate(2);
                    while (more.hasRemaining()) s.channel().read(more);
                    byte[] all = java.util.Arrays.copyOf(r, r.length + 2);
                    System.arraycopy(more.array(), 0, all, r.length, 2);
                    replayed.complete(all);
                    s.channel().write(ByteBuffer.wrap("ok".getBytes(UTF_8)));
                } catch (IOException e) {
                    replayed.completeExceptionally(e);
                }
                return null;
            };
            var lc = ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build())
                    .admission(deciding(3, calls, (info, prefix) -> Admission.Decision.FALLBACK).build()).fallback(fallback).build();
            try (var l = Zmux.listen(new java.net.InetSocketAddress("127.0.0.1", 0), lc);
                 SocketChannel c = SocketChannel.open(l.localAddress())) {
                c.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));
                entered.get(30, TimeUnit.SECONDS);
                assertEquals(1, calls.get());
                c.write(ByteBuffer.wrap(new byte[] {4, 5}));
                assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, replayed.get(10, TimeUnit.SECONDS));
                assertEquals("false", bothViews.get(), "taken once");
                c.socket().setSoTimeout(10_000);
                var in = c.socket().getInputStream();
                assertEquals('o', in.read());
                assertEquals('k', in.read());
                assertEquals(-1, in.read(), "closed when the fallback returned");
            }
        }
    }

    // HS-03. close executes inside the decider, before its completed verdict reaches the loop.
    @Test void stoppingInsideAdmissionOverridesAcceptAndFallback() throws Exception {
        // HS-03.
        for (var decision : java.util.List.of(Admission.Decision.accept(0), Admission.Decision.FALLBACK)) {
            var owner = new AtomicReference<ZmuxListener>();
            var cfg = deciding(0, new AtomicInteger(), (i,p) -> { owner.get().close(); return decision; });
            try (var loops = EventLoops.create(1); var l = listener(loops, cfg, t -> { throw new AssertionError("fallback"); })) {
                owner.set(l);
                var counts = new TakeoverTest.Counted();
                var h = l.adapter().handle(counts.wrap(MemoryPipe.pair(Framing.BYTE_STREAM)[1]), hint());
                assertThrows(ListenerClosedException.class, h::awaitSetup);
                h.closed().get(3, TimeUnit.SECONDS);
                assertEquals(0, l.queued.get());
                assertEquals(1, counts.readerCloses.get());
                assertEquals(1, counts.writerCloses.get());
            }
        }
    }

    // Observe the deadline while the synchronous host invocation is still on its stack.
    @Test void lateDeciderHoldsItsQuotaUntilItReturns() throws Exception {
        // HS-55.
        var entered = new CompletableFuture<Admission.Deadline>();
        var release = new CompletableFuture<Void>();
        var cfg = Admission.builder(0, (i,p,d) -> {
            entered.complete(d);
            release.join();
            return CompletableFuture.completedFuture(Admission.Decision.accept(0));
        }).peekTimeout(Duration.ofMillis(200));
        try (var loops = EventLoops.create(1); var l = listener(loops, cfg, null)) {
            var h = l.adapter().handle(MemoryPipe.pair(Framing.BYTE_STREAM)[1], hint());
            try {
                var d = entered.get(3, TimeUnit.SECONDS);
                var expired = new CompletableFuture<Void>();
                CompletableFuture.delayedExecutor(d.remaining().toNanos(), TimeUnit.NANOSECONDS)
                        .execute(() -> expired.complete(null));
                expired.get(3, TimeUnit.SECONDS);
                assertTrue(d.expired());
                var held = l.adapter().reserve(hint().source(), null);
                assertNotNull(held);
                try { assertNull(l.adapter().reserve(hint().source(), null)); }
                finally { held.release(); }
            } finally { release.complete(null); }
            assertThrows(IOException.class, h::awaitSetup);
            h.closed().get(3, TimeUnit.SECONDS);
            assertEquals(1, l.stats().admission().peekTimedOut());
            var slot = l.adapter().reserve(hint().source(), null);
            assertNotNull(slot);
            slot.release();
        }
    }

    // Cancellation is a call into a host-provided future too.
    @Test void throwingStageCancellationCannotStrandTheTransform() throws Exception {
        // HS-01, HS-07.
        var entered = new CompletableFuture<Void>();
        var stage = new CompletableFuture<PipeIo>() {
            @Override public boolean cancel(boolean interrupt) { throw new IllegalStateException("cancel failed"); }
        };
        try (var loops = EventLoops.create(1);
             var l = Zmux.listener(ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build())
                     .inboundTransform((io, ctx) -> { entered.complete(null); return stage; }).build())) {
            var counted = new TakeoverTest.Counted();
            var h = l.adapter().handle(counted.wrap(MemoryPipe.pair(Framing.BYTE_STREAM)[1]), hint());
            entered.get(3, TimeUnit.SECONDS);
            io.zmux.internal.spi.AdapterPorts.setup(h).interrupt();
            assertThrows(java.io.InterruptedIOException.class, h::awaitSetup);
            h.closed().get(3, TimeUnit.SECONDS);
            assertTrue(stage.isCompletedExceptionally());
            assertEquals(1, l.stats().setupFailures());
            assertEquals(1, counted.readerCloses.get());
            assertEquals(1, counted.writerCloses.get());
        }
    }

    @Test void anAcceptedCarriersLaterRefusalDoesNotCountAsAdmissionStallOverflow() throws Exception {
        try (var loops = EventLoops.create(1)) {
            var config = ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build()).maxPending(1)
                    .admission(deciding(0, new AtomicInteger(), (i, p) -> Admission.Decision.accept(0).withVariant("accepted")).build())
                    .inboundTransform((io, context) -> CompletableFuture.failedFuture(new CarrierRejectedException(0, null, "test refusal")))
                    .rejection(RejectInfo.Action.STALL).build();
            try (var l = Zmux.listener(config)) {
                var h = l.adapter().handle(MemoryPipe.pair(Framing.BYTE_STREAM)[1], hint());
                assertEquals(AdmissionVerdict.ACCEPT, verdict(h));
                h.closed().get(5, TimeUnit.SECONDS);
                assertEquals(1, l.stats().rejected());
                assertEquals(new AdmissionStats(1, 0, 0, 0, 0, 0, 0, 0, 0), l.stats().admission());
                assertEquals(java.util.List.of(new Admission.VariantStat("accepted", false, 1, 0, 0, 0, 0)), l.variantStats());
            }
        }
    }

    /** Every outcome of a listener counts once: the fields add up to the carriers that took a slot. */
    @Test void outcomesAddUp() throws Exception {
        // HS-19.
        try (EventLoops loops = EventLoops.create(1)) {
            var next = new AtomicReference<Admission.Decision>();
            var calls = new AtomicInteger();
            var lc = ListenerConfig.builder().session(ZmuxConfig.builder().eventLoops(loops).build()).maxPendingPerSource(8)
                    .admission(deciding(1, calls, (info, prefix) -> next.get()).maxPeek(8).maxMessage(4).stall(Duration.ofSeconds(20), Duration.ofSeconds(20)).build())
                    .fallback(t -> null).build();
            try (var l = Zmux.listener(lc)) {
                for (var d : new Admission.Decision[] {Admission.Decision.CLOSE, Admission.Decision.FALLBACK, Admission.Decision.STALL,
                        Admission.Decision.more(1), new Admission.Decision(AdmissionVerdict.DEFER, 0)}) {
                    next.set(d);
                    var p = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var h = l.adapter().handle(p[1], hint());
                    send(p[0], new byte[] {1});
                    verdict(h);
                }
                var big = MemoryPipe.pair(Framing.MESSAGES);
                var h = l.adapter().handle(big[1], hint());
                send(big[0], new byte[5]);
                verdict(h);
                next.set(Admission.Decision.TAKEOVER);
                var request = MemoryPipe.pair(Framing.BYTE_STREAM);
                var failed = l.adapter().handleRequest(request[1], request(), true, null,
                        () -> CompletableFuture.failedFuture(new IOException("head write failed")));
                assertThrows(IOException.class, failed::awaitSetup);
                failed.closed().get(3, TimeUnit.SECONDS);
                var st = l.stats().admission();
                assertEquals(new AdmissionStats(0, 1, 0, 1, 3, 0, 1, 0, 1), st);
                addsUp(st, 7);
            }
        }
    }
}
