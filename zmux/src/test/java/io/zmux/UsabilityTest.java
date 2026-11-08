package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Configuration errors before any byte, carrier ownership on every failure, the independent join
 * timeout, the unattended-resume event and the logger, the error names, and close after finish.
 */
@Timeout(30)
class UsabilityTest {
    private static final byte[] PSK = new byte[32];

    /** A pipe input that counts how often each half is closed and how many writes it took. */
    static final class Counting {
        final AtomicInteger readerClosed = new AtomicInteger(), writerClosed = new AtomicInteger(), writes = new AtomicInteger();
        final PipeIo io;

        Counting(PipeIo base) {
            PipeReader r = base.reader();
            PipeWriter w = base.writer();
            PipeReader reader = new PipeReader() {
                @Override public void start(Sink sink) { r.start(sink); }
                @Override public void close() { readerClosed.incrementAndGet(); r.close(); }
            };
            PipeWriter writer = new PipeWriter() {
                @Override public void write(OutboundBatch batch, Completion done) { writes.incrementAndGet(); w.write(batch, done); }
                @Override public void close() { writerClosed.incrementAndGet(); w.close(); }
                @Override public void abort(Throwable cause) { writerClosed.incrementAndGet(); w.abort(cause); }
            };
            io = PipeIo.of(reader, writer);
        }

        /** Both halves closed exactly once, and nothing written. */
        void closedUnwritten() throws Exception {
            PipePoolTest.await(() -> readerClosed.get() > 0 && writerClosed.get() > 0);
            Thread.sleep(50);
            assertEquals(1, readerClosed.get(), "reader closed once");
            assertEquals(1, writerClosed.get(), "writer closed once");
            assertEquals(0, writes.get(), "no byte written");
        }
    }

    private static Counting counting() {
        return new Counting(MemoryPipe.pair(Framing.BYTE_STREAM)[0]);
    }

    @Test void multipathOrResumeWithoutASourceFailsBeforeAnyByteAndClosesTheCarrier() throws Exception {
        for (ZmuxConfig cfg : new ZmuxConfig[] {
                ZmuxConfig.builder().maxPipes(2).build(),
                ZmuxConfig.builder().resumeTimeout(Duration.ofSeconds(5)).build(),
                ZmuxConfig.builder().maxPipes(2).pipeAuth(PipeAuth.none().withEndToEnd()).build()}) {
            var c = counting();
            var e = assertThrows(IllegalArgumentException.class, () -> Zmux.open(c.io, Role.CLIENT, cfg));
            assertTrue(e.getMessage().contains("secret source"), e.getMessage());
            c.closedUnwritten();
        }
    }

    @Test void aSecureFirstPipeTakesNoOtherSource() throws Exception {
        PipeIo.Exporter none = (label, context, n) -> null;
        for (PipeAuth auth : new PipeAuth[] {PipeAuth.psk(PSK), PipeAuth.deferred(), PipeAuth.exporter(), PipeAuth.psk(PSK).withEndToEnd()}) {
            var c = counting();
            var cfg = ZmuxConfig.builder().maxPipes(2).pipeAuth(auth).build();
            PipeIo secure = c.io.withEstablished(RecordMode.TRUSTED, none, null, null);
            var e = assertThrows(IllegalArgumentException.class, () -> Zmux.open(secure, Role.CLIENT, cfg));
            assertTrue(e.getMessage().contains("secure first pipe"), e.getMessage());
            c.closedUnwritten();
        }
    }

    @Test void aServerOrPeerWithoutAListenerRejectsJoinsAndResume() throws Exception {
        var multipath = ZmuxConfig.builder().maxPipes(2).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).build();
        var resume = ZmuxConfig.builder().resumeTimeout(Duration.ofSeconds(5)).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).build();
        for (Role role : new Role[] {Role.SERVER, Role.PEER}) {
            for (ZmuxConfig cfg : new ZmuxConfig[] {multipath, resume}) {
                var c = counting();
                assertThrows(IllegalArgumentException.class, () -> Zmux.open(c.io, role, cfg));
                c.closedUnwritten();
            }
        }
    }

    @Test void earlyDataTowardAZmuxPeerWithTheSymmetricRoleIsAnError() throws Exception {
        var c = counting();
        var cfg = ZmuxConfig.builder().earlyData(EarlyData.ZMUX_PEER).build();
        assertThrows(IllegalArgumentException.class, () -> Zmux.open(c.io, Role.PEER, cfg));
        c.closedUnwritten();
    }

    @Test void valuesOutOfRangeAreErrorsNeverCorrected() {
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().resumeTimeout(Duration.ZERO), "resume off is not calling it");
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().resumeTimeout(Duration.ofNanos(-1)));
        assertEquals(2, ZmuxConfig.builder().resumeTimeout(Duration.ofMillis(1).plusNanos(1)).pipeAuth(PipeAuth.psk(PSK)).build()
                .toCore(io.zmux.internal.core.CoreConfig.Role.INITIATOR, io.zmux.internal.core.CoreConfig.Role.INITIATOR).local().resumeGraceMillis,
                "a fraction of a millisecond is rounded up on the wire");
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().joinEarlyHello(-1));
        assertEquals(0, ZmuxConfig.builder().joinEarlyHello(0).build().joinEarlyHello(), "0 turns the first batch off");
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().joinTimeout(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().joinTimeout(Duration.ofSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().joinTimeout(Duration.ofSeconds(61)));
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().maxPipes(4).replayWindow(11).build(), "below 4 + 8");
        assertEquals(12, ZmuxConfig.builder().maxPipes(4).replayWindow(12).build().replayWindow().orElseThrow());
        assertThrows(IllegalArgumentException.class,
                () -> ZmuxConfig.builder().redialMinBackoff(Duration.ofSeconds(2)).redialMaxBackoff(Duration.ofSeconds(1)).build());
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().redialMinBackoff(Duration.ofSeconds(61)).build(),
                "a minimum above the default maximum");
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().sendBufferMax(1 << 20).build(), "below the send buffer");
        assertEquals(OptionalLong.empty(), ZmuxConfig.defaults().incomingUniStreams(), "unset: staged credit");
        assertEquals(OptionalLong.of(0), ZmuxConfig.builder().incomingUniStreams(0).build().incomingUniStreams(), "none");
        assertThrows(IllegalArgumentException.class, () -> ZmuxConfig.builder().incomingUniStreams(-1));
    }

    @Test void theJoinTimeoutBoundsAJoinWhileTheHandshakeHasNone() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).disableHandshakeTimeout()
                    .joinTimeout(Duration.ofMillis(400)).maxPipes(3).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).probation(Duration.ZERO).build();
            try (var f = new PipePoolTest.Fixture(cfg)) {
                long t0 = System.nanoTime();
                assertThrows(SocketTimeoutException.class, () -> PeerAttachedTest.lateJoin(f, 3_000, PipeOptions.defaults()));
                long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                assertTrue(took >= 350 && took < 2_000, "the join took " + took + " ms");
            }
        }
    }

    @Test void aFailingRecordPolicyClosesTheJoinsCarrier() throws Exception {
        var logger = new CapturingLogger();
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(3).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).probation(Duration.ZERO)
                    .recordPolicy(c -> { throw new IllegalStateException("policy failed"); }).logger(logger).build();
            try (var f = new PipePoolTest.Fixture(cfg)) {
                var c = counting();
                var e = assertThrows(java.io.IOException.class, () -> f.client.addPipe(c.io, PipeOptions.defaults()));
                assertInstanceOf(IllegalStateException.class, e.getCause());
                c.closedUnwritten();
                assertTrue(logger.has("record policy"));
            }
        }
    }

    @Test void joinPendingNamesWhatIsMissing() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            record Case(PipeAuth auth, boolean endToEnd, boolean psk, String text) {}
            for (Case k : new Case[] {
                    new Case(PipeAuth.psk(PSK), true, false, "end-to-end declaration"),
                    new Case(PipeAuth.deferred().withEndToEnd(), false, true, "deferred PSK"),
                    new Case(PipeAuth.deferred(), true, true, "and the deferred PSK")}) {
                var cfg = ZmuxConfig.builder().eventLoops(loops).maxPipes(3).pipeAuth(k.auth()).build();
                try (var f = new PipePoolTest.Fixture(cfg)) {
                    var c = counting();
                    var e = assertThrows(JoinPendingException.class, () -> f.client.addPipe(c.io, PipeOptions.defaults()));
                    assertEquals(k.endToEnd(), e.endToEndMissing());
                    assertEquals(k.psk(), e.pskMissing());
                    assertTrue(e.getMessage().contains(k.text()), e.getMessage());
                    c.closedUnwritten();
                }
            }
        }
    }

    @Test void theLastPipeErrorIsTheCauseOfAResumeTimeoutOrLostPipes() {
        var last = new java.io.IOException("refused");
        assertSame(last, new SessionClosedException(new SessionError.ResumeTimeout(last)).getCause());
        assertSame(last, new SessionClosedException(new SessionError.PipesLost(last)).getCause());
        assertNull(new SessionClosedException(new SessionError.ResumeFailed(true)).getCause());
    }

    private static ZmuxConfig.Builder resuming(EventLoops loops, CapturingLogger logger) {
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(2).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).probation(Duration.ZERO)
                .resumeTimeout(Duration.ofSeconds(20)).logger(logger);
    }

    /** Both ends of a resumable session whose client has a listener too, so that either can rejoin. */
    private record Rejoinable(ZmuxListener serverListener, ZmuxListener clientListener, ZmuxSession client, ZmuxSession server)
            implements AutoCloseable {
        static Rejoinable open(ZmuxConfig server, ZmuxConfig client) throws Exception {
            var sl = Zmux.listener(ListenerConfig.of(server));
            var cl = Zmux.listener(ListenerConfig.of(client));
            var io = MemoryPipe.pair(Framing.BYTE_STREAM);
            sl.handle(io[1], JoinHint.inBand(TestAddr.peer("bootstrap")));
            var c = cl.open(io[0], Role.CLIENT);
            return new Rejoinable(sl, cl, c, sl.accept(Duration.ofSeconds(5)));
        }

        @Override public void close() throws Exception {
            client.close();
            server.close();
            clientListener.close();
            serverListener.close();
        }
    }

    @Test void aSuspensionWithNothingToRedialIsReportedAtBothEnds() throws Exception {
        var clientLog = new CapturingLogger();
        var serverLog = new CapturingLogger();
        try (EventLoops loops = EventLoops.create(2);
             var r = Rejoinable.open(resuming(loops, serverLog).build(), resuming(loops, clientLog).build())) {
            var open = r.client().openStream();
            open.write(new byte[] {1});
            r.client().pipes().getFirst().abort();
            RelayTest.event(r.client(), SessionEvent.SessionSuspended.class, e -> true, 5_000);
            RelayTest.event(r.client(), SessionEvent.ResumeUnattended.class, e -> true, 5_000);
            RelayTest.event(r.server(), SessionEvent.ResumeUnattended.class, e -> true, 5_000);
            assertTrue(clientLog.has("nothing to redial"));
            assertTrue(clientLog.has("the last of a direction, while streams are open"));
            assertTrue(serverLog.has("nothing to redial"));
        }
    }

    @Test void aRedialHookResumesASuspendedSessionAndLeavesItAttended() throws Exception {
        var logger = new CapturingLogger();
        try (EventLoops loops = EventLoops.create(2)) {
            var server = resuming(loops, new CapturingLogger()).build();
            var listener = Zmux.listener(ListenerConfig.of(server));
            var dials = new AtomicInteger();
            var client = resuming(loops, logger).redial((need, cancellation) -> {
                dials.incrementAndGet();
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.handle(io[1], JoinHint.inBand(TestAddr.peer("redial")));
                return io[0];
            }).build();
            try (listener) {
                var io = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.handle(io[1], JoinHint.inBand(TestAddr.peer("bootstrap")));
                try (var c = Zmux.open(io[0], Role.CLIENT, client); var s = listener.accept(Duration.ofSeconds(5))) {
                    c.pipes().getFirst().abort();
                    RelayTest.event(c, SessionEvent.SessionResumed.class, e -> true, 10_000);
                    assertEquals(1, dials.get());
                    assertFalse(logger.has("nothing to redial"));
                    var st = c.openStream();
                    st.write(new byte[] {7});
                    st.finish();
                    try (var accepted = s.acceptStream(Duration.ofSeconds(5))) {
                        assertArrayEquals(new byte[] {7}, SessionTest.readAll(accepted));
                    }
                }
            }
        }
    }

    @Test void closeAfterFinishEndsTheStreamCleanly() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var p = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var s = p.client().openStream();
            s.write(new byte[] {1, 2, 3});
            s.finish();
            try (var accepted = p.server().acceptStream(Duration.ofSeconds(5))) {
                accepted.write(new byte[] {9});
                accepted.finish();
                assertArrayEquals(new byte[] {9}, SessionTest.readAll(s));
                s.close();
                assertArrayEquals(new byte[] {1, 2, 3}, SessionTest.readAll(accepted), "a FIN, no reset");
            }
        }
    }
}
