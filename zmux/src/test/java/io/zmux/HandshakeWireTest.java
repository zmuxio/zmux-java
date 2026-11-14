package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.HandshakeShaper;
import io.zmux.carrier.HandshakeShaper.UnitPlan;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.seal.*;
import io.zmux.shaping.ShapingLimits;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** What a handshake plan does to the units on the carrier, per message, on both framings and record modes. */
@Timeout(60)
class HandshakeWireTest {
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final InetSocketAddress SRC = new InetSocketAddress("127.0.0.1", 9);

    /** The bytes of each write. */
    static final class Writes implements PipeWriter {
        final PipeWriter inner;
        final List<byte[]> units = Collections.synchronizedList(new ArrayList<>());

        Writes(PipeWriter inner) { this.inner = inner; }

        @Override public void write(OutboundBatch data, Completion done) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            for (ByteBuffer b : data.segments(new ByteBuffer[data.segmentCount()])) {
                var d = b.duplicate();
                byte[] a = new byte[d.remaining()];
                d.get(a);
                out.writeBytes(a);
            }
            units.add(out.toByteArray());
            inner.write(data, done);
        }
        @Override public void close() { inner.close(); }
        @Override public void abort(Throwable cause) { inner.abort(cause); }

        int length(int i) { return units.get(i).length; }
    }

    record Side(PipeIo io, Writes writes) {}

    static Side[] carrier(Framing f, int overhead) {
        var m = MemoryPipe.pair(f);
        Side[] s = new Side[2];
        for (int i = 0; i < 2; i++) {
            Writes w = new Writes(m[i].writer());
            s[i] = new Side(new PipeIo(m[i].reader(), w, f, "memory", null).withWriteOverhead(overhead), w);
        }
        return s;
    }

    static HandshakeShaper plans(Map<String, Integer> padTo) {
        return v -> {
            Integer t = padTo.get(v.handshake() + "/" + v.message());
            return t == null ? null : new UnitPlan(t, 0, 0, null);
        };
    }

    /** Runs an Establish and a Join under the plans and returns the writes of the join's carrier, joiner and acceptor. */
    static void run(Framing f, int overhead, RecordMode mode, HandshakeShaper shaper, Check check) throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            ZmuxConfig cfg = SealTest.sessions(loops).allowRecordMode((c, m, p) -> true).handshakeShaper(shaper).build();
            var seal = Seal.server(ServerConfig.builder(SealTest.gate(null), SealTest.NODE).allowRecordMode((c, m, p) -> true).shaper(shaper).build());
            try (ZmuxListener l = ZmuxListener.of(ListenerConfig.builder().session(cfg).inboundTransform(seal).build())) {
                Side[] first = carrier(f, overhead);
                l.adapter().handle(first[1].io(), JoinHint.inBand(SRC));
                PipeIo p = Seal.establish(first[0].io(), SealTest.client().shaper(shaper).build()).toCompletableFuture().get(10, java.util.concurrent.TimeUnit.SECONDS);
                try (var c = Zmux.open(p, Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                    Side[] second = carrier(f, overhead);
                    l.adapter().handle(second[1].io(), JoinHint.inBand(SRC));
                    var options = PipeOptions.builder().recordMode(mode).build();
                    c.addPipe(second[0].io(), options);
                    SealTest.echo(c, s);
                    check.check(first, second, c, s);
                }
            }
        }
    }

    interface Check { void check(Side[] establish, Side[] join, ZmuxSession c, ZmuxSession s) throws Exception; }

    private static void assertOn(int target, int unit, int overhead, String what) {
        int wire = unit + overhead;
        assertTrue(wire >= target && wire <= target + 2, what + ": " + wire + " on the carrier, planned " + target);
    }

    @Test void everyMessageReachesItsPlannedLength() throws Exception {
        Map<String, Integer> t = Map.of("establish/hello", 1700, "establish/reply", 1800, "establish/finish", 1100,
                "join/hello", 700, "join/reply", 800, "join/confirm", 300);
        for (Framing f : List.of(Framing.BYTE_STREAM, Framing.MESSAGES)) {
            for (int over : new int[] {0, 14}) {
                for (RecordMode mode : List.of(RecordMode.SEALED, RecordMode.TRUSTED)) {
                    String what = f + " over " + over + " " + mode;
                    run(f, over, mode, plans(t), (e, j, c, s) -> {
                        assertOn(1700, e[0].writes().length(0), over, what + " establish/hello");
                        assertOn(1100, e[0].writes().length(1), over, what + " establish/finish");
                        assertOn(1800, e[1].writes().length(0), over, what + " establish/reply");
                        assertOn(700, j[0].writes().length(0), over, what + " join/hello");
                        assertOn(800, j[1].writes().length(0), over, what + " join/reply");
                        assertEquals(300, j[0].writes().length(1) + over, what + " join/confirm");
                        assertTrue(c.shapingStats().wireSpent() > 0, what);
                        assertTrue(s.shapingStats().wireSpent() > 0, what);
                    });
                }
            }
        }
    }

    @Test void theMessageOfAKeylessHelloIsPaddedWithRandomBytes() throws Exception {
        var m = MemoryPipe.pair(Framing.MESSAGES);
        Writes w = new Writes(m[0].writer());
        PipeIo io = new PipeIo(m[0].reader(), w, Framing.MESSAGES, "memory", null);
        var cfg = ClientConfig.builder().anchor(new PinAnchor(KeyId.of(KeyAlg.X25519, SealTest.NODE.publicKey())))
                .shaper(v -> new UnitPlan(2000, 0, 0, null)).handshakeTimeout(Duration.ofMillis(200)).build();
        var stage = Seal.establish(io, cfg).toCompletableFuture();
        long until = System.nanoTime() + WAIT.toNanos();
        while (w.units.isEmpty() && System.nanoTime() < until) Thread.sleep(2);
        byte[] hello = w.units.getFirst();
        int zeros = 0;
        // After the header, e and the hybrid Profiles' plain encapsulation key.
        int from = 1300;
        assertTrue(hello.length >= 1990, "unpadded: " + hello.length);
        for (int i = from; i < hello.length; i++) if (hello[i] == 0) zeros++;
        assertTrue(zeros < (hello.length - from) / 8, zeros + " zero bytes in the padding of " + (hello.length - from));
        stage.cancel(true);
    }

    @Test void aReplyStaysWithinThreeTimesWhatArrived() throws Exception {
        run(Framing.BYTE_STREAM, 0, RecordMode.SEALED, plans(Map.of("join/reply", 60000)), (e, j, c, s) -> {
            int hello = j[0].writes().length(0);
            assertTrue(j[1].writes().length(0) <= 3 * hello && j[1].writes().length(0) >= 3 * hello - 2,
                    j[1].writes().length(0) + " for a Hello of " + hello);
        });
    }

    @Test void aFailingHookIsNoPlanAndAFault() throws Exception {
        HandshakeShaper bad = v -> { throw new IllegalStateException("plan"); };
        run(Framing.BYTE_STREAM, 0, RecordMode.SEALED, bad, (e, j, c, s) -> {
            assertTrue(c.shapingStats().faults() >= 2, c.shapingStats().toString()); // hello and confirm
            assertTrue(s.shapingStats().faults() >= 1, s.shapingStats().toString());
        });
    }

    @Test void aMessageWithoutAPlanIsCounted() throws Exception {
        run(Framing.BYTE_STREAM, 0, RecordMode.SEALED, plans(Map.of()), (e, j, c, s) -> {
            assertTrue(c.shapingStats().handshakeUnplanned() >= 2, c.shapingStats().toString());
            assertEquals(0, c.shapingStats().faults());
        });
    }

    @Test void limitsClearAPlan() {
        UnitPlan p = new UnitPlan(100, 16, 5, Duration.ofMillis(7));
        assertEquals(new UnitPlan(0, 0, 0, Duration.ofMillis(7)), io.zmux.internal.core.HandshakePlans.limited(p, ShapingLimits.defaults().withPadding(false)));
        assertEquals(new UnitPlan(100, 16, 5, null), io.zmux.internal.core.HandshakePlans.limited(p, ShapingLimits.defaults().withPause(false)));
        assertEquals(new UnitPlan(100, 16, 5, Duration.ofMillis(3)), io.zmux.internal.core.HandshakePlans.limited(p, ShapingLimits.defaults().withMaxDelay(Duration.ofMillis(3))));
    }
}
