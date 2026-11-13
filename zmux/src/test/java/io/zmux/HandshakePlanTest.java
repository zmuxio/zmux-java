package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.HandshakeShaper;
import io.zmux.carrier.HandshakeShaper.UnitPlan;
import io.zmux.carrier.PipeIo;
import io.zmux.seal.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** The handshake hook shared by Establish and Join, and split carriers. */
@Timeout(60)
class HandshakePlanTest {
    private static final Duration WAIT = Duration.ofSeconds(10);

    @Test void theHookPlansEveryMessageOfAnEstablishAndAJoin() throws Exception {
        Set<String> asked = ConcurrentHashMap.newKeySet();
        HandshakeShaper shaper = v -> {
            asked.add(v.handshake() + "/" + v.message());
            return new UnitPlan(700, 0, 0, Duration.ofMillis(1));
        };
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = SealTest.sessions(loops).handshakeShaper(shaper).build();
            var lc = ListenerConfig.builder().session(cfg).inboundTransform(
                    Seal.server(ServerConfig.builder(SealTest.gate(null), SealTest.NODE).shaper(shaper).build())).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                InetSocketAddress at = l.localAddress();
                PipeIo p = SealTest.establish(SealTest.tcp(at), SealTest.client().shaper(shaper).build());
                try (var c = Zmux.open(p, Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                    c.addPipe(at, PipeOptions.defaults());
                    assertTrue(Set.of("establish/hello", "establish/reply", "establish/finish", "join/reply", "join/confirm").stream().allMatch(asked::contains), asked.toString());
                    assertTrue(asked.contains("join/hello") || asked.contains("join/hello_early"), asked.toString());
                    SealTest.echo(c, s);
                }
            }
        }
    }

    @Test void aSplitCarrierRunsOneEstablishAcrossItsHalvesInEveryMode() throws Exception {
        for (RecordMode mode : List.of(RecordMode.SEALED, RecordMode.BOUND)) {
            try (EventLoops loops = EventLoops.create(2)) {
                ZmuxConfig cfg = SealTest.sessions(loops).allowRecordMode((c, m, peer) -> true).build();
                int[] calls = new int[1];
                var seal = Seal.server(ServerConfig.builder(SealTest.gate(null), SealTest.NODE).allowRecordMode((c, m, peer) -> c.secure()).build());
                InboundTransform once = (carrier, ctx) -> { calls[0]++; return seal.transform(carrier, ctx); };
                try (ZmuxListener l = ZmuxListener.of(ListenerConfig.builder().session(cfg).inboundTransform(once).build())) {
                    var up = MemoryPipe.pair(Framing.BYTE_STREAM);
                    var down = MemoryPipe.pair(Framing.BYTE_STREAM);
                    PipeIo.Exporter eu = (x, y, n) -> new byte[n], ed = (x, y, n) -> { byte[] b = new byte[n]; b[0] = 1; return b; };
                    String token = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
                    var src = new InetSocketAddress("127.0.0.1", 9);
                    l.handle(new PipeIo(up[1].reader(), null, Framing.BYTE_STREAM, null, null).withExporter(eu).withSecure(true), JoinHint.pair(token, true, src));
                    l.handle(new PipeIo(null, down[1].writer(), Framing.BYTE_STREAM, null, null).withExporter(ed).withSecure(true), JoinHint.pair(token, false, src));
                    PipeIo client = PipeIo.split(new PipeIo(null, up[0].writer(), Framing.BYTE_STREAM, null, null).withExporter(eu).withSecure(true),
                            new PipeIo(down[0].reader(), null, Framing.BYTE_STREAM, null, null).withExporter(ed).withSecure(true));
                    PipeIo p = Seal.establish(client, SealTest.client().mode(mode).build()).toCompletableFuture().get(10, TimeUnit.SECONDS);
                    try (var c = Zmux.open(p, Role.CLIENT, cfg); var s = l.accept(WAIT)) {
                        assertEquals(1, calls[0]);
                        assertEquals(mode, s.weakestRecordMode());
                        SealTest.echo(c, s);
                    }
                }
            }
        }
    }

    @Test void aSplitIsSecureOnlyWhenBothHalvesDeclareIt() {
        var a = MemoryPipe.pair(Framing.BYTE_STREAM);
        var b = MemoryPipe.pair(Framing.BYTE_STREAM);
        PipeIo.Exporter e = (x, y, n) -> new byte[n];
        PipeIo up = new PipeIo(null, a[0].writer(), Framing.BYTE_STREAM, null, null).withExporter(e);
        PipeIo down = new PipeIo(b[0].reader(), null, Framing.BYTE_STREAM, null, null).withExporter(e);
        assertFalse(PipeIo.split(up.withSecure(true), down).isSecure());
        assertFalse(PipeIo.split(up, down.withSecure(true)).isSecure());
        assertTrue(PipeIo.split(up.withSecure(true), down.withSecure(true)).isSecure());
    }
}
