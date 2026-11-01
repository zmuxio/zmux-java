package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import io.zmux.shaping.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class ShapedPipeTest {
    /** A factory that records the pipes it is asked for and shapes none of them. */
    static final class Seen implements ShaperFactory {
        final List<ShapedPipe> pipes = new CopyOnWriteArrayList<>();
        @Override public Shaper create(ShapedPipe pipe) { pipes.add(pipe); return null; }
        Set<CarrierKind> kinds() {
            Set<CarrierKind> s = new HashSet<>();
            for (ShapedPipe p : pipes) s.add(p.carrier());
            return s;
        }
        void await(int n) throws InterruptedException {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (pipes.size() < n && System.nanoTime() < until) Thread.sleep(5);
            assertTrue(pipes.size() >= n, pipes.toString());
        }
    }

    static MultipathConfig multipath() {
        return MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO);
    }

    /** A server configured with a shaper pads its very first record and every one after. */
    @Test void aConfiguredServerIsShapedFromItsFirstRecord() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var shaped = ZmuxConfig.builder().eventLoops(loops).shaper(ShapingTest.planShaper(p -> p.padMultiple(64))).build();
            var plain = ZmuxConfig.builder().eventLoops(loops).build();
            var m = MemoryPipe.pair(Framing.BYTE_STREAM);
            var rec = new ShapingTest.Recorder(m[1].writer());
            rec.startNow();
            PipeIo s = new PipeIo(m[1].reader(), rec, Framing.BYTE_STREAM, "memory", null);
            Future<ZmuxSession> sf = ex.submit(() -> Zmux.open(s, Role.SERVER, shaped));
            try (var client = Zmux.open(m[0], Role.CLIENT, plain); var server = sf.get(5, TimeUnit.SECONDS)) {
                ShapingTest.transfer(server, client, 5000);
                var recs = rec.records();
                assertFalse(recs.isEmpty());
                for (int[] r : recs) assertEquals(0, r[0] % 64, "record of " + r[0] + " bytes");
            }
        }
    }

    /** Both sides' factories see each pipe's declared kind, joined pipes included; events and stats carry it. */
    @Test void factoriesSeeTheDeclaredKindOfEveryPipe() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            Seen cs = new Seen(), ss = new Seen();
            var ccfg = ZmuxConfig.builder().eventLoops(loops).multipath(multipath()).shaper(cs).build();
            var scfg = ZmuxConfig.builder().eventLoops(loops).multipath(multipath()).shaper(ss).build();
            try (var listener = Zmux.listener(ListenerConfig.of(scfg))) {
                var first = MemoryPipe.pair(Framing.MESSAGES);
                listener.handle(first[1].withCarrier(CarrierKind.WEBSOCKET), JoinHint.inBand(TestAddr.peer("peer")), null);
                try (var client = Zmux.open(first[0].withCarrier(CarrierKind.WEBSOCKET), Role.CLIENT, ccfg);
                     var server = listener.accept(Duration.ofSeconds(5))) {
                    ShapingTest.transfer(client, server, 100);
                    ShapingTest.transfer(server, client, 100);
                    var second = MemoryPipe.pair(Framing.BYTE_STREAM);
                    listener.handle(second[1].withCarrier(CarrierKind.STREAMING), JoinHint.inBand(TestAddr.peer("peer")), null);
                    ZmuxPipe joined = client.paths().add(second[0].withCarrier(CarrierKind.STREAMING), PipeOptions.defaults());
                    assertEquals(CarrierKind.STREAMING, joined.stats().orElseThrow().carrier());
                    cs.await(2); ss.await(2);
                    assertEquals(Set.of(CarrierKind.WEBSOCKET, CarrierKind.STREAMING), cs.kinds());
                    assertEquals(Set.of(CarrierKind.WEBSOCKET, CarrierKind.STREAMING), ss.kinds());
                    for (ShapedPipe p : cs.pipes) {
                        assertEquals(p.id() == 0 ? CarrierKind.WEBSOCKET : CarrierKind.STREAMING, p.carrier(), p.toString());
                    }
                    Set<CarrierKind> evented = new HashSet<>();
                    while (!evented.contains(CarrierKind.STREAMING)) {
                        if (client.nextEvent(Duration.ofSeconds(5)) instanceof SessionEvent.OutboundPipeStateChanged c) evented.add(c.carrier());
                    }
                    assertTrue(evented.contains(CarrierKind.STREAMING), evented.toString());
                }
            }
        }
    }

    /** A raw TCP carrier declares no kind. */
    @Test void aRawTcpCarrierIsUndeclared() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            Seen cs = new Seen(), ss = new Seen();
            var ccfg = ZmuxConfig.builder().eventLoops(loops).shaper(cs).build();
            var scfg = ZmuxConfig.builder().eventLoops(loops).shaper(ss).build();
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(scfg));
                 var client = Zmux.connect(listener.localAddress(), ccfg);
                 var server = listener.accept(Duration.ofSeconds(5))) {
                ShapingTest.transfer(client, server, 100);
                cs.await(1); ss.await(1);
                assertEquals(Collections.singleton(null), cs.kinds());
                assertEquals(Collections.singleton(null), ss.kinds());
                assertNull(client.paths().list().getFirst().stats().orElseThrow().carrier());
            }
        }
    }
}
