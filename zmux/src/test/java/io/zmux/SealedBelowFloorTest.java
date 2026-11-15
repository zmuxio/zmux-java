package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import io.zmux.seal.Seal;
import io.zmux.seal.ServerConfig;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A Sealed carrier whose records fall below the floor is refused the same way on every path; nothing else is hurt. */
@Timeout(60)
class SealedBelowFloorTest {
    static final int CAP = PipeIo.MIN_RECORD;

    /** addPipe fails (the pipe input is closed by zmux), the session carries on. */
    @Test void addPipeFailsAndTheSessionGoesOn() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = MaxRecordTest.config(loops);
            try (var a = ZmuxListener.of(ListenerConfig.of(cfg)); var b = ZmuxListener.of(ListenerConfig.of(cfg))) {
                var bootstrap = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("bootstrap")));
                try (var client = a.open(bootstrap[0], Role.CLIENT); var server = b.accept(Duration.ofSeconds(5))) {
                    var pair = MemoryPipe.pair(Framing.MESSAGES);
                    b.adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("joined")));
                    // Sealed, as the PSK asks, with no room for the floor: the failure is reported, not thrown from the loop.
                    assertThrows(IllegalArgumentException.class, () -> client.addPipe(pair[0].withMaxRecord(CAP), PipeOptions.defaults()));
                    assertEquals(1, client.pipes().size());
                    SealTest.echo(client, server);
                }
            }
        }
    }

    /** A listener over a sealing transform refuses the carrier below the floor and keeps taking others. */
    @Test void aListenerRefusesTheCarrierAndKeepsListening() throws Exception {
        var logged = new CopyOnWriteArrayList<String>();
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = SealTest.sessions(loops).build();
            InboundTransform seal = Seal.server(ServerConfig.builder(SealTest.gate(null), SealTest.NODE).build());
            // The sealed carrier this listener gets takes records the floor does not fit in.
            InboundTransform capped = (carrier, ctx) -> seal.transform(carrier, ctx).thenApply(io -> io.withMaxRecord(CAP));
            var lc = ListenerConfig.builder().session(cfg).logger(recorder(logged)).inboundTransform(capped).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                var f = Seal.establish(SealTest.tcp(l.localAddress()), SealTest.client().handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                try { f.get(10, java.util.concurrent.TimeUnit.SECONDS); } catch (Exception expected) { /* Either end may report it. */ }
                for (long end = System.nanoTime() + 5_000_000_000L; logged.isEmpty() && System.nanoTime() < end; ) Thread.sleep(10);
                assertEquals("the transformed carrier takes records below the floor; refused", logged.get(0));
                // The listener lives on: the next carrier is taken up and refused the same way.
                var again = Seal.establish(SealTest.tcp(l.localAddress()), SealTest.client().handshakeTimeout(Duration.ofSeconds(5)).build()).toCompletableFuture();
                try { again.get(10, java.util.concurrent.TimeUnit.SECONDS); } catch (Exception expected) { /* Either end may report it. */ }
                for (long end = System.nanoTime() + 5_000_000_000L; logged.size() < 2 && System.nanoTime() < end; ) Thread.sleep(10);
                assertEquals(2, logged.size(), logged.toString());
            }
        }
    }

    private static System.Logger recorder(java.util.List<String> into) {
        return new System.Logger() {
            @Override public String getName() { return "test"; }
            @Override public boolean isLoggable(Level level) { return true; }
            @Override public void log(Level level, java.util.ResourceBundle b, String msg, Throwable t) { into.add(msg); }
            @Override public void log(Level level, java.util.ResourceBundle b, String format, Object... params) { into.add(format); }
        };
    }
}
