package io.zmux.seal;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.HandshakeCost;
import io.zmux.carrier.HandshakeShaper;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Session;
import io.zmux.shaping.ProfileLimits;
import io.zmux.shaping.ShapingBudget;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class EstablishProfileTest {
    @Test void theSelectedProfileBoundsEstablishBeforeWriting() throws Exception {
        try (var key = StaticKey.fromScalar(new byte[StaticKey.KEY_LEN])) {
            checkProfiles(key);
        }
    }

    private static void checkProfiles(StaticKey key) throws Exception {
        var gate = new Gate() {
            @Override public byte[] resolve(byte[] handle, java.net.SocketAddress source) { return null; }
            @Override public Credential credential(byte[] handle, Profile profile, java.net.SocketAddress source) { return null; }
            @Override public Object commit(CommitRequest request, java.net.SocketAddress source) { return null; }
        };
        for (long micros : new long[] {500, 4000}) {
            var profile = new ProfileLimits(10, 50_000, 540, Duration.ofNanos(micros * 1000));
            var client = ClientConfig.builder().shapingProfile(profile).build();
            var server = ServerConfig.builder(gate, key).shapingProfile(profile).build();
            var session = new Session(CoreConfig.builder()
                    .shapingBudget(new ShapingBudget(null, profile, false)).build(), 0);
            var carrier = PipeIo.of(null, new PipeWriter() {
                @Override public void write(io.zmux.carrier.OutboundBatch batch, Completion completed) {
                    completed.completed();
                }
                @Override public void close() {}
            });
            HandshakeShaper shaper = view -> new HandshakeShaper.UnitPlan(0, 0, 0, Duration.ofMillis(8));
            var clock = new AtomicLong();
            for (String message : List.of("hello", "reply", "finish")) {
                var limits = message.equals("reply") ? server.establishLimits() : client.establishLimits();
                var cost = new Seal.ShapeCost();
                long before = clock.get();
                Seal.shape(shaper, limits, carrier, message, 128, 128, cost, clock::addAndGet);
                assertEquals(micros * 1000, clock.get() - before);
                assertEquals(clock.get() - before, cost.delay);
                assertTrue(session.handshakeFits(new HandshakeCost(List.of(
                        new HandshakeCost.Unit(clock.get(), 128, cost.allowance, cost.extra, cost.delay)))));
            }
        }
    }
}
