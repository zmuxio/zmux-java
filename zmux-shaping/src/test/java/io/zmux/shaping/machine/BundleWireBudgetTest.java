package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.zmux.internal.core.Batch;
import io.zmux.internal.core.CarrierInfo;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Framing;
import io.zmux.internal.core.ReadBuf;
import io.zmux.internal.core.Session;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class BundleWireBudgetTest {
    private record Attempt(long millis, int bytes, long left, long spent, long injected, long dropped) {}

    @Test void coverDebitsAndDropsMatchGo() throws Exception {
        Bundle bundle;
        try (var input = Objects.requireNonNull(getClass().getResourceAsStream("/wire-budget-cover.json"))) {
            bundle = Bundle.parse(input.readAllBytes(), MachineOptions.defaults());
        }
        var config = CoreConfig.builder().role(CoreConfig.Role.INITIATOR).transportRole(CoreConfig.Role.INITIATOR)
                .shapingBudget(bundle.budget(null).get()).build();
        var session = new Session(config, 0);
        var peer = new Session(CoreConfig.builder().role(CoreConfig.Role.RESPONDER)
                .transportRole(CoreConfig.Role.RESPONDER).build(), 0);
        var carrier = new CarrierInfo(Framing.BYTES, false, true, false, true);
        session.attachBootstrap(carrier, carrier, null);
        peer.attachBootstrap(carrier, carrier, null);
        exchange(session, peer);
        exchange(peer, session);
        session.handleTimeout(1_000_000);
        while (transmit(session, 1_000_000) != 0) {
            // Drain establishment records before installing the cover machine.
        }
        session.setShaper(bundle.shaperFactory());

        // Trace from zmux-go's bundle parser, machine and session core, with the same clock and carrier.
        var attempts = List.of(
                new Attempt(2, 100, 100, 100, 1, 0),
                new Attempt(3, 100, 0, 200, 2, 0),
                new Attempt(4, 0, 0, 200, 2, 1),
                new Attempt(999, 0, 99, 200, 2, 2),
                new Attempt(1000, 100, 0, 300, 3, 2),
                new Attempt(2000, 100, 0, 400, 4, 2));
        for (Attempt want : attempts) {
            long nanos = want.millis() * 1_000_000;
            session.handleTimeout(nanos);
            int bytes = transmit(session, nanos);
            var meters = session.shapingMeters();
            var stats = session.shapingStats();
            assertEquals(want, new Attempt(want.millis(), bytes, meters.wireLeft(), meters.wireSpent(),
                    stats.injected(), stats.injectDropped()));
        }
    }

    private static void exchange(Session from, Session to) {
        var batch = new Batch();
        while (from.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, 0)) {
            var data = ByteBuffer.allocate(batch.bytes());
            for (int i = 0; i < batch.segmentCount(); i++) data.put(batch.segment(i));
            data.flip();
            from.writeDone(batch, null, 0);
            assertEquals(data.remaining(), to.receive(0, 0, new ReadBuf(data, null), data, 0, data.limit(), 0));
        }
    }

    private static int transmit(Session session, long nanos) {
        var batch = new Batch();
        if (!session.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, nanos)) return 0;
        int bytes = batch.bytes();
        session.writeDone(batch, null, nanos);
        return bytes;
    }
}
