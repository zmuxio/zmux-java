package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import io.zmux.internal.core.*;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class PairingShutdownTest {
    @Test void downlinkOwnerShutdownRacesTransferWithoutDoubleClosingOrLosingHalves() throws Exception {
        Session peer = new Session(CoreConfig.defaults(), Clock.now());
        CarrierInfo info = new CarrierInfo(io.zmux.internal.core.Framing.BYTES, false, false, false, false);
        peer.pipes().attachBootstrap(info, info, null); Batch batch = new Batch();
        assertTrue(peer.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, Clock.now()));
        byte[] preface = io.zmux.internal.core.BatchFixture.flat(batch); batch.discard();
        for (int iteration=0; iteration<24; iteration++) {
            EventLoop up = new EventLoop("pair-up", true, BufferPool.DEFAULT_IDLE_BYTES), down = new EventLoop("pair-down", true, BufferPool.DEFAULT_IDLE_BYTES);
            AtomicInteger readsClosed = new AtomicInteger(), writesClosed = new AtomicInteger();
            var hub = new ListenerHub(new ListenerHub.Owner() {
                @Override public CoreConfig config() { return CoreConfig.builder().role(CoreConfig.Role.RESPONDER).transportRole(CoreConfig.Role.RESPONDER).build(); }
                @Override public DriverHooks hooks() { return DriverHooks.NONE; }
                @Override public void started(SessionRuntime driver, String protocol) {}
            }, null, new byte[0], 8, 0, 0);
            try {
                PipeReader reader = new PipeReader() {
                    @Override public void start(Sink sink) { sink.onData(ByteBuffer.wrap(preface), () -> {}); }
                    @Override public void close() { readsClosed.incrementAndGet(); }
                };
                PipeWriter writer = new PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch bytes, Completion done) { done.completed(); }
                    @Override public void close() { writesClosed.incrementAndGet(); }
                };
                String token = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
                var a = hub.external(up, PipeIo.of(reader, null), reserved(hub, io.zmux.JoinHint.pair(token, true, null)), false, null, null, null);
                var b = hub.external(down, PipeIo.of(null, writer), reserved(hub, io.zmux.JoinHint.pair(token, false, null)), false, null, null, null);
                down.close(); hub.close(); up.close();
                a.whenClosed().toCompletableFuture().get(5, TimeUnit.SECONDS); b.whenClosed().toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertEquals(1, readsClosed.get()); assertEquals(1, writesClosed.get());
                assertEquals(0, up.pool.outstanding()); assertEquals(0, down.pool.outstanding());
            } finally { hub.close(); up.close(); down.close(); }
        }
    }

    private static io.zmux.JoinHint reserved(ListenerHub hub, io.zmux.JoinHint hint) {
        if (!hub.reserve(hint.source())) throw new AssertionError("no slot");
        return hint;
    }
}
