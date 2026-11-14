package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeWriter;
import io.zmux.seal.Seal;
import io.zmux.seal.ServerConfig;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Duration;

/**
 * A client that puts its first zmux write into message 3 as carrier first bytes (no public interface offers
 * that: a sender needs the sans-IO client). The server's session must start from them.
 */
final class EstablishFirstBytes {
    private EstablishFirstBytes() {}

    static void run() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = SealTest.sessions(loops).allowRecordMode((c, m, p) -> true).build();
            var server = ServerConfig.builder(SealTest.gate(null), SealTest.NODE).allowRecordMode((c, m, p) -> true).build();
            var lc = ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(server)).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc)) {
                SocketChannel ch = SocketChannel.open(l.localAddress());
                PipeIo carrier = io.zmux.seal.SealTestAccess.handshakeUpToFinish(ch);
                PipeWriter raw = carrier.writer();
                PipeIo withFirst = new PipeIo(carrier.reader(), new PipeWriter() {
                    private boolean first = true;
                    @Override public void write(OutboundBatch batch, Completion done) {
                        if (!first) { raw.write(batch, done); return; }
                        first = false;
                        byte[] out = new byte[batch.bytes()];
                        ByteBuffer to = ByteBuffer.wrap(out);
                        for (int i = 0; i < batch.segmentCount(); i++) to.put(batch.segment(i).duplicate());
                        try {
                            io.zmux.seal.SealTestAccess.finishWith(ch, out);
                            done.completed();
                        } catch (Exception e) {
                            done.failed(e);
                        }
                    }
                    @Override public void close() { raw.close(); }
                }, Framing.BYTE_STREAM, null, null);
                try (var c = Zmux.open(io.zmux.seal.SealTestAccess.established(withFirst), Role.CLIENT, cfg); var s = l.accept(Duration.ofSeconds(10))) {
                    SealTest.echo(c, s);
                    assertEquals(RecordMode.TRUSTED, s.weakestRecordMode());
                }
            }
        }
    }
}
