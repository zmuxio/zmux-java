package io.zmux;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import java.util.concurrent.CompletableFuture;

/** Two connected sessions for tests. */
record Pair(ZmuxSession client, ZmuxSession server) implements AutoCloseable {

    static Pair memory(ZmuxConfig cfg) throws Exception {
        return memory(cfg, cfg, Framing.BYTE_STREAM);
    }

    static Pair memory(ZmuxConfig ccfg, ZmuxConfig scfg, Framing framing) throws Exception {
        PipeIo[] p = MemoryPipe.pair(framing);
        CompletableFuture<ZmuxSession> server = CompletableFuture.supplyAsync(() -> {
            try {
                return Zmux.open(p[1], Role.SERVER, scfg);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, r -> Thread.ofVirtual().start(r));
        ZmuxSession c = Zmux.open(p[0], Role.CLIENT, ccfg);
        return new Pair(c, server.get());
    }

    @Override
    public void close() throws Exception {
        client.close();
        server.close();
        client.awaitClosed();
        server.awaitClosed();
    }
}
