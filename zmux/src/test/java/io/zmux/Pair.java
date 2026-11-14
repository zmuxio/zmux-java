package io.zmux;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import java.util.concurrent.CompletableFuture;

/**
 * Two connected sessions for tests. A server whose configuration offers multipath is accepted
 * through a listener, since a server session without one cannot accept joins.
 */
record Pair(ZmuxSession client, ZmuxSession server, ZmuxListener listener) implements AutoCloseable {
    Pair(ZmuxSession client, ZmuxSession server) { this(client, server, null); }

    static Pair memory(ZmuxConfig cfg) throws Exception {
        return memory(cfg, cfg, Framing.BYTE_STREAM);
    }

    static Pair memory(ZmuxConfig ccfg, ZmuxConfig scfg, Framing framing) throws Exception {
        PipeIo[] p = MemoryPipe.pair(framing);
        if (scfg.isMultipath()) {
            ZmuxListener l = ZmuxListener.of(ListenerConfig.of(scfg));
            l.adapter().handle(p[1], JoinHint.inBand(TestAddr.peer("pair")));
            ZmuxSession c = Zmux.open(p[0], Role.CLIENT, ccfg);
            return new Pair(c, l.accept(java.time.Duration.ofSeconds(5)), l);
        }
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
        if (listener != null) listener.close();
    }
}
