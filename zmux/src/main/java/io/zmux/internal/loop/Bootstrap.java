package io.zmux.internal.loop;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Session;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.JoinCrypto;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.SSLEngine;

/** Starts sessions on a loop over the built-in carriers or a pipe input. */
public final class Bootstrap {
    private Bootstrap() {}

    /** A started session: its driver and the negotiated application protocol ("" when none). */
    public record Started(SessionRuntime driver, String applicationProtocol) {}

    /**
     * A client's credential presentation: the bytes its carrier starts with, before any
     * record, given the carrier's completed handshake (alpn and serverName "" and exporter
     * null without TLS). Called on the carrier's loop; an exception fails the session.
     */
    @FunctionalInterface
    public interface Presenter {
        byte[] prefix(String alpn, String serverName, io.zmux.carrier.PipeIo.Exporter exporter) throws java.io.IOException;
    }

    /** The prefix bytes of a carrier, computed on its loop. */
    @FunctionalInterface
    private interface Prefix {
        byte[] get() throws java.io.IOException;
    }

    /** Holds a carrier while its prefix is written, then starts the session. */
    private record Presenting(Runnable then, java.util.function.Consumer<Throwable> failed) implements Inbound {
        @Override public int bytes(Transport t, io.zmux.internal.core.ReadBuf buf, java.nio.ByteBuffer data, int pos, int lim) { return 0; }
        @Override public void ended(Transport t, boolean graceful, boolean partial) {}

        @Override public void written(Transport t, io.zmux.internal.core.Batch b, Throwable cause) {
            b.discard();
            if (cause != null) failed.accept(cause);
            else then.run();
        }
    }

    private static CompletableFuture<Started> start(SessionHost loop, CoreConfig cfg, Transport c, String alpn,
                                                    DriverHooks hooks, java.util.function.Function<SessionRuntime, Runnable> registration) {
        return start(loop, cfg, c, alpn, hooks, registration, null);
    }

    /**
     * On c's loop, before c is started: writes the prefix of presenter on the TLS carrier c
     * (its handshake completed), then runs then; fail gets the failure (c is still open).
     */
    static void present(TlsTransport c, Presenter presenter, Runnable then, java.util.function.Consumer<Throwable> fail) {
        present(c, () -> presenter.prefix(c.info().applicationProtocol(), c.serverName(), c::export), then, fail);
    }

    private static void present(Transport c, Prefix prefix, Runnable then, java.util.function.Consumer<Throwable> fail) {
        byte[] bytes;
        try {
            bytes = prefix.get();
        } catch (java.io.IOException | RuntimeException e) {
            fail.accept(e);
            return;
        }
        if (bytes == null || bytes.length == 0) {
            then.run();
            return;
        }
        if (c.info().carrier().framing() != io.zmux.internal.core.Framing.BYTES) {
            fail.accept(new IllegalArgumentException("a credential prefix needs a byte-stream carrier"));
            return;
        }
        c.rebind(new Presenting(then, fail));
        io.zmux.internal.core.Batch b = new io.zmux.internal.core.Batch();
        b.wrap(io.zmux.carrier.OutboundBatch.of(java.nio.ByteBuffer.wrap(bytes)));
        try {
            c.write(b);
        } catch (RuntimeException e) {
            b.discard();
            fail.accept(e);
        }
    }

    /** Starts a session on c, after writing prefix's bytes (prefix null: none). */
    private static CompletableFuture<Started> start(SessionHost loop, CoreConfig cfg, Transport c, String alpn,
                                                    DriverHooks hooks, java.util.function.Function<SessionRuntime, Runnable> registration,
                                                    Prefix prefix) {
        CompletableFuture<Started> f = new CompletableFuture<>();
        java.util.function.Consumer<Throwable> fail = e -> {
            c.close(e);
            Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(e));
        };
        Runnable session = () -> {
            SessionRuntime driver = null;
            Runnable unregister = () -> {};
            try {
                SessionRuntime d = new SessionRuntime(loop, new Session(cfg, Clock.now()), hooks);
                driver = d;
                if (registration != null) unregister = registration.apply(d);
                d.attachBootstrap(c);
                Callbacks.EXECUTOR.execute(() -> f.complete(new Started(d, alpn)));
            } catch (RuntimeException e) {
                unregister.run();
                if (driver != null) driver.failInternal(e);
                c.close(e);
                Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(e));
            }
        };
        Runnable r = prefix == null ? session : () -> present(c, prefix, session, fail);
        if (loop.inLoop()) r.run();
        else if (!loop.tryExecute(r)) {
            c.close(null); f.completeExceptionally(new IllegalStateException("event loop closed"));
        }
        return f;
    }

    /** A session over a connected TCP or Unix-domain socket. */
    public static CompletableFuture<Started> socket(EventLoop loop, CoreConfig cfg, SocketChannel ch, DriverHooks hooks) {
        return socket(loop, cfg, ch, hooks, null);
    }

    public static CompletableFuture<Started> socket(EventLoop loop, CoreConfig cfg, SocketChannel ch, DriverHooks hooks,
                                                   java.util.function.Function<SessionRuntime, Runnable> registration) {
        return start(loop, cfg, new SocketTransport(loop, ch), "", hooks, registration);
    }

    /** A session over a pipe input. */
    public static CompletableFuture<Started> external(java.util.function.Supplier<? extends SessionHost> loop, CoreConfig cfg,
                                                     PipeIo io, DriverHooks hooks) {
        return external(loop, cfg, io, hooks, null);
    }

    /**
     * A session over a pipe input: on the host of the input's own carrier when it offers one
     * ({@link NativePipe}), else on the one loop supplies.
     */
    public static CompletableFuture<Started> external(java.util.function.Supplier<? extends SessionHost> loop, CoreConfig cfg,
                                                     PipeIo io, DriverHooks hooks,
                                                     java.util.function.Function<SessionRuntime, Runnable> registration) {
        Transport carrier = Transport.claim(io);
        if (carrier == null) carrier = new ExternalTransport(loop.get(), io);
        if ((cfg.local().secretFlags & TransportParams.SECRET_EXPORTER) != 0) {
            cfg = cfg.toBuilder().exporter(carrier.export(JoinCrypto.EXPORTER_SESSION, null, carrier.info().exporterLength())).build();
        }
        return start(carrier.loop(), cfg, carrier, io.capabilities().applicationProtocol(), hooks, registration);
    }

    /**
     * Runs the TLS handshake of engine over ch, then a session over it. A client without a
     * negotiated application protocol closes before sending any record (SPEC.md Section 6.2).
     */
    public static CompletableFuture<Started> tls(EventLoop loop, CoreConfig cfg, SocketChannel ch, SSLEngine engine,
                                                 DriverHooks hooks) {
        return tls(loop, cfg, ch, engine, hooks, null);
    }

    public static CompletableFuture<Started> tls(EventLoop loop, CoreConfig cfg, SocketChannel ch, SSLEngine engine,
                                                DriverHooks hooks, java.util.function.Function<SessionRuntime, Runnable> registration) {
        return tls(loop, cfg, ch, engine, hooks, registration, null);
    }

    /** A TLS session whose carrier starts, after the handshake, with presenter's prefix (null: none). */
    public static CompletableFuture<Started> tls(EventLoop loop, CoreConfig cfg, SocketChannel ch, SSLEngine engine,
                                                DriverHooks hooks, java.util.function.Function<SessionRuntime, Runnable> registration,
                                                Presenter presenter) {
        CompletableFuture<Started> f = new CompletableFuture<>();
        TlsTransport.handshake(loop, ch, engine, cfg.handshakeTimeout(), (c, err) -> {
            if (err != null) {
                Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(err));
                return;
            }
            String alpn = c.info().applicationProtocol();
            if (engine.getUseClientMode() && alpn.isEmpty()) {
                c.shutdownOutput();
                c.close(null);
                Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(
                        new javax.net.ssl.SSLHandshakeException("no application protocol negotiated")));
                return;
            }
            CoreConfig sc = (cfg.local().secretFlags & TransportParams.SECRET_EXPORTER) == 0 ? cfg
                    : cfg.toBuilder().exporter(c.export(JoinCrypto.EXPORTER_SESSION, null, 32)).build();
            start(loop, sc, c, alpn, hooks, registration,
                    presenter == null ? null : () -> presenter.prefix(alpn, c.serverName(), c::export)).whenComplete((s, e) -> {
                if (e != null) f.completeExceptionally(e);
                else f.complete(s);
            });
        });
        return f;
    }
}
