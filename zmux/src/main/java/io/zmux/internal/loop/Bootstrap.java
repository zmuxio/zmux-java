package io.zmux.internal.loop;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.Session;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.join.Kdf;
import io.zmux.internal.spi.AdapterPorts;
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
        byte[] prefix(String alpn, String serverName, io.zmux.carrier.PipeIo.Exporter exporter, String tlsVersion) throws java.io.IOException;
    }

    /** The prefix bytes of a carrier, computed on its loop. */
    @FunctionalInterface
    private interface Prefix {
        byte[] get() throws java.io.IOException;
    }

    /** Holds a carrier while its prefix is written, then starts the session. */
    private record Presenting(Runnable then, java.util.function.Consumer<Throwable> failed) implements Inbound {
        @Override public int bytes(Transport t, io.zmux.internal.buffer.ReadBuf buf, java.nio.ByteBuffer data, int pos, int lim) { return 0; }
        @Override public void ended(Transport t, boolean graceful, boolean partial) {}

        @Override public void written(Transport t, io.zmux.internal.core.Batch b, int confirmed, Throwable cause) {
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
        present(c, () -> presenter.prefix(c.info().applicationProtocol(), c.serverName(), c::export, c.handshakeProtocol()), then, fail);
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
            c.reject(e);
            Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(e));
        };
        Runnable session = () -> {
            SessionRuntime driver = null;
            Runnable unregister = () -> {};
            try {
                CoreConfig admitted = cfg.withMemoryLedger();
                if (!CarrierCapacity.reserve(c, admitted.budget(), false)) {
                    throw new IllegalStateException("carrier backing exceeds memory budget");
                }
                SessionRuntime d = new SessionRuntime(loop, new Session(admitted, hooks.clockDomain().now()), hooks);
                driver = d;
                if (registration != null) unregister = registration.apply(d);
                d.attachBootstrap(c);
                Callbacks.EXECUTOR.execute(() -> f.complete(new Started(d, alpn)));
            } catch (RuntimeException e) {
                unregister.run();
                if (driver != null) driver.failInternalNow(e);
                c.reject(e);
                Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(e));
            }
        };
        Runnable r = prefix == null ? session : () -> present(c, prefix, session, fail);
        if (loop.inLoop()) r.run();
        else if (!loop.tryExecute(r)) {
            c.reject(null); f.completeExceptionally(new IllegalStateException("event loop closed"));
        }
        return f;
    }

    /**
     * Closes a pipe input handed to zmux that never became a carrier: zmux owns it whatever the
     * outcome, so every failure before it is adopted closes both halves, each once.
     */
    public static CompletableFuture<Void> discard(PipeIo io, Throwable cause) {
        if (io == null) return CompletableFuture.completedFuture(null);
        try { if (io.reader() != null) io.reader().close(); }
        catch (RuntimeException error) { if (cause != null && cause != error) cause.addSuppressed(error); }
        finally {
            try {
                if (io.writer() != null) {
                    if (cause == null) io.writer().close(); else io.writer().abort(cause);
                }
            } catch (RuntimeException error) { if (cause != null && cause != error) cause.addSuppressed(error); }
        }
        return AdapterPorts.released(io);
    }

    /**
     * cfg checked against its first pipe c, before any byte is written, and reading the session
     * exporter late from c, the one source: when the peer's parameters arrive, through c, so
     * that whatever connection serves c by then is the one read. A secure first pipe is the
     * session secret's source and takes no other; multipath or resume without any source, and
     * the exporter on a carrier without one, are configuration errors. On an error c is closed.
     */
    static CoreConfig withExporter(CoreConfig cfg, Transport c) {
        io.zmux.RecordMode r = c.firstPipeMode();
        io.zmux.internal.join.Mode first = r == null || r == io.zmux.RecordMode.UNMANAGED ? null : io.zmux.internal.join.Mode.of(r);
        TransportParams l = cfg.local();
        if (first != null) {
            if (l.secretFlags != 0) {
                c.reject(null);
                throw new IllegalArgumentException("a pre-shared key or the exporter in PipeAuth on a secure first pipe, whose Establish is the session secret's only source: leave PipeAuth unset or with withEndToEnd() only");
            }
            l.secretFlags = TransportParams.SECRET_EXPORTER;
            return cfg.toBuilder().secureFirstPipe(first).exporter(() -> c.setup().host(() -> c.export(Kdf.EXPORTER_SESSION, null, 32))).build();
        }
        if (l.has(TransportParams.MULTIPATH) && l.secretFlags == 0) {
            c.reject(null);
            throw new IllegalArgumentException("multipath or resume without a secret source: set PipeAuth with a pre-shared key"
                    + " (static or deferred) or the exporter, or use the secure carrier for the first pipe; withEndToEnd() alone is no source");
        }
        if ((l.secretFlags & TransportParams.SECRET_EXPORTER) == 0) return cfg;
        if (!c.exporterPresent()) {
            c.reject(null);
            throw new IllegalArgumentException("PipeAuth.exporter() on a carrier without an exporter");
        }
        return cfg.toBuilder().exporter(() -> c.setup().host(() -> c.export(Kdf.EXPORTER_SESSION, null, 32))).build();
    }

    /** Runs the TLS handshake of engine over ch; f gets the connection as a pipe input. */
    public static void tlsPipe(EventLoop loop, CoreConfig cfg, SocketChannel ch, SSLEngine engine, boolean secure,
                               CompletableFuture<PipeIo> f) {
        TlsTransport.handshake(loop, ch, engine, cfg.handshakeTimeout(), secure, cfg.withMemoryLedger().budget(), (c, err) -> {
            if (err != null) Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(err));
            else {
                PipeIo io = c.asPipe();
                // A dial cancelled meanwhile has nobody to take the connection: close it.
                Callbacks.EXECUTOR.execute(() -> { if (!f.complete(io)) c.reject(null); });
            }
        });
    }

    /** A session over a connected TCP or Unix-domain socket. */
    public static CompletableFuture<Started> socket(EventLoop loop, CoreConfig cfg, SocketChannel ch, DriverHooks hooks) {
        return socket(loop, cfg, ch, hooks, null);
    }

    public static CompletableFuture<Started> socket(EventLoop loop, CoreConfig cfg, SocketChannel ch, DriverHooks hooks,
                                                   java.util.function.Function<SessionRuntime, Runnable> registration) {
        SocketTransport c = new SocketTransport(loop, ch);
        return start(loop, withExporter(cfg, c), c, "", hooks, registration);
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
        var records = PipeIoAccess.get().records(io);
        try {
            if (records != null) carrier = new SealedTransport(carrier, records, io.capabilities().exporter());
        } catch (IllegalArgumentException belowFloor) {
            carrier.reject(belowFloor); // The carrier owns io now: the caller must not close io again.
            return CompletableFuture.failedFuture(belowFloor);
        }
        try {
            cfg = withExporter(cfg, carrier);
        } catch (IllegalArgumentException invalid) {
            // The carrier owns io now and was closed: the caller must not close io again.
            return CompletableFuture.failedFuture(invalid);
        }
        return start(carrier.loop(), cfg, carrier, io.capabilities().applicationProtocol(), hooks, registration);
    }

    /**
     * Runs the TLS handshake of engine over ch, then a session over it. A client without a
     * negotiated application protocol closes before sending any record.
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
        return tls(loop, cfg, ch, engine, false, hooks, registration, presenter);
    }

    /** As above; secure: the host declares the Bound contract for this connection. */
    public static CompletableFuture<Started> tls(EventLoop loop, CoreConfig cfg, SocketChannel ch, SSLEngine engine, boolean secure,
                                                DriverHooks hooks, java.util.function.Function<SessionRuntime, Runnable> registration,
                                                Presenter presenter) {
        CompletableFuture<Started> f = new CompletableFuture<>();
        CoreConfig admitted = cfg.withMemoryLedger();
        TlsTransport.handshake(loop, ch, engine, cfg.handshakeTimeout(), secure, admitted.budget(), (c, err) -> {
            if (err != null) {
                Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(err));
                return;
            }
            String alpn = c.info().applicationProtocol();
            if (engine.getUseClientMode() && alpn.isEmpty()) {
                c.shutdownOutput();
                c.reject(null);
                Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(
                        new javax.net.ssl.SSLHandshakeException("no application protocol negotiated")));
                return;
            }
            CoreConfig sc;
            try {
                sc = withExporter(admitted, c);
            } catch (IllegalArgumentException invalid) {
                Callbacks.EXECUTOR.execute(() -> f.completeExceptionally(invalid));
                return;
            }
            start(loop, sc, c, alpn, hooks, registration,
                    presenter == null ? null : () -> presenter.prefix(alpn, c.serverName(), c::export, c.handshakeProtocol())).whenComplete((s, e) -> {
                if (e != null) f.completeExceptionally(e);
                else f.complete(s);
            });
        });
        return f;
    }
}
