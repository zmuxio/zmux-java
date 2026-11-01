package io.zmux;

import io.zmux.carrier.PipeIo;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.loop.Bootstrap;
import io.zmux.internal.loop.EventLoop;
import io.zmux.internal.loop.SessionHost;
import io.zmux.internal.loop.SessionRuntime;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLEngine;

/**
 * Entry points: sessions over TCP, TLS or any pipe input. Establishment returns once the
 * peer's parameters were processed, or at once when early data toward a declared zmux peer
 * is configured (API.md Section 3).
 */
public final class Zmux {
    private Zmux() {}

    /**
     * Transfers both streams to a bounded, bidirectional relay (API.md Section A.2). Cancelling the returned stage
     * ({@code toCompletableFuture().cancel(...)}) cancels the splice: both streams are reset and stopped with code 0
     * and there is no result. Otherwise the stage completes only after forwarding tasks and ownership cleanup have
     * finished, with how each direction ended, or exceptionally when a limiter failed.
     */
    public static java.util.concurrent.CompletionStage<SpliceResult> splice(ZmuxStream a, ZmuxStream b, SpliceOptions options) {
        return Splice.start(a, b, options);
    }

    public static java.util.concurrent.CompletionStage<SpliceResult> splice(ZmuxStream a, ZmuxStream b) {
        return splice(a, b, SpliceOptions.defaults());
    }

    /** Resolves two established sessions the application authenticated as connecting the same endpoints. */
    public static DuplicateResult resolveDuplicate(ZmuxSession a, ZmuxSession b, DuplicateOptions options) throws IOException {
        return Duplicates.resolve(a, b, options);
    }
    public static DuplicateResult resolveDuplicate(ZmuxSession a, ZmuxSession b) throws IOException {
        return resolveDuplicate(a, b, DuplicateOptions.defaults());
    }

    /** Connects within timeoutNanos (the handshake timeout); SocketTimeoutException after it. */
    static SocketChannel dial(InetSocketAddress address, long timeoutNanos) throws IOException {
        if (SessionHost.onHostThread()) throw new IllegalStateException("dial on a zmux event-loop thread");
        int millis = timeoutNanos >= io.zmux.internal.core.Time.NEVER / 2 ? 0
                : Math.clamp(Math.ceilDiv(Math.max(timeoutNanos, 1), 1_000_000L), 1, Integer.MAX_VALUE);
        SocketChannel ch = SocketChannel.open();
        try {
            ch.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
            ch.socket().connect(address, millis);
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
        return ch;
    }

    /** A client session over TCP with the default configuration. */
    public static ZmuxSession connect(InetSocketAddress address) throws IOException {
        return connect(address, ZmuxConfig.defaults());
    }

    /** A client session over TCP; the connection starts with the prefix of {@link ZmuxConfig#credential()}. */
    public static ZmuxSession connect(InetSocketAddress address, ZmuxConfig cfg) throws IOException {
        EventLoop loop = cfg.nextLoop();
        SocketChannel ch = dial(address, cfg.handshakeNanos());
        present(ch, cfg.credential(), address);
        return established(Bootstrap.socket(loop, cfg.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR), ch, cfg.hooks), cfg);
    }

    /** Writes the prefix of credential (null: none) on a connected blocking socket without TLS; closes it on failure. */
    static void present(SocketChannel ch, Credential credential, InetSocketAddress address) throws IOException {
        Bootstrap.Presenter p = presenter(credential, address);
        if (p == null) return;
        try {
            byte[] b = p.prefix("", "", null);
            if (b == null) return;
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b);
            while (buf.hasRemaining()) ch.write(buf);
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    /**
     * The presentation of credential on a carrier to address; null without a credential.
     */
    static Bootstrap.Presenter presenter(Credential credential, InetSocketAddress address) {
        if (credential == null) return null;
        return (alpn, serverName, exporter) ->
                credential.prefix(new Admission.CarrierInfo(address, address, alpn, serverName, exporter, RequestHead.NONE));
    }

    /** {@link #connect(InetSocketAddress, ZmuxConfig)} on a virtual thread. */
    public static CompletableFuture<ZmuxSession> connectAsync(InetSocketAddress address, ZmuxConfig cfg) {
        CompletableFuture<ZmuxSession> f = new CompletableFuture<>();
        Thread.ofVirtual().name("zmux-connect").start(() -> {
            try { f.complete(connect(address, cfg)); } catch (Throwable e) { f.completeExceptionally(e); }
        });
        return f;
    }

    /**
     * A client session over TLS; ALPN offers {@link ZmuxConfig#alpn()}. Once the handshake
     * completed, the connection starts with the prefix of {@link ZmuxConfig#credential()},
     * which may bind to the handshake's exporter.
     */
    public static ZmuxSession connectTls(InetSocketAddress address, TlsOptions tls, ZmuxConfig cfg) throws IOException {
        SSLEngine engine = tls.clientEngine(address.getPort(), cfg.alpn());
        EventLoop loop = cfg.nextLoop();
        SocketChannel ch = dial(address, cfg.handshakeNanos());
        return established(Bootstrap.tls(loop, cfg.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR), ch, engine, cfg.hooks,
                null, presenter(cfg.credential(), address)), cfg);
    }

    /**
     * A session in the given role over any pipe input. A credential is presented where the
     * pipe was dialed, not here ({@link Credential}).
     */
    public static ZmuxSession open(PipeIo io, Role role, ZmuxConfig cfg) throws IOException {
        CoreConfig.Role r = core(role);
        return external(io, cfg, r, r);
    }

    static CoreConfig.Role core(Role role) {
        return switch (role) {
            case CLIENT -> CoreConfig.Role.INITIATOR;
            case SERVER -> CoreConfig.Role.RESPONDER;
            case PEER -> CoreConfig.Role.AUTO;
        };
    }

    /**
     * Dials a client session over the carrier the mode selects (API.md Section 4.8); each
     * carrier presents cfg's {@link ZmuxConfig#credential()}. policy is the policy of
     * {@link CarrierMode#auto} (null: the defaults).
     *
     * <p>An explicit mode dials its one candidate as its own dialer does. An auto mode does not
     * use early data, so that nothing the application sends goes out before a carrier is chosen,
     * and it does not race candidates: it skips the kinds the cache holds as failed (unless all
     * are) and, when the configuration allows more than one pipe, establishes the session on the
     * most robust candidate (a kind the cache holds as usable, else the exchange carrier, else
     * the last) and joins the preferred ones as further pipes in the background, retiring the
     * pipes of the kind they replace once one has stayed joined for {@link ModePolicy#settle()};
     * it keeps retrying preferred kinds as their backoff expires, and joins the best kind
     * available again when the active one fails. Without joins (one pipe,
     * {@link ModePolicy#noJoin()}, or a peer without multipath) it tries the candidates one after
     * another, in order, and keeps the first usable one. A candidate is usable when the peer's
     * parameters arrive within its timer ({@link ModePolicy#timerFactor()}); one that misses it
     * is closed without CONNECTION_CLOSE.
     *
     * <p>Each try is reported as a {@link SessionEvent.CarrierCandidate}, the choice as a
     * {@link SessionEvent.CarrierChosen} (again on every switch), and both in
     * {@link SessionStats}.
     *
     * @throws ModeException when no candidate is usable, with the outcome of every one tried
     */
    public static ZmuxSession dial(CarrierMode mode, ZmuxConfig cfg, ModePolicy policy) throws IOException {
        return CarrierSelection.dial(mode, cfg, policy);
    }

    /** Listens for TCP carriers on address with the default configuration. */
    public static ZmuxListener listen(InetSocketAddress address) throws IOException {
        return listen(address, ListenerConfig.defaults());
    }

    /** Listens for TCP carriers, or TLS ones when {@link ListenerConfig#tls()} is set. */
    public static ZmuxListener listen(InetSocketAddress address, ListenerConfig cfg) throws IOException {
        return ZmuxListener.bind(address, cfg);
    }

    /** A listener without a socket: carriers arrive through {@link ZmuxListener#handle}. */
    public static ZmuxListener listener(ListenerConfig cfg) {
        return ZmuxListener.external(cfg);
    }

    private static ZmuxSession external(PipeIo io, ZmuxConfig cfg, CoreConfig.Role role, CoreConfig.Role transport)
            throws IOException {
        return established(startExternal(io, cfg, role, transport), cfg);
    }

    /** Starts a session over io without waiting for the peer's parameters. */
    static CompletableFuture<Bootstrap.Started> startExternal(PipeIo io, ZmuxConfig cfg, CoreConfig.Role role, CoreConfig.Role transport) {
        if (SessionHost.onHostThread()) throw new IllegalStateException("connect on a zmux event-loop thread");
        CompletableFuture<Bootstrap.Started> started;
        try {
            started = Bootstrap.external(() -> io.carrierConn() instanceof SessionRuntime parent
                    ? parent.loop() : cfg.nextLoop(), cfg.toCore(role, transport), io, cfg.hooks);
        } catch (RuntimeException error) {
            try { if (io.reader() != null) io.reader().close(); }
            finally { if (io.writer() != null) io.writer().abort(error); }
            throw error;
        }
        return started;
    }

    static ZmuxSession started(CompletableFuture<Bootstrap.Started> f, Credential credential) throws IOException {
        try {
            Bootstrap.Started s = f.get();
            return new ZmuxSession(s.driver(), s.driver().core(), s.applicationProtocol(), credential);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            f.thenAccept(s -> s.driver().post(() -> s.driver().core().close(0, null)));
            throw new InterruptedIOException("interrupted");
        } catch (ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
    }

    static ZmuxSession established(CompletableFuture<Bootstrap.Started> f, ZmuxConfig cfg) throws IOException {
        ZmuxSession s = started(f, cfg.credential());
        if (cfg.earlyData() == EarlyData.OFF) {
            try {
                s.awaitEstablished(Long.MAX_VALUE);
            } catch (IOException e) {
                s.close();
                throw e;
            }
        }
        return s;
    }
}
