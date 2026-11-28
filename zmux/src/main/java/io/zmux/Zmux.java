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
 * peer's current transport parameters have been processed.
 */
public final class Zmux {
    private Zmux() {}

    /**
     * The largest stream count credit the encoding allows, 2<sup>60</sup> (RFC 9000 Section 4.6):
     * the value that grants the peer as many streams as the protocol can count, in effect no
     * limit, for {@link ZmuxConfig.Builder#maxIncomingStreams(long)}, {@link ZmuxConfig.Builder#maxIncomingUniStreams}
     * and {@link ZmuxSession#raiseIncomingStreams}. It is a value of their range, not an unlimited
     * setting; their none setting stays 0.
     */
    public static final long MAX_STREAM_CREDIT = io.zmux.internal.wire.VarInt.MAX_STREAM_COUNT;

    /** The record size limit before the peer's parameters arrive, 16382 bytes (SPEC.md Section 3.2). */
    public static final int DEFAULT_MAX_RECORD_SIZE = io.zmux.internal.wire.TransportParams.DEFAULT_MAX_RECORD_SIZE;

    /**
     * Transfers both streams to a bounded, bidirectional relay. Cancelling the returned future, or
     * completing it from outside ({@link ZmuxFuture#orTimeout}), cancels the splice: both streams are reset with ZMUX_CANCELLED (0x5a4d0001) and stopped with code 0
     * and there is no result. A {@link SpliceAbort} in the options ends the splice with a chosen code instead: both
     * streams are reset and stopped with it and the result reports {@link SpliceEnd.Cancelled}. Otherwise the stage completes only after forwarding tasks and ownership cleanup have
     * finished, with how each direction ended. A limiter that fails in {@code acquire} (an exception, a failed stage, or a grant
     * outside its range) ends that direction {@link SpliceEnd.LimiterFailed}: the downstream stream is reset and the upstream stream
     * stopped with ZMUX_CANCELLED, and the other direction goes on. A limiter that fails for a datagram
     * ({@code tryAcquire} throwing or granting outside {@code 0..max}) only drops that datagram, counted as limited; the splice goes on. The ownership cleanup waits
     * for each stream's event loop at most one second; a loop that does not answer by then completes the stage
     * exceptionally with an {@link java.io.IOException} saying so (the cleanup still runs if the loop resumes).
     */
    public static ZmuxFuture<SpliceResult> splice(ZmuxStream a, ZmuxStream b, SpliceOptions options) {
        return Splice.start(a, b, options, false);
    }

    public static ZmuxFuture<SpliceResult> splice(ZmuxStream a, ZmuxStream b) {
        return splice(a, b, SpliceOptions.DEFAULT);
    }

    /**
     * Hands a lane carrier, a further stream that the peer opened on a lane-capable connection, to the process: it
     * reads the lane header, routes the stream by its Lane Key over the sessions of the process, checks the
     * connection and closes a stream that names no session or pipe (MULTIPATH.md 5.8). It is one process-wide entry,
     * not a method of a session, since the stream names its session only through the Lane Key; a listener's
     * {@link io.zmux.adapter.ListenerAdapter#handle} accepts only lanes of its own sessions. A session with a
     * listener also follows that listener's stop when a lane arrives through this process-wide entry. The header is the carrier's first record:
     * it has {@link ListenerConfig#DEFAULT_FIRST_RECORD_TIMEOUT} to deliver it, else the carrier is closed as one
     * naming no pipe. The built-in QUIC adapter calls it itself.
     */
    public static void handleLane(io.zmux.carrier.PipeIo pipe) {
        io.zmux.internal.loop.ListenerHub.laneInput(java.util.Objects.requireNonNull(pipe, "pipe"));
    }

    /** A listener without a socket: carriers arrive through its {@link ZmuxListener#adapter()}. */
    public static ZmuxListener listener(ListenerConfig cfg) {
        return ZmuxListener.unbound(java.util.Objects.requireNonNull(cfg, "cfg"));
    }

    /**
     * {@link #splice(ZmuxStream, ZmuxStream, SpliceOptions)} for two packet streams
     * (EXTENSIONS.md Section 12, {@link io.zmux.helper.PacketStream}): the stream bytes are
     * capsules, including opaque types. With a capsule hook, the relay validates and assembles
     * each capsule before passing it to the chain. Otherwise it streams the value to the next hop.
     * A datagram for a next hop without flow support can be written as a DATAGRAM capsule at
     * a reliable boundary when the send policy allows it. Malformed or truncated capsule input
     * ends the direction with {@link SpliceEnd.PacketError} and propagates ZMUX_PACKET_ERROR.
     */
    public static ZmuxFuture<SpliceResult> splicePackets(ZmuxStream a, ZmuxStream b, SpliceOptions options) {
        return Splice.start(a, b, options, true);
    }

    public static ZmuxFuture<SpliceResult> splicePackets(ZmuxStream a, ZmuxStream b) {
        return splicePackets(a, b, SpliceOptions.DEFAULT);
    }

    /** Starts a byte splice with live controls for the two forwarding directions. */
    public static SpliceController spliceControlled(ZmuxStream a, ZmuxStream b, SpliceOptions options) {
        return Splice.controlled(a, b, options, false);
    }

    /** Starts a packet splice with live controls for the two forwarding directions. */
    public static SpliceController splicePacketsControlled(ZmuxStream a, ZmuxStream b, SpliceOptions options) {
        return Splice.controlled(a, b, options, true);
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

    /** A client session over TCP; the connection starts with the prefix of {@link ZmuxConfig#carrierCredential()}. */
    public static ZmuxSession connect(InetSocketAddress address, ZmuxConfig cfg) throws IOException {
        CoreConfig core = cfg.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR);
        EventLoop loop = cfg.nextLoop();
        SocketChannel ch = dial(address, cfg.handshakeNanos());
        present(ch, cfg.carrierCredential, address);
        return established(Bootstrap.socket(loop, core, ch, cfg.hooks), cfg);
    }

    /** Writes the prefix of credential (null: none) on a connected blocking socket without TLS; closes it on failure. */
    static void present(SocketChannel ch, CarrierCredential credential, InetSocketAddress address) throws IOException {
        Bootstrap.Presenter p = presenter(credential, address);
        if (p == null) return;
        try {
            byte[] b = p.prefix("", "", null, null);
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
    static Bootstrap.Presenter presenter(CarrierCredential credential, InetSocketAddress address) {
        if (credential == null) return null;
        return (alpn, serverName, exporter, tlsVersion) ->
                credential.prefix(new Admission.CarrierInfo(address, address, alpn, serverName, exporter, RequestHead.NONE)
                        .withOuterSecurity(Admission.OuterSecurity.tls(tlsVersion)));
    }

    /** {@link #connect(InetSocketAddress, ZmuxConfig)} on a virtual thread. */
    public static ZmuxFuture<ZmuxSession> connectAsync(InetSocketAddress address, ZmuxConfig cfg) {
        return ZmuxFuture.onThread("zmux-connect", () -> connect(address, cfg));
    }

    /** {@link #connectTls} on a virtual thread; cancelling interrupts it. */
    public static ZmuxFuture<ZmuxSession> connectTlsAsync(InetSocketAddress address, TlsOptions tls, ZmuxConfig cfg) {
        return ZmuxFuture.onThread("zmux-connect-tls", () -> connectTls(address, tls, cfg));
    }

    /**
     * A client session over TLS; ALPN offers {@link ZmuxConfig#alpn()}. Once the handshake
     * completed, the connection starts with the prefix of {@link ZmuxConfig#carrierCredential()},
     * which may bind to the handshake's exporter.
     */
    public static ZmuxSession connectTls(InetSocketAddress address, TlsOptions tls, ZmuxConfig cfg) throws IOException {
        CoreConfig core = cfg.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR);
        SSLEngine engine = tls.clientEngine(address.getPort(), cfg.alpn());
        EventLoop loop = cfg.nextLoop();
        SocketChannel ch = dial(address, cfg.handshakeNanos());
        return established(Bootstrap.tls(loop, core, ch, engine, tls.secure(), cfg.hooks,
                null, presenter(cfg.carrierCredential, address)), cfg);
    }

    /**
     * Dials a TLS connection and runs its handshake: the connection as a pipe input, for a layer such as an
     * Establish, with the handshake's exporter and, per {@link TlsOptions#withSecure}, the Bound declaration.
     * ALPN offers {@link ZmuxConfig#alpn()}.
     */
    public static ZmuxFuture<PipeIo> dialTlsPipeAsync(InetSocketAddress address, TlsOptions tls, ZmuxConfig cfg) {
        CompletableFuture<PipeIo> f = new CompletableFuture<>();
        try {
            var core = cfg.toCore(CoreConfig.Role.INITIATOR, CoreConfig.Role.INITIATOR);
            SSLEngine engine = tls.clientEngine(address.getPort(), cfg.alpn());
            SocketChannel ch = dial(address, cfg.handshakeNanos());
            Bootstrap.tlsPipe(cfg.nextLoop(), core, ch, engine, tls.secure(), f);
        } catch (IOException | RuntimeException e) {
            f.completeExceptionally(e);
        }
        return ZmuxFuture.of(f, () -> f.cancel(false));
    }

    /** {@link #dialTlsPipeAsync} waiting for the pipe input. */
    public static PipeIo dialTlsPipe(InetSocketAddress address, TlsOptions tls, ZmuxConfig cfg) throws IOException {
        try {
            return dialTlsPipeAsync(address, tls, cfg).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("interrupted");
        } catch (java.util.concurrent.ExecutionException e) {
            throw Errors.failure(e.getCause());
        }
    }

    /**
     * A session in the given role over any pipe input. A credential is presented where the
     * pipe was dialed, not here ({@link CarrierCredential}). The pipe input belongs to zmux from the
     * call on, whatever the outcome: zmux closes it on every failure, a configuration error
     * included, and the caller never does.
     *
     * <p>A {@link Role#SERVER} or {@link Role#AUTO} session opened here has no listener, so it
     * cannot accept joins: more than one steady attachment or a
     * {@link ZmuxConfig.Builder#resumeTimeout(Duration)} on it is rejected with
     * {@link IllegalArgumentException} (use {@link io.zmux.adapter.ListenerAdapter#handle} instead).
     *
     * @throws IllegalArgumentException for a configuration that cannot work on this pipe input,
     *     before any byte is written; io is closed
     */
    public static ZmuxSession open(PipeIo io, Role role, ZmuxConfig cfg) throws IOException {
        CoreConfig.Role r;
        try {
            r = core(java.util.Objects.requireNonNull(role, "role"));
            cfg.checkWithoutListener(r);
        } catch (RuntimeException error) {
            Bootstrap.discard(io, error);
            throw error;
        }
        return external(io, cfg, r, r);
    }

    static CoreConfig.Role core(Role role) {
        return switch (role) {
            case CLIENT -> CoreConfig.Role.INITIATOR;
            case SERVER -> CoreConfig.Role.RESPONDER;
            case AUTO -> CoreConfig.Role.AUTO;
        };
    }

    /** Listens for TCP carriers on address with the default configuration. */
    public static ZmuxListener listen(InetSocketAddress address) throws IOException {
        return listen(address, ListenerConfig.defaults());
    }

    /** Listens for TCP carriers, or TLS ones when {@link ListenerConfig#tls()} is set. */
    public static ZmuxListener listen(InetSocketAddress address, ListenerConfig cfg) throws IOException {
        return ZmuxListener.bind(address, cfg);
    }


    private static ZmuxSession external(PipeIo io, ZmuxConfig cfg, CoreConfig.Role role, CoreConfig.Role transport)
            throws IOException {
        return established(startExternal(io, cfg, role, transport), cfg);
    }

    /** Starts a session over io without waiting for the peer's parameters. */
    static CompletableFuture<Bootstrap.Started> startExternal(PipeIo io, ZmuxConfig cfg, CoreConfig.Role role, CoreConfig.Role transport) {
        return startExternal(io, cfg, role, transport, cfg.hooks);
    }

    static CompletableFuture<Bootstrap.Started> startExternal(PipeIo io, ZmuxConfig cfg, CoreConfig.Role role,
                                                             CoreConfig.Role transport, io.zmux.internal.loop.DriverHooks hooks) {
        CompletableFuture<Bootstrap.Started> started;
        try {
            if (SessionHost.onHostThread()) throw new IllegalStateException("connect on a zmux event-loop thread");
            started = Bootstrap.external(() -> io.carrierConn() instanceof SessionRuntime parent
                    ? parent.loop() : cfg.nextLoop(), cfg.toCore(role, transport), io, hooks);
        } catch (RuntimeException error) {
            Bootstrap.discard(io, error);
            throw error;
        }
        return started;
    }

    static ZmuxSession started(CompletableFuture<Bootstrap.Started> f, CarrierCredential credential) throws IOException {
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
        ZmuxSession s = started(f, cfg.carrierCredential);
        try {
            s.awaitEstablished(Long.MAX_VALUE);
        } catch (IOException e) {
            s.close();
            throw e;
        }
        return s;
    }
}
