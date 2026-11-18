package io.zmux.seal;

import static io.zmux.seal.SecureCarrierException.Kind.INVALID_CONFIG;

import io.zmux.InboundTransform;
import io.zmux.RecordPolicyInput;
import io.zmux.ZmuxFuture;
import io.zmux.RecordMode;
import io.zmux.CarrierRejectedException;
import io.zmux.carrier.Framing;
import io.zmux.carrier.HandshakeShaper;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.join.RecordProtection;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.core.CarrierAdmission;
import io.zmux.internal.core.HandshakePlans;
import io.zmux.internal.join.Dispatch;
import io.zmux.internal.join.Kdf;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.Tlv;
import io.zmux.internal.loop.PipeIoAccess;
import io.zmux.internal.loop.Bootstrap;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Runs Establish on carriers: the client's as a stage, the server's as a listener's inbound transform. */
public final class Seal {
    /** Enough for a first record's classification; the listener itself bounds what follows. */
    private static final long MAX_RECORD = 1 << 20;

    private Seal() {}

    /** The key of clients without their own, generated once per process; a failed generation is tried again later. */
    private static volatile StaticKey processKey;
    private static final Object PROCESS_KEY_LOCK = new Object();

    private static StaticKey processKey() throws SecureCarrierException {
        StaticKey k = processKey;
        if (k != null) return k;
        synchronized (PROCESS_KEY_LOCK) {
            if (processKey == null) processKey = StaticKey.generate();
            return processKey;
        }
    }

    /**
     * Runs the client's handshake on carrier and completes with the pipe input for a session. On a failure,
     * and when the stage is cancelled, the carrier is closed; nothing is retried, a different choice being a
     * new carrier. The stage fails with a {@link SecureCarrierException} of the protocol's class, a {@link
     * CredentialsFailedException} when the {@link Anchor} passes no credential of the server, a {@link
     * NoReplyException} when the carrier ends or the timeout passes before the server's reply, a
     * {@link java.net.SocketTimeoutException} when it passes later, or with the carrier's own failure.
     * Cancelling the future, or completing it from outside ({@link ZmuxFuture#orTimeout}), stops the
     * handshake and closes the carrier.
     * A server that stalls a refused carrier leaves the client waiting until this timeout, so without a
     * handshake timeout the client waits until the server ends the stall.
     */
    public static ZmuxFuture<PipeIo> establish(PipeIo carrier, ClientConfig cfg) {
        Objects.requireNonNull(carrier, "carrier");
        Objects.requireNonNull(cfg, "cfg");
        CompletableFuture<PipeIo> f = new CompletableFuture<>();
        java.util.concurrent.atomic.AtomicBoolean replied = new java.util.concurrent.atomic.AtomicBoolean();
        Thread t = Thread.ofVirtual().name("zmux-seal-establish").unstarted(() -> {
            try {
                PipeIo r = client(carrier, cfg, replied);
                // A handshake that lost the race to the timeout or a cancel still owns the carrier: closing r's
                // reader also closes what the handshake holds of it and gives its receive credit back.
                if (!f.complete(r)) Bootstrap.discard(r, new java.util.concurrent.CancellationException());
            } catch (Throwable e) {
                Bootstrap.discard(carrier, e);
                f.completeExceptionally(e);
            }
        });
        cfg.handshakeTimeout().ifPresent(limit -> {
            // The timer goes when the handshake ends first (completing it cancels the timer).
            CompletableFuture<Boolean> timer = new CompletableFuture<Boolean>().completeOnTimeout(true, limit.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
            timer.thenAccept(expired -> {
                if (!expired) return;
                var timeout = new java.net.SocketTimeoutException("handshake timeout " + limit);
                f.completeExceptionally(replied.get() ? timeout : new NoReplyException(timeout));
            });
            f.whenComplete((r, e) -> timer.complete(false));
        });
        f.whenComplete((r, e) -> {
            if (e != null) t.interrupt(); // The handshake wakes and closes the carrier.
        });
        t.start();
        return io.zmux.internal.spi.Spi.future(f, () -> f.completeExceptionally(new java.util.concurrent.CancellationException("establish cancelled")));
    }

    /**
     * The server's side of Establish as a listener's {@link InboundTransform}. A carrier whose first byte is
     * 02 is a Join and a lane is its session's: both pass on untouched. Anything else that is no Establish, and
     * every failed authentication, is a {@link CarrierRejectedException} and goes to {@link io.zmux.ListenerConfig#onReject()}.
     * Its reason is local, finer than the protocol's classes: {@code profile_disabled}
     * (not among the accepted Profiles), {@code unknown_handle}, {@code auth},
     * {@code binding_unavailable} (Bound on a carrier without a Binding), {@code record_mode} (a mode not
     * allowed), {@code gate_error} (a {@link Gate} call failed, its exception the cause), or another class of
     * the protocol, whose failure is the cause. The response and the way of refusing are the same for every
     * reason.
     */
    public static InboundTransform server(ServerConfig cfg) {
        Objects.requireNonNull(cfg, "cfg");
        return (carrier, ctx) -> {
            CompletableFuture<PipeIo> f = new CompletableFuture<>();
            Thread t = Thread.ofVirtual().name("zmux-seal-server").start(() -> {
                try {
                    f.complete(server(carrier, cfg, ctx.hint().source()));
                } catch (Throwable e) {
                    f.completeExceptionally(e); // The listener closes it, or sends it to OnReject.
                }
            });
            f.whenComplete((r, e) -> {
                if (f.isCancelled()) t.interrupt();
            });
            return f;
        };
    }

    /** The peer of a pipe input {@link #establish} returned; empty for any other. */
    public static Optional<Peer> peerOf(PipeIo io) {
        return PipeIoAccess.get().carrierPeer(Objects.requireNonNull(io, "io")) instanceof Peer p ? Optional.of(p) : Optional.empty();
    }

    private static io.zmux.internal.join.Framing framing(PipeIo p) {
        return p.framing() == Framing.MESSAGES ? io.zmux.internal.join.Framing.MESSAGE : io.zmux.internal.join.Framing.STREAM;
    }

    /** The carrier's Binding, from its exporter, null when it has none that holds. */
    private static byte[] binding(PipeIo p) throws SecureCarrierException {
        var exporter = p.capabilities().exporter();
        if (!p.isCarrierSecure() || exporter == null) return null;
        byte[] v = exporter.export(Kdf.EXPORTER_SEAL_BOUND, null, Kdf.BINDING_LEN);
        if (v == null) return null;
        // A split carrier's exporter gives the uplink's value followed by the downlink's when they differ.
        if (v.length == 2 * Kdf.BINDING_LEN) {
            return Kdf.binding(java.util.Arrays.copyOf(v, Kdf.BINDING_LEN), java.util.Arrays.copyOfRange(v, Kdf.BINDING_LEN, v.length));
        }
        return Kdf.binding(v, v);
    }

    /**
     * The plan of a message under the limits, after its wait: the padding field that takes the message from its length to the
     * planned one, wire and noise being what it is without padding.
     */
    private static int shape(HandshakeShaper s, io.zmux.shaping.ShapingLimits limits, PipeIo p, String message, int wire, int noise, long[] padded) throws InterruptedIOException {
        if (s == null) return 0;
        // Each handshake family has its own name: a plan made for the lengths of one is never applied to another.
        HandshakeShaper.UnitPlan plan = HandshakePlans.ask(s, new HandshakeShaper.View(p.carrier(), Profile.HANDSHAKE_ESTABLISH, message), () -> { });
        if (plan == null) return 0;
        plan = HandshakePlans.limited(plan, limits);
        if (plan.delay() != null && plan.delay().isPositive()) {
            try {
                Thread.sleep(plan.delay());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
        }
        int item = HandshakePlans.item(plan, wire + p.capabilities().writeOverhead(), Math.min(io.zmux.internal.join.Sealer.MAX_MESSAGE - noise, io.zmux.internal.join.Sealer.MESSAGE_FLOOR - wire));
        if (item != HandshakePlans.NO_ITEM) padded[0] += HandshakePlans.itemSize(item);
        return Tlv.paddingField(item);
    }

    /** What a framed unit of n Noise bytes takes on the carrier. */
    private static int unit(PipeIo p, int n) { return io.zmux.internal.join.Frames.unitLength(framing(p), n); }

    private static PipeIo client(PipeIo p, ClientConfig c, java.util.concurrent.atomic.AtomicBoolean replied) throws IOException {
        if (p.reader() == null || p.writer() == null) throw new SecureCarrierException(INVALID_CONFIG, "an Establish needs both halves");
        if (c.anchor() == null && !c.isAllowPskOnly()) throw new SecureCarrierException(INVALID_CONFIG, "no Anchor");
        if (c.isRequirePqAuth() && c.anchor() == null) throw new SecureCarrierException(INVALID_CONFIG, "requirePqAuth without an Anchor");
        PinAnchor.checkPqAuth(c.anchor(), c.isRequirePqAuth());
        Profile profile = Profiles.client(c);
        if (c.isAllowPskOnly() && !profile.isPskBased()) throw new SecureCarrierException(INVALID_CONFIG, "allowPskOnly without a pre-shared key");
        //noinspection resource: borrowed: the config or the process owns the key
        StaticKey key = c.staticKey() != null ? c.staticKey() : processKey();
        Mode mode = Mode.of(c.mode());
        byte[] binding = null;
        if (mode == Mode.BOUND && (binding = binding(p)) == null) {
            throw new SecureCarrierException(INVALID_CONFIG, "Bound on a carrier without a Secure declaration or an exporter");
        }
        var hs = EstablishClient.create(profile, mode, framing(p), c.handle(), c.psk(), binding, key).maxAppData(ClientConfig.appDataLimit(c.maxAppDataBound()));
        // Message 1 carries application data only under a pre-shared key.
        EstablishPayload first = new EstablishPayload(null, profile.isPskBased() ? appData(c.appData(), 1, new AppDataSource.Known(profile, null, null)) : null, null, 0);
        int noise = Handshake.minLength(profile, 0) + first.length();
        long[] padded = new long[1];
        int pad = shape(c.shaper(), c.shapingLimits(), p, "hello", EstablishHello.headerLength(profile) + unit(p, noise), noise, padded);
        byte[] hello = hs.writeHello(first.withPadding(pad, false));
        HandshakeIo io = new HandshakeIo(p);
        try {
            io.write(hello);
            EstablishClient.Reply reply;
            try {
                reply = readUnit(io, b -> hs.readReply(b, 0, b.length));
            } catch (SecureCarrierException e) {
                throw e; // the reply came and failed: no NoReply
            } catch (IOException e) {
                if (e.getClass() == InterruptedIOException.class) throw e; // cancelled, or the timeout, which reports itself
                throw new NoReplyException(e);
            }
            replied.set(true);
            VerifyVerdict verdict = null;
            boolean pqAuth = false;
            // The server is judged before anything of this client is written.
            if (c.anchor() != null) {
                Verification.Judged j;
                try {
                    j = hs.verify(c.anchor(), c.localContext(), c.isRequirePqAuth());
                } catch (VerifyUnavailableException e) {
                    // The client judged the server and could not decide: its establishment fails as credentials failed.
                    throw new CredentialsFailedException("the verifier is unavailable", e);
                }
                verdict = j.verdict();
                pqAuth = j.pqAuthenticated();
            } else {
                hs.trustPskOnly(c.localContext());
            }
            // A proof's placeholder gives the message its length before the signature exists: shaping plans the whole plaintext.
            EstablishPayload last = new EstablishPayload(c.credential(), appData(c.appData(), 3, hs.known()), null, 0)
                    .withProof(c.signer() == null ? null : Proof.placeholder(c.signer()));
            noise = Handshake.minLength(profile, 2) + last.length();
            pad = shape(c.shaper(), c.shapingLimits(), p, "finish", unit(p, noise), noise, padded);
            io.write(hs.writeFinish(last.withPadding(pad, false), c.signer()));
            Established done = hs.finish();
            Peer peer = new Peer(reply.peerStatic(), reply.credential(), verdict, null, profile, c.mode(), hs.peerAppData(), profile.pqConfidential(), pqAuth);
            return padded(wrap(io, done, peer), padded[0]);
        } catch (Throwable e) {
            io.fail(); // what the handshake held of the carrier goes with it
            throw e;
        }
    }

    /**
     * The application data of message msg from src, null for none; peer is what is known of the peer then. A failure, or
     * an exception, ends the handshake; an interrupt (the timeout, a cancel) ends the wait.
     */
    private static byte[] appData(AppDataSource src, int msg, AppDataSource.Known peer) throws IOException {
        if (src == null) return null;
        try {
            Optional<byte[]> r = src.data(msg, peer).toCompletableFuture().get();
            return r.orElse(null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("application data of message " + msg);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new io.zmux.CallbackFailedException("application data of message " + msg, e.getCause());
        } catch (RuntimeException e) {
            throw new io.zmux.CallbackFailedException("application data of message " + msg, e);
        }
    }

    private interface UnitReader<T> {
        T read(byte[] b) throws SecureCarrierException;
    }

    /** Reads until reader takes a whole unit, and returns its result with the unit consumed. */
    private static <T> T readUnit(HandshakeIo io, UnitReader<T> reader) throws IOException {
        if (io.buf.length == 0 || io.messages()) io.fill();
        while (true) {
            try {
                T r = reader.read(io.buf);
                int used = switch (r) {
                    case EstablishClient.Reply x -> x.consumed();
                    case EstablishServer.Finish x -> x.consumed();
                    default -> throw new IllegalStateException();
                };
                io.consume(used);
                return r;
            } catch (SecureCarrierException e) {
                if (e.kind() != SecureCarrierException.Kind.INCOMPLETE || io.messages()) throw e;
                io.fill();
            }
        }
    }

    /** A handshake on a layered carrier adds to what the layers below it padded. */
    private static PipeIo padded(PipeIo p, long n) { return p.withHandshakePadding(p.capabilities().handshakePadding() + n); }

    /** The carrier of a finished handshake, with the carrier first bytes before any later byte. */
    private static PipeIo wrap(HandshakeIo io, Established done, Peer peer) {
        PipeIo.Exporter exporter = (label, context, length) -> {
            try {
                return done.export(label, context, length);
            } catch (SecureCarrierException invalid) {
                return null;
            }
        };
        if (peer.mode() != RecordMode.SEALED) {
            return PipeIoAccess.get().withEstablished(io.rest(done.firstBytes()), peer.mode(), exporter, null, peer);
        }
        byte[] first = done.firstBytes();
        return PipeIoAccess.get().withEstablished(io.rest(null), peer.mode(), exporter, new RecordProtection() {
            @Override public int seal(ByteBuffer plain, ByteBuffer out) throws SecureCarrierException { return done.sealer().seal(plain, out); }
            @Override public int open(ByteBuffer ct, ByteBuffer out) throws SecureCarrierException { return done.opener().open(ct, out); }
            @Override public byte[] firstBytes() { return first; }
        }, peer);
    }

    private static String reason(SecureCarrierException e) { return e.kind().name().toLowerCase(Locale.ROOT); }

    private static PipeIo server(PipeIo p, ServerConfig c, SocketAddress src) throws Exception {
        if (p.reader() == null || p.writer() == null) throw new SecureCarrierException(INVALID_CONFIG, "an Establish needs both halves");
        HandshakeIo io = new HandshakeIo(p);
        try {
            var f = framing(p);
            // What the first bytes begin.
            loop:
            while (true) {
                if (io.buf.length == 0 || io.messages()) io.fill();
                Dispatch.Result d = Dispatch.classify(io.buf, 0, io.buf.length, f, MAX_RECORD);
                switch (d.kind()) {
                    case INCOMPLETE -> io.fill();
                    case JOIN -> { return io.rest(null); }
                    case ESTABLISH -> { break loop; }
                    case RECORD -> {
                        // The one record that passes here is a lane's first.
                        ByteBuffer b = ByteBuffer.allocate(io.buf.length + 8);
                        if (io.messages()) VarInt.put(b, io.buf.length);
                        b.put(io.buf).flip();
                        switch (CarrierAdmission.classify(b, 0, b.limit(), MAX_RECORD).kind()) {
                            case MORE -> io.fill();
                            case LANE -> { return io.rest(null); }
                            default -> throw new CarrierRejectedException(firstByte(io), null, "unknown_profile");
                        }
                    }
                    default -> throw new CarrierRejectedException(firstByte(io), null, "unknown_profile");
                }
            }
            EstablishHello h;
            while (true) {
                try {
                    h = EstablishHello.parseExact(io.buf, 0, io.buf.length, f);
                    break;
                } catch (SecureCarrierException e) {
                    if (e.kind() != SecureCarrierException.Kind.INCOMPLETE) throw new CarrierRejectedException(firstByte(io), null, reason(e), e);
                    if (io.messages()) throw new CarrierRejectedException(firstByte(io), null, "malformed");
                    io.fill();
                }
            }
            int wireProfile = (int) h.profile().value();
            RecordMode mode = Mode.record(h.mode());
            Refuser refuse = (why, cause) -> new CarrierRejectedException(wireProfile, mode, why, cause);
            boolean withPsk = h.profile().isPskBased();
            if (!c.acceptedProfiles().contains(h.profile())) throw refuse.of("profile_disabled", null);
            if (mode != RecordMode.SEALED && c.allowRecordMode() == null) throw refuse.of("record_mode", null);
            byte[] psk = null;
            if (withPsk) {
                try {
                    psk = c.gate().resolve(h.handle(), src);
                } catch (Exception failed) {
                    throw refuse.of("gate_error", failed);
                }
                if (psk == null) throw refuse.of("unknown_handle", null);
            }
            // A carrier without the Bound contract has no Binding to check it with, whatever the client mixed.
            byte[] binding = h.mode() == Mode.BOUND ? binding(p) : null;
            if (h.mode() == Mode.BOUND && binding == null) throw refuse.of("binding_unavailable", null);
            io.consume(io.buf.length);
            EstablishServer hs;
            try {
                hs = EstablishServer.create(h, f, psk, binding, c.staticKey()).maxAppData(ClientConfig.appDataLimit(c.maxAppDataBound()));
                hs.readHello();
            } catch (SecureCarrierException e) {
                throw refuse.of(reason(e), e);
            }
            Credential cred;
            try {
                cred = c.gate().credential(h.handle(), h.profile(), src);
            } catch (Exception failed) {
                throw refuse.of("gate_error", failed);
            }
            byte[] data;
            try {
                data = appData(c.appData(), 2, hs.known(c.localContext()));
            } catch (InterruptedIOException e) {
                throw e;
            } catch (IOException failed) {
                throw refuse.of("gate_error", failed);
            }
            EstablishPayload second = new EstablishPayload(cred, data, null, 0).withProof(c.signer() == null ? null : Proof.placeholder(c.signer()));
            int noise = Handshake.minLength(h.profile(), 1) + second.length();
            long[] padded = new long[1];
            int pad = shape(c.shaper(), c.shapingLimits(), p, "reply", unit(p, noise), noise, padded);
            // The client's e or ek is checked here: a low-order e or an ek coefficient out of range is refused.
            byte[] reply;
            try {
                reply = hs.writeReply(second.withPadding(pad, false), c.signer());
            } catch (SecureCarrierException e) {
                throw refuse.of(reason(e), e);
            }
            io.write(reply);
            EstablishServer.Finish finish;
            try {
                finish = readUnit(io, b -> hs.readFinish(b, 0, b.length));
            } catch (SecureCarrierException e) {
                throw refuse.of(reason(e), e);
            }
            VerifyVerdict verdict = null;
            boolean pqAuth = false;
            if (c.anchor() != null) {
                Verification.Judged j;
                try {
                    j = hs.verify(c.anchor(), finish, c.localContext(), c.isRequirePqAuth());
                } catch (CredentialsFailedException e) {
                    throw refuse.of("credentials_failed", e);
                } catch (VerifyUnavailableException e) {
                    throw new IOException(e.getMessage(), e); // closes the carrier without counting a refusal
                }
                verdict = j.verdict();
                pqAuth = j.pqAuthenticated();
            }
            List<AppData> clientData = c.anchor() != null ? AppData.verified(finish.appData()) : finish.appData();
            Peer early = new Peer(finish.peerStatic(), finish.credential(), verdict, null, h.profile(), mode, clientData, h.profile().pqConfidential(), pqAuth);
            if (c.allowRecordMode() != null) {
                boolean exporter = p.capabilities().exporter() != null;
                if (!c.allowRecordMode().allow(new RecordPolicyInput(exporter, p.isCarrierSecure(), java.util.Optional.ofNullable(p.label()), true), mode, early)) throw refuse.of("record_mode", null);
            }
            Object value;
            try {
                value = c.gate().commit(new CommitRequest(h.handle(), early), src);
            } catch (Exception failed) {
                throw refuse.of("gate_error", failed);
            }
            Established done;
            try {
                done = hs.finish();
            } catch (SecureCarrierException e) {
                throw refuse.of(reason(e), e);
            }
            return padded(wrap(io, done, early.withValue(value)), padded[0]);
        } catch (Throwable e) {
            io.fail(); // what the handshake held of the carrier goes with it
            throw e;
        }
    }

    private interface Refuser {
        CarrierRejectedException of(String why, Throwable cause);
    }

    private static int firstByte(HandshakeIo io) { return io.buf.length == 0 ? 0 : io.buf[0] & 0xff; }

}
