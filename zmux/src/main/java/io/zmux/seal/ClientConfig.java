package io.zmux.seal;

import io.zmux.RecordMode;
import io.zmux.carrier.HandshakeShaper;
import io.zmux.shaping.ProfileLimits;
import io.zmux.shaping.ShapingBudget;
import io.zmux.shaping.ShapingLimits;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/** The client's side of Establish, built with {@link #builder()}. Immutable. */
public final class ClientConfig {
    /** The default bound on the application data of a received message, in bytes. */
    static final int DEFAULT_MAX_APP_DATA = 256;
    /** No bound on received application data beyond the message. */
    static final int NO_BOUND = -1;
    private static final Duration DEFAULT_HANDSHAKE_TIMEOUT = Duration.ofSeconds(10);

    private final Anchor anchor;
    private final boolean allowPskOnly;
    private final byte[] handle, psk;
    private final StaticKey key;
    private final List<Profile> allowedProfiles;
    private final RecordMode mode;
    private final Credential credential;
    private final Object localContext;
    private final boolean requirePqAuth;
    private final Signer signer;
    private final AppDataSource appData;
    private final int maxAppData;
    private final HandshakeShaper shaper;
    private final Duration handshakeTimeout; // null: none
    private final ShapingLimits shapingLimits;
    private final ProfileLimits shapingProfile;

    private ClientConfig(Builder b) {
        anchor = b.anchor;
        allowPskOnly = b.allowPskOnly;
        handle = b.handle;
        psk = b.psk;
        key = b.key;
        if (b.allowedProfiles != null) {
            for (Profile p : b.allowedProfiles) if (p == null) throw new IllegalArgumentException("an unknown or reserved Profile among the allowed ones");
        }
        if (b.mode == RecordMode.UNMANAGED) throw new IllegalArgumentException("a handshake has no Unmanaged mode");
        allowedProfiles = b.allowedProfiles == null ? null : List.copyOf(b.allowedProfiles);
        mode = b.mode;
        credential = b.credential;
        localContext = b.localContext;
        requirePqAuth = b.requirePqAuth;
        signer = b.signer;
        appData = b.appData;
        maxAppData = b.maxAppData;
        shaper = b.shaper;
        handshakeTimeout = b.handshakeTimeout;
        shapingLimits = b.shapingLimits;
        shapingProfile = b.shapingProfile;
    }

    public static Builder builder() { return new Builder(); }

    /** Judges the server; null only with {@link #isAllowPskOnly()}. */
    public Anchor anchor() { return anchor; }

    /** Whether any server that knows the pre-shared key is accepted. */
    public boolean isAllowPskOnly() { return allowPskOnly; }

    /** The Handle of the pre-shared key; null without one. */
    public byte[] handle() { return handle == null ? null : handle.clone(); }

    /** The pre-shared key; null without one. */
    public byte[] psk() { return psk == null ? null : psk.clone(); }

    /** Whether a pre-shared key is set. */
    boolean isPskBased() { return psk != null && psk.length > 0; }

    /** Whether a Handle is set. */
    boolean hasHandle() { return handle != null; }

    /** The client's static key; null is a key generated once per process. */
    public StaticKey staticKey() { return key; }

    /** The Profiles the client may use, in order; null: the default list. */
    public List<Profile> allowedProfiles() { return allowedProfiles; }

    /** The carrier's record mode. */
    public RecordMode mode() { return mode; }

    /** The credential of message 3; null: none. */
    public Credential credential() { return credential; }

    /** Handed unread to the anchor ({@link VerifyInput#localContext()}); may be null. */
    public Object localContext() { return localContext; }

    /** Whether a server that is not PQ authenticated fails. */
    public boolean isRequirePqAuth() { return requirePqAuth; }

    /** Signs the transcript in message 3; null: no proof. */
    public Signer signer() { return signer; }

    /** Supplies the application data of messages 1 and 3; null: none. */
    public AppDataSource appData() { return appData; }

    /** The bound on the application data of a received message, in bytes; empty: no bound beyond the message. */
    public OptionalInt maxAppData() { return maxAppData == NO_BOUND ? OptionalInt.empty() : OptionalInt.of(maxAppData); }

    /** Shapes the handshake; null: none. */
    public HandshakeShaper shaper() { return shaper; }

    /** The bound on the handshake; empty: no timeout. */
    public Optional<Duration> handshakeTimeout() { return Optional.ofNullable(handshakeTimeout); }

    /** The limits on the shaper's plans. */
    public ShapingLimits shapingLimits() { return shapingLimits; }

    /** The selected Establish profile. Defaults to Lightweight. */
    public ProfileLimits shapingProfile() { return shapingProfile; }

    ShapingLimits.Effective establishLimits() {
        return shapingLimits.under(new ShapingBudget(null, shapingProfile, false));
    }

    /** The stored bound on the application data of a received message: bytes, or {@link #NO_BOUND}. */
    int maxAppDataBound() { return maxAppData; }

    /** The internal form of a stored bound on received application data: bytes, or {@link Long#MAX_VALUE} for none. */
    static long appDataLimit(int bound) { return bound == NO_BOUND ? Long.MAX_VALUE : bound; }

    static int checkMaxAppData(int bytes) {
        if (bytes < 0) throw new IllegalArgumentException("negative maxAppData; noMaxAppData() sets no bound");
        return bytes;
    }

    /** Builds a {@link ClientConfig}: Sealed, no credential, and the defaults each method gives. */
    public static final class Builder {
        private Anchor anchor;
        private boolean allowPskOnly;
        private byte[] handle, psk;
        private StaticKey key;
        private List<Profile> allowedProfiles;
        private RecordMode mode = RecordMode.SEALED;
        private Credential credential;
        private Object localContext;
        private boolean requirePqAuth;
        private Signer signer;
        private AppDataSource appData;
        private int maxAppData = DEFAULT_MAX_APP_DATA;
        private HandshakeShaper shaper;
        private Duration handshakeTimeout = DEFAULT_HANDSHAKE_TIMEOUT;
        private ShapingLimits shapingLimits = ShapingLimits.defaults();
        private ProfileLimits shapingProfile = ProfileLimits.LIGHTWEIGHT;

        private Builder() {}

        /** Judges the server; required unless {@link #allowPskOnly}. */
        public Builder anchor(Anchor a) { anchor = a; return this; }

        /**
         * Accepts any server that knows the pre-shared key: an explicit bootstrap, which must not override trust the
         * host already holds.
         */
        public Builder allowPskOnly(boolean on) { allowPskOnly = on; return this; }

        /** Selects a Profile with a pre-shared key, under this Handle; without it, one without a pre-shared key. */
        public Builder psk(byte[] handle, byte[] psk) {
            this.handle = handle == null ? null : handle.clone();
            this.psk = psk == null ? null : psk.clone();
            return this;
        }

        /** The client's static key; null (the default) is a key generated once per process. */
        public Builder staticKey(StaticKey k) { key = k; return this; }

        /**
         * The Profiles it may use, the first that fits the key being used; none set: the Profiles of both ciphers, the
         * one with hardware support first. A refusal never makes a client choose another Profile: that is the host's
         * explicit choice.
         */
        public Builder allowedProfiles(Profile... p) { allowedProfiles = java.util.Arrays.asList(p.clone()); return this; }

        /** The carrier's record mode; Sealed unless set. */
        public Builder mode(RecordMode m) { mode = Objects.requireNonNull(m, "mode"); return this; }

        /** Goes in message 3; null: none. */
        public Builder credential(Credential c) { credential = c; return this; }

        /** Handed unread to the anchor ({@link VerifyInput#localContext()}). */
        public Builder localContext(Object c) { localContext = c; return this; }

        /** Fails a server that is not PQ authenticated: no verdict named a PQ proof key whose proof verified. */
        public Builder requirePqAuth(boolean on) { requirePqAuth = on; return this; }

        /** Signs the transcript in message 3, for a server whose verdict names a proof key; null: no proof. */
        public Builder signer(Signer s) { signer = s; return this; }

        /**
         * Supplies the application data of message 1, in Profiles 1 and 3 only, and of message 3, after the server is
         * judged; null: none.
         */
        public Builder appData(AppDataSource s) { appData = s; return this; }

        /**
         * Bounds the application data of a received message, in bytes; 256 unless set. More fails the handshake as
         * {@code too_large}.
         *
         * @throws IllegalArgumentException when negative
         */
        public Builder maxAppData(int bytes) { maxAppData = checkMaxAppData(bytes); return this; }

        /** No bound on received application data beyond the message. */
        public Builder noMaxAppData() { maxAppData = NO_BOUND; return this; }

        /** Shapes the handshake; null: none. */
        public Builder shaper(HandshakeShaper s) { shaper = s; return this; }

        /**
         * Bounds the handshake; 10 s unless set. A server that stalls a refused carrier leaves the client waiting until
         * it passes ({@link NoReplyException}), so keep it bounded.
         *
         * @throws IllegalArgumentException unless positive
         */
        public Builder handshakeTimeout(Duration t) {
            if (!Objects.requireNonNull(t, "handshakeTimeout").isPositive()) {
                throw new IllegalArgumentException("handshakeTimeout must be positive; noHandshakeTimeout() sets none");
            }
            handshakeTimeout = t;
            return this;
        }

        /** No bound on the handshake: a stalled client waits until the server ends the stall. */
        public Builder noHandshakeTimeout() { handshakeTimeout = null; return this; }

        /** The limits on the shaper's plans; the defaults unless set. */
        public Builder shapingLimits(ShapingLimits l) { shapingLimits = Objects.requireNonNull(l, "shapingLimits"); return this; }

        /** Supplies the session's selected profile before Establish writes its first message. */
        public Builder shapingProfile(ProfileLimits profile) {
            shapingProfile = Objects.requireNonNull(profile, "shapingProfile");
            return this;
        }

        public ClientConfig build() { return new ClientConfig(this); }
    }
}
