package io.zmux.seal;

import io.zmux.AllowRecordMode;
import io.zmux.carrier.HandshakeShaper;
import io.zmux.shaping.ShapingLimits;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * The server's side of Establish. Refusals go to {@link io.zmux.ListenerConfig#onReject()}; nothing is ever sent
 * to report one. Built with {@link #builder}, and validated when it is built: each mistake is an
 * {@link IllegalArgumentException}. Immutable.
 */
public final class ServerConfig {
    /** The Profiles a server accepts unless told otherwise: those with a pre-shared key. */
    public static final List<Profile> DEFAULT_PROFILES = List.of(Profile.XXHFS_PSK0_CHACHA, Profile.XXHFS_PSK0_AES);

    private final Gate gate;
    private final StaticKey key;
    private final List<Profile> acceptedProfiles;
    private final AllowRecordMode allowRecordMode;
    private final HandshakeShaper shaper;
    private final ShapingLimits shapingLimits;
    private final Anchor anchor;
    private final Object localContext;
    private final boolean requirePqAuth;
    private final Signer signer;
    private final AppDataSource appData;
    private final int maxAppData; // NO_BOUND: none

    private ServerConfig(Builder b) {
        if (b.gate == null) throw new IllegalArgumentException("no Gate");
        if (b.key == null) throw new IllegalArgumentException("no static key");
        if (b.acceptedProfiles.isEmpty()) {
            acceptedProfiles = DEFAULT_PROFILES;
        } else {
            for (Profile p : b.acceptedProfiles) if (p == null) throw new IllegalArgumentException("an unknown or reserved Profile among the accepted ones");
            acceptedProfiles = List.copyOf(new java.util.LinkedHashSet<>(b.acceptedProfiles));
        }
        if (b.requirePqAuth && b.anchor == null) throw new IllegalArgumentException("requirePqAuth without an anchor");
        try {
            PinAnchor.checkPqAuth(b.anchor, b.requirePqAuth);
        } catch (SecureCarrierException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        gate = b.gate;
        key = b.key;
        allowRecordMode = b.allowRecordMode;
        shaper = b.shaper;
        shapingLimits = b.shapingLimits;
        anchor = b.anchor;
        localContext = b.localContext;
        requirePqAuth = b.requirePqAuth;
        signer = b.signer;
        appData = b.appData;
        maxAppData = b.maxAppData;
    }

    /** A builder of a server with the node's static key and its {@link Gate}, both required. */
    public static Builder builder(Gate gate, StaticKey key) {
        return new Builder(gate, key);
    }

    /** A builder holding this configuration. */
    public Builder toBuilder() {
        Builder b = new Builder(gate, key);
        b.acceptedProfiles = acceptedProfiles == DEFAULT_PROFILES ? List.of() : acceptedProfiles;
        b.allowRecordMode = allowRecordMode;
        b.shaper = shaper;
        b.shapingLimits = shapingLimits;
        b.anchor = anchor;
        b.localContext = localContext;
        b.requirePqAuth = requirePqAuth;
        b.signer = signer;
        b.appData = appData;
        b.maxAppData = maxAppData;
        return b;
    }

    /** The host's decision on clients. */
    public Gate gate() { return gate; }

    /** The node's static key. */
    public StaticKey key() { return key; }

    /** The Profiles a client may use, without duplicates. */
    public List<Profile> acceptedProfiles() { return acceptedProfiles; }

    /** The check of a Hello's mode; null: only Sealed. */
    public AllowRecordMode allowRecordMode() { return allowRecordMode; }

    /** Shapes the handshake; null: none. */
    public HandshakeShaper shaper() { return shaper; }

    /** The limits on the shaper's plans. */
    public ShapingLimits shapingLimits() { return shapingLimits; }

    /** Judges the client after message 3; null: clients are not judged. */
    public Anchor anchor() { return anchor; }

    /** Handed unread to the anchor ({@link VerifyInput#localContext()}); may be null. */
    public Object localContext() { return localContext; }

    /** Whether a client that is not PQ authenticated fails. */
    public boolean isRequirePqAuth() { return requirePqAuth; }

    /** Signs the transcript in message 2; null: no proof. */
    public Signer signer() { return signer; }

    /** Supplies the application data of message 2; null: none. */
    public AppDataSource appData() { return appData; }

    /** The bound on the application data of a received message, in bytes; empty: no bound beyond the message. */
    public OptionalInt maxAppData() { return maxAppData == ClientConfig.NO_BOUND ? OptionalInt.empty() : OptionalInt.of(maxAppData); }

    /** Builds a {@link ServerConfig}: the default Profiles, Sealed only, unshaped, clients not judged. */
    public static final class Builder {
        private final Gate gate;
        private final StaticKey key;
        private List<Profile> acceptedProfiles = List.of();
        private AllowRecordMode allowRecordMode;
        private HandshakeShaper shaper;
        private ShapingLimits shapingLimits = ShapingLimits.defaults();
        private Anchor anchor;
        private Object localContext;
        private boolean requirePqAuth;
        private Signer signer;
        private AppDataSource appData;
        private int maxAppData = ClientConfig.DEFAULT_MAX_APP_DATA;

        private Builder(Gate gate, StaticKey key) {
            this.gate = gate;
            this.key = key;
        }

        /**
         * The Profiles a client may use; none is the default, {@link #DEFAULT_PROFILES}. Listing a Profile without a
         * pre-shared key (4, 5) is the only way to accept it: it sends the node's credentials to anonymous clients. A
         * null element, which is what {@link Profile#of} gives for an unknown or reserved value, is an error. Join is
         * always accepted on the carriers of the server's sessions.
         */
        public Builder acceptedProfiles(Profile... profiles) {
            acceptedProfiles = profiles == null ? List.of() : java.util.Arrays.asList(profiles.clone());
            return this;
        }

        /**
         * The check of a Hello's mode, with the Establish's {@link Peer} as its peer; null: only Sealed. Joins have
         * their own check, {@link io.zmux.ZmuxConfig.Builder#allowRecordMode}.
         */
        public Builder allowRecordMode(AllowRecordMode a) { allowRecordMode = a; return this; }

        /** Shapes the handshake; null: none. */
        public Builder shaper(HandshakeShaper s) { shaper = s; return this; }

        /** The limits on the shaper's plans; the defaults unless set. */
        public Builder shapingLimits(ShapingLimits l) { shapingLimits = Objects.requireNonNull(l, "shapingLimits"); return this; }

        /** Judges the client after message 3, with its credential or none; null: clients are not judged. */
        public Builder anchor(Anchor a) { anchor = a; return this; }

        /** Handed unread to the anchor ({@link VerifyInput#localContext()}); may be null. */
        public Builder localContext(Object c) { localContext = c; return this; }

        /** Fails a client that is not PQ authenticated; needs an anchor. */
        public Builder requirePqAuth(boolean on) { requirePqAuth = on; return this; }

        /**
         * Signs the transcript in message 2, for clients whose verdict names a proof key; in Profiles 4 and 5 anyone
         * can ask for it, so it limits its own rate. Null: no proof.
         */
        public Builder signer(Signer s) { signer = s; return this; }

        /** Supplies the application data of message 2 ({@link AppDataSource}); null: none. */
        public Builder appData(AppDataSource s) { appData = s; return this; }

        /**
         * Bounds the application data of a received message, in bytes; 256 unless set. More fails the handshake as
         * {@code too_large}.
         *
         * @throws IllegalArgumentException when negative
         */
        public Builder maxAppData(int bytes) { maxAppData = ClientConfig.checkMaxAppData(bytes); return this; }

        /** No bound on received application data beyond the message. */
        public Builder noMaxAppData() { maxAppData = ClientConfig.NO_BOUND; return this; }

        /**
         * The configuration.
         *
         * @throws IllegalArgumentException as the class says
         */
        public ServerConfig build() { return new ServerConfig(this); }
    }
}
