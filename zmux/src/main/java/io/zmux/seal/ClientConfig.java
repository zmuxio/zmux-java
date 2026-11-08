package io.zmux.seal;

import io.zmux.RecordMode;
import java.time.Duration;
import java.util.List;

/**
 * The client's side of Establish.
 *
 * @param anchor judges the server; required unless allowPskOnly
 * @param allowPskOnly accepts any server that knows the pre-shared key: an explicit bootstrap, which must not
 *     override trust the host already holds
 * @param handle with psk selects a Profile with a pre-shared key; both null selects one without
 * @param psk the pre-shared key
 * @param key the client's static key; null is a key generated once per process
 * @param allowedProfiles the Profiles it may use, the first that fits the key being used; null: the
 *     Profiles of both ciphers, the one with hardware support first. A refusal never makes a client choose another
 *     Profile: that is the host's explicit choice
 * @param mode the carrier's record mode
 * @param credential goes in message 3; null: none
 * @param localContext handed unread to the anchor ({@link VerifyInput#localContext()})
 * @param requirePqAuth fails a server that is not PQ authenticated: no verdict named a PQ proof key whose proof verified
 * @param signer signs the transcript in message 3, for a server whose verdict names a proof key; null: no proof
 * @param appData supplies the application data of message 1, in Profiles 1 and 3 only, and of message 3, after
 *     the server is judged; null: none
 * @param maxAppData bounds the application data of a received message; 0: 256 bytes, negative: no bound beyond the
 *     message. More is too_large
 * @param shaper shapes the handshake; null: none
 * @param timeout bounds the handshake; zero: 10 s, negative: unbounded. A server that stalls a refused carrier
 *     leaves the client waiting until it passes ({@link NoReplyException}), so keep it bounded
 * @param shapingLimits apply to the shaper's plans
 */
public record ClientConfig(Anchor anchor, boolean allowPskOnly, byte[] handle, byte[] psk, StaticKey key, List<Profile> allowedProfiles,
                           RecordMode mode, Credential credential, io.zmux.carrier.HandshakeShaper shaper, Duration timeout, io.zmux.shaping.ShapingLimits shapingLimits,
                           Object localContext, boolean requirePqAuth, Signer signer, AppDataSource appData, int maxAppData) {
    public static Builder builder() { return new Builder(); }

    /** Builds a {@link ClientConfig}: Sealed, no credential, the defaults above. */
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
        private int maxAppData;
        private io.zmux.carrier.HandshakeShaper shaper;
        private Duration timeout = Duration.ZERO;
        private io.zmux.shaping.ShapingLimits shapingLimits = io.zmux.shaping.ShapingLimits.defaults();

        public Builder anchor(Anchor a) { anchor = a; return this; }
        public Builder allowPskOnly(boolean on) { allowPskOnly = on; return this; }
        public Builder psk(byte[] handle, byte[] psk) { this.handle = handle; this.psk = psk; return this; }
        public Builder key(StaticKey k) { key = k; return this; }
        public Builder allowedProfiles(Profile... p) { allowedProfiles = List.of(p); return this; }
        public Builder mode(RecordMode m) { mode = m; return this; }
        public Builder credential(Credential c) { credential = c; return this; }
        public Builder localContext(Object c) { localContext = c; return this; }
        public Builder requirePqAuth(boolean on) { requirePqAuth = on; return this; }
        public Builder signer(Signer s) { signer = s; return this; }
        public Builder appData(AppDataSource s) { appData = s; return this; }
        public Builder maxAppData(int n) { maxAppData = n; return this; }
        public Builder shaper(io.zmux.carrier.HandshakeShaper s) { shaper = s; return this; }
        public Builder timeout(Duration t) { timeout = t; return this; }
        public Builder shapingLimits(io.zmux.shaping.ShapingLimits l) { shapingLimits = l; return this; }
        public ClientConfig build() { return new ClientConfig(anchor, allowPskOnly, handle, psk, key, allowedProfiles, mode, credential, shaper, timeout, shapingLimits, localContext, requirePqAuth, signer, appData, maxAppData); }
    }
}
