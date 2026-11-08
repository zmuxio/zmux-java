package io.zmux.seal;

import io.zmux.RecordCarrier;
import java.util.ArrayList;
import java.util.List;

/**
 * The server's side of Establish. Refusals go to {@link io.zmux.ListenerConfig#onReject()}; nothing is ever sent
 * to report one. Validated when it is made: each mistake below is an {@link IllegalArgumentException}.
 *
 * @param gate the host's decision on clients; required
 * @param key the node's static key; required
 * @param acceptedProfiles the Profiles a client may use; null or empty is the default, the Profiles with a
 *     pre-shared key (1, 3). Listing a Profile without one (4, 5) is the only way to accept it: it sends the
 *     node's credentials to anonymous clients. A null element, which is what {@link Profile#of} gives for an
 *     unknown or reserved value, is an error. Join is always accepted on the carriers of the server's sessions
 * @param allowRecordMode the check of a Hello's mode, with the Establish's {@link Peer} as its peer; null: only Sealed.
 *     Joins have their own check, {@link io.zmux.ZmuxConfig.Builder#allowRecordMode}
 * @param shaper shapes the handshake; null: none
 * @param shapingLimits apply to the shaper's plans; null: the defaults
 * @param anchor judges the client after message 3, with its credential or none; null: clients are not judged
 * @param localContext handed unread to the anchor ({@link VerifyInput#localContext()})
 * @param requirePqAuth fails a client that is not PQ authenticated; needs an anchor
 * @param signer signs the transcript in message 2, for clients whose verdict names a proof key; in Profiles 4 and 5
 *     anyone can ask for it, so it limits its own rate; null: no proof
 * @param appData supplies the application data of message 2 ({@link AppDataSource}); null: none
 * @param maxAppData bounds the application data of a received message; 0: 256 bytes, negative: no bound beyond the
 *     message. More is too_large
 */
public record ServerConfig(Gate gate, StaticKey key, List<Profile> acceptedProfiles,
                           RecordCarrier.Allow allowRecordMode, io.zmux.carrier.HandshakeShaper shaper, io.zmux.shaping.ShapingLimits shapingLimits,
                           Anchor anchor, Object localContext, boolean requirePqAuth, Signer signer, AppDataSource appData, int maxAppData) {
    /** The Profiles a server accepts unless told otherwise. */
    public static final List<Profile> DEFAULT_PROFILES = List.of(Profile.XXHFS_PSK0_CHACHA, Profile.XXHFS_PSK0_AES);

    public ServerConfig {
        if (gate == null) throw new IllegalArgumentException("no Gate");
        if (key == null) throw new IllegalArgumentException("no static key");
        if (acceptedProfiles == null || acceptedProfiles.isEmpty()) {
            acceptedProfiles = DEFAULT_PROFILES;
        } else {
            for (Profile p : acceptedProfiles) if (p == null) throw new IllegalArgumentException("an unknown or reserved Profile among the accepted ones");
            acceptedProfiles = List.copyOf(new java.util.LinkedHashSet<>(acceptedProfiles));
        }
        if (shapingLimits == null) shapingLimits = io.zmux.shaping.ShapingLimits.defaults();
        if (requirePqAuth && anchor == null) throw new IllegalArgumentException("requirePqAuth without an anchor");
        try {
            PinAnchor.checkPqAuth(anchor, requirePqAuth);
        } catch (io.zmux.carrier.SecureCarrierException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /** Clients are not judged. */
    public ServerConfig(Gate gate, StaticKey key, List<Profile> acceptedProfiles,
                        RecordCarrier.Allow allowRecordMode, io.zmux.carrier.HandshakeShaper shaper, io.zmux.shaping.ShapingLimits shapingLimits) {
        this(gate, key, acceptedProfiles, allowRecordMode, shaper, shapingLimits, null, null, false, null, null, 0);
    }

    /** The default Profiles, Sealed only, unshaped. */
    public ServerConfig(Gate gate, StaticKey key) { this(gate, key, null, null, null, io.zmux.shaping.ShapingLimits.defaults()); }

    /** The default Profiles and limits on the shaper's plans. */
    public ServerConfig(Gate gate, StaticKey key, RecordCarrier.Allow allowRecordMode, io.zmux.carrier.HandshakeShaper shaper) {
        this(gate, key, null, allowRecordMode, shaper, io.zmux.shaping.ShapingLimits.defaults());
    }

    /** The same configuration accepting exactly {@code profiles}; none is the default. */
    public ServerConfig withAcceptedProfiles(Profile... profiles) {
        return new ServerConfig(gate, key, profiles == null ? null : new ArrayList<>(java.util.Arrays.asList(profiles)), allowRecordMode, shaper, shapingLimits, anchor, localContext, requirePqAuth, signer, appData, maxAppData);
    }

    /** The same configuration judging clients with anchor, which gets localContext unread. */
    public ServerConfig withAnchor(Anchor anchor, Object localContext, boolean requirePqAuth) {
        return new ServerConfig(gate, key, acceptedProfiles, allowRecordMode, shaper, shapingLimits, anchor, localContext, requirePqAuth, signer, appData, maxAppData);
    }

    /** The same configuration with the application data of message 2 from source, and received data bounded by maxAppData (0: 256 bytes, negative: the message). */
    public ServerConfig withAppData(AppDataSource source, int maxAppData) {
        return new ServerConfig(gate, key, acceptedProfiles, allowRecordMode, shaper, shapingLimits, anchor, localContext, requirePqAuth, signer, source, maxAppData);
    }

    /** The same configuration signing the transcript of message 2 with signer; null: no proof. */
    public ServerConfig withSigner(Signer signer) {
        return new ServerConfig(gate, key, acceptedProfiles, allowRecordMode, shaper, shapingLimits, anchor, localContext, requirePqAuth, signer, appData, maxAppData);
    }
}
