package io.zmux.seal;

import static io.zmux.seal.SecureCarrierException.Code.INVALID_CONFIG;

import io.zmux.internal.join.Suite;
import java.util.List;

/** The choices of Profile of both ends, in one place. */
final class Profiles {
    /**
     * A client's default list, without and with AES hardware. A refusal never makes a client choose another Profile.
     */
    static final List<Profile> CLIENT = List.of(Profile.XXHFS_PSK0_CHACHA, Profile.XXHFS_CHACHA, Profile.XXHFS_PSK0_AES, Profile.XXHFS_AES);
    static final List<Profile> CLIENT_AES = List.of(Profile.XXHFS_PSK0_AES, Profile.XXHFS_AES, Profile.XXHFS_PSK0_CHACHA, Profile.XXHFS_CHACHA);

    private Profiles() {}

    /** The Profile a client uses: the first allowed one that fits its pre-shared key. */
    static Profile client(ClientConfig c) throws SecureCarrierException {
        boolean withPsk = c.hasPsk();
        if (c.hasHandle() && !withPsk) throw new SecureCarrierException(INVALID_CONFIG, "Handle set without a pre-shared key");
        List<Profile> list = c.allowedProfiles();
        if (list == null) list = Suite.aesHardware() ? CLIENT_AES : CLIENT;
        for (Profile p : list) {
            if (p.hasPsk() == withPsk) return p;
        }
        throw new SecureCarrierException(INVALID_CONFIG, "no allowed profile fits the pre-shared key");
    }
}
