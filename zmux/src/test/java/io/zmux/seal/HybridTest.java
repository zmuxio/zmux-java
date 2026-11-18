package io.zmux.seal;

import io.zmux.internal.kex.MlKem768;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zmux.internal.join.Mode;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The hybrid Profiles' table, defaults and KEM key handling. */
class HybridTest {
    private static final byte[] HANDLE = new byte[16];
    private static final byte[] PSK = new byte[32];

    @Test void tableAndDefaults() {
        for (Profile p : Profile.values()) {
            assertTrue(p.pqConfidential(), p.name());
        }
        assertEquals("Noise_XXhfs+psk0_25519+MLKEM768_ChaChaPoly_SHA256", Profile.XXHFS_PSK0_CHACHA.noiseName());
        assertEquals("Noise_XXhfs_25519+MLKEM768_AESGCM_SHA256", Profile.XXHFS_AES.noiseName());
        assertEquals(List.of(1L, 3L, 4L, 5L), java.util.Arrays.stream(Profile.values()).map(Profile::value).toList());
        assertEquals(List.of(1L, 3L), ServerConfig.DEFAULT_PROFILES.stream().map(Profile::value).toList());
        assertEquals(List.of(Profile.XXHFS_PSK0_CHACHA, Profile.XXHFS_CHACHA), Profiles.CLIENT.subList(0, 2));
        assertEquals(List.of(Profile.XXHFS_PSK0_AES, Profile.XXHFS_AES), Profiles.CLIENT_AES.subList(0, 2));
        for (long unassigned : new long[] {0, 2, 6, 7}) assertNull(Profile.of(unassigned));
        assertEquals(List.of(1248, 1216, 1200, 64), List.of(Handshake.minLength(Profile.XXHFS_PSK0_AES, 0), Handshake.minLength(Profile.XXHFS_CHACHA, 0),
                Handshake.minLength(Profile.XXHFS_CHACHA, 1), Handshake.minLength(Profile.XXHFS_PSK0_CHACHA, 2)));
    }

    /** A new KEM key pair for every handshake: message 1 of the no-PSK pattern shows it after the X25519 key. */
    @Test void freshKemKeyEveryHandshake() throws Exception {
        StaticKey s = StaticKey.generate();
        byte[][] ek = new byte[2][];
        for (int i = 0; i < 2; i++) {
            byte[] m = new Handshake(Profile.XXHFS_CHACHA, Mode.SEALED, true, null, null, null, s, StaticKey::generate).writeMessage(EstablishPayload.padding(0, true));
            assertEquals(1216, m.length);
            ek[i] = Arrays.copyOfRange(m, 32, m.length);
            MlKem768.check(ek[i]);
        }
        assertFalse(Arrays.equals(ek[0], ek[1]));
    }

    /** The modulus check is the one judge of a received key; the JDK's own check agrees with it. */
    @Test void encapsulationKeyCheck() throws Exception {
        byte[] ek = MlKem768.generate(null).encapsulationKey();
        MlKem768.check(ek);
        assertEquals(SecureCarrierException.Kind.KEX, assertThrows(SecureCarrierException.class, () -> MlKem768.check(Arrays.copyOf(ek, 1183))).kind());
        byte[] bad = ek.clone();
        bad[1151] = (byte) 0xff; // the last coefficient's high bits: 0xff0 | low nibble, at least 4080
        assertEquals(SecureCarrierException.Kind.KEX, assertThrows(SecureCarrierException.class, () -> MlKem768.check(bad)).kind());
        assertEquals(SecureCarrierException.Kind.KEX, assertThrows(SecureCarrierException.class, () -> MlKem768.encapsulate(bad, null)).kind());
        byte[] rhoOnly = ek.clone();
        Arrays.fill(rhoOnly, 1152, 1184, (byte) 0xff); // rho is not a coefficient
        MlKem768.check(rhoOnly);
        var dk = MlKem768.generate(null);
        var enc = MlKem768.encapsulate(dk.encapsulationKey(), null);
        assertEquals(MlKem768.CIPHERTEXT_LEN, enc.ciphertext().length);
        assertArrayEquals(enc.secret(), MlKem768.decapsulate(dk.key(), enc.ciphertext()));
    }

    @Test void clientChoosesByKey() throws Exception {
        var both = ClientConfig.builder().psk(HANDLE, PSK).allowedProfiles(Profile.XXHFS_CHACHA, Profile.XXHFS_PSK0_AES);
        assertEquals(Profile.XXHFS_PSK0_AES, Profiles.client(both.build()));
        var none = ClientConfig.builder().psk(HANDLE, PSK).allowedProfiles(Profile.XXHFS_CHACHA).build();
        assertEquals(SecureCarrierException.Kind.INVALID_CONFIG, assertThrows(SecureCarrierException.class, () -> Profiles.client(none)).kind());
        assertTrue(Profiles.client(ClientConfig.builder().psk(HANDLE, PSK).build()).isPskBased());
        assertFalse(Profiles.client(ClientConfig.builder().build()).isPskBased());
    }
}
