package io.zmux.seal;

import io.zmux.internal.kex.MlKem768;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** Establish's X25519 and ML-KEM randomness, without a vector's fixed source, is the operating system's. */
class HandshakeRandomTest {
    @Test void keysAndEncapsulationsDrawFromTheSystemSource() throws Exception {
        assertFalse(Arrays.equals(StaticKey.generate().publicRaw(), StaticKey.generate().publicRaw()));
        MlKem768.DecapsulationKey a = MlKem768.generate(null), b = MlKem768.generate(null);
        assertFalse(Arrays.equals(a.encapsulationKey(), b.encapsulationKey()));
        MlKem768.Encapsulation x = MlKem768.encapsulate(a.encapsulationKey(), null);
        MlKem768.Encapsulation y = MlKem768.encapsulate(a.encapsulationKey(), null);
        assertFalse(Arrays.equals(x.ciphertext(), y.ciphertext()));
        assertArrayEquals(x.secret(), MlKem768.decapsulate(a.key(), x.ciphertext()));
    }
}
