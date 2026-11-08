package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

class JoinSecretsTest {
    @Test void closingClearsThePsk() {
        byte[] psk = {1, 2, 3, 4};
        Session session = new Session(CoreConfig.builder().psk(psk).build(), 0);
        session.close(0, null);
        assertArrayEquals(new byte[4], psk);
    }
}
