package io.zmux.seal;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.RecordMode;
import org.junit.jupiter.api.Test;

/** A client configuration a handshake cannot run fails when built, as a server configuration does. */
class ClientConfigValidationTest {
    @Test void anUnknownProfileIsRefusedWhenBuilt() {
        var b = ClientConfig.builder().allowedProfiles((Profile) null);
        assertThrows(IllegalArgumentException.class, b::build);
    }

    @Test void theUnmanagedModeIsRefusedWhenBuilt() {
        assertDoesNotThrow(() -> ClientConfig.builder().mode(RecordMode.TRUSTED).allowedProfiles(Profile.values()).build());
        var b = ClientConfig.builder().mode(RecordMode.UNMANAGED);
        assertThrows(IllegalArgumentException.class, b::build);
    }
}
