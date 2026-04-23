package io.zmux;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ErrorCodeTest {
    @Test
    void fromCodeResolvesKnownValues() {
        assertEquals(ErrorCode.PROTOCOL, ErrorCode.fromCode(ErrorCode.PROTOCOL.code()));
    }

    @Test
    void fromCodeRejectsUnknownValuesInsteadOfSilentlyMappingToInternal() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ErrorCode.fromCode(999L)
        );

        assertEquals("unknown zmux error code: 999", error.getMessage());
    }
}
