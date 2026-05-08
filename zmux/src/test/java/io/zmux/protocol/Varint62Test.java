package io.zmux.protocol;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class Varint62Test {
    private static void assertDecoded(long value) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Varint62.write(output, value);
        Varint62.Decoded decoded = Varint62.read(new ByteArrayInputStream(output.toByteArray()));
        assertEquals(value, decoded.value(), "decoded varint value mismatch");
        assertEquals(Varint62.length(value), decoded.length(), "decoded varint length mismatch");
    }

    @Test
    void readDecodesCanonicalValuesWithoutTemporaryBufferAssembly() throws Exception {
        assertDecoded(0L);
        assertDecoded(63L);
        assertDecoded(64L);
        assertDecoded(16_383L);
        assertDecoded(16_384L);
        assertDecoded(1_073_741_823L);
        assertDecoded(1_073_741_824L);
        assertDecoded(Protocol.MAX_VARINT62);
    }

    @Test
    void readWrapsMissingFirstByteAsStructuredProtocolError() {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> Varint62.read(new ByteArrayInputStream(new byte[0]))
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code());
        assertEquals("truncated varint62", error.getMessage());
        assertEquals(ZmuxErrorScope.SESSION, error.scope());
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.READ, error.direction());
    }

    @Test
    void readWrapsTruncatedContinuationAsStructuredProtocolError() {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> Varint62.read(new ByteArrayInputStream(new byte[]{0x40}))
        );

        assertEquals("truncated varint62", error.getMessage());
        assertEquals(ZmuxErrorScope.SESSION, error.scope());
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.READ, error.direction());
    }

    @Test
    void decodeRejectsNegativeOffsetAsLocalArgumentError() {
        IndexOutOfBoundsException error = assertThrows(
                IndexOutOfBoundsException.class,
                () -> Varint62.decode(new byte[]{0}, -1)
        );

        assertEquals("offset < 0", error.getMessage());
    }

    @Test
    void decodeKeepsOffsetAtEndAsStructuredTruncation() {
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> Varint62.decode(new byte[]{0}, 1)
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code());
        assertEquals("truncated varint62", error.getMessage());
        assertEquals(ZmuxErrorScope.SESSION, error.scope());
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.READ, error.direction());
    }

    @Test
    void decodeRejectsOffsetPastEndAsLocalArgumentError() {
        IndexOutOfBoundsException error = assertThrows(
                IndexOutOfBoundsException.class,
                () -> Varint62.decode(new byte[]{0}, 2)
        );

        assertEquals("offset > source.length", error.getMessage());
    }
}
