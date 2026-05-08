package io.zmux.adapter.quic.netty;

import io.zmux.ErrorCode;
import io.zmux.OpenOptions;
import io.zmux.StreamMetadata;
import io.zmux.ZmuxErrorDirection;
import io.zmux.ZmuxErrorScope;
import io.zmux.ZmuxErrorSource;
import io.zmux.ZmuxException;
import io.zmux.protocol.DecodedVarint;
import io.zmux.protocol.Protocol;
import io.zmux.protocol.ZmuxCodec;
import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NettyQuicPreludeTest {
    @Test
    void emptyPreludeIsSingleZeroByte() throws IOException {
        assertArrayEquals(new byte[]{0}, NettyQuicPrelude.encode(OpenOptions.empty()));
    }

    @Test
    void metadataPreludeRoundTripsPriorityGroupAndOpenInfo() throws IOException {
        byte[] openInfo = new byte[]{1, 2, 3, 4};
        byte[] prelude = NettyQuicPrelude.encode(new OpenOptions(7L, 11L, openInfo));

        DecodedVarint decoded = ZmuxCodec.parseVarint(prelude, 0);
        byte[] metadataBytes = Arrays.copyOfRange(prelude, decoded.length(), prelude.length);
        StreamMetadata metadata = NettyQuicPrelude.decode(metadataBytes);

        assertEquals(7L, metadata.priority());
        assertEquals(11L, metadata.group());
        assertArrayEquals(openInfo, metadata.openInfo());
    }

    @Test
    void duplicateSingletonMetadataIsIgnored() throws IOException {
        byte[] duplicatePriorityMetadata = new byte[]{
                0x01, 0x01, 0x05,
                0x01, 0x01, 0x06
        };
        StreamMetadata metadata = NettyQuicPrelude.decode(duplicatePriorityMetadata);

        assertEquals(0L, metadata.priority());
        assertNull(metadata.group());
        assertArrayEquals(new byte[0], metadata.openInfo());
    }

    @Test
    void malformedMetadataIsRejected() {
        byte[] truncatedPriorityMetadata = new byte[]{
                (byte) Protocol.METADATA_STREAM_PRIORITY, 0x02, 0x05
        };
        ZmuxException error = assertThrows(
                ZmuxException.class,
                () -> NettyQuicPrelude.decode(truncatedPriorityMetadata)
        );

        assertEquals(ErrorCode.PROTOCOL.code(), error.code());
        assertEquals(ZmuxErrorScope.STREAM, error.scope());
        assertEquals(ZmuxErrorSource.REMOTE, error.source());
        assertEquals(ZmuxErrorDirection.BOTH, error.direction());
    }
}
