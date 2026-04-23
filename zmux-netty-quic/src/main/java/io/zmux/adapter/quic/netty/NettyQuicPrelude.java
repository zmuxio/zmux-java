package io.zmux.adapter.quic.netty;

import io.zmux.*;
import io.zmux.internal.FrameCodec;

import java.io.IOException;

final class NettyQuicPrelude {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[] EMPTY_PRELUDE = new byte[]{0};

    private NettyQuicPrelude() {
    }

    static byte[] encode(OpenOptions options) throws IOException {
        return encode(
                options != null ? options.initialPriority() : null,
                options != null ? options.initialGroup() : null,
                options != null ? options.openInfo() : EMPTY_BYTES
        );
    }

    static byte[] encode(Long priority, Long group, byte[] openInfo) throws IOException {
        byte[] prefix = FrameCodec.buildOpenMetadataPrefix(
                NettyQuicSupport.OPEN_CAPABILITIES,
                priority,
                group,
                openInfo == null ? EMPTY_BYTES : openInfo,
                NettyQuicSupport.STREAM_PRELUDE_MAX_PAYLOAD
        );
        return prefix.length == 0 ? EMPTY_PRELUDE : prefix;
    }

    static StreamMetadata decode(byte[] metadataBytes) throws IOException {
        return decodeMetadata(metadataBytes).toStreamMetadata();
    }

    static DecodedMetadata decodeMetadata(byte[] metadataBytes) throws IOException {
        if (metadataBytes == null || metadataBytes.length == 0) {
            return DecodedMetadata.empty();
        }
        FrameCodec.ParsedMetadata parsed;
        try {
            parsed = FrameCodec.parseStreamMetadataView(metadataBytes);
        } catch (IOException parseError) {
            throw invalidPreludeMetadata(parseError);
        }
        if (!parsed.valid()) {
            return DecodedMetadata.empty();
        }
        return new DecodedMetadata(parsed.priority(), parsed.group(), parsed.openInfo());
    }

    private static IOException invalidPreludeMetadata(IOException cause) {
        return new ZmuxException(
                ErrorCode.PROTOCOL.code(),
                "decode stream prelude",
                "malformed stream prelude metadata",
                cause,
                ZmuxErrorScope.STREAM,
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.ABORT
        );
    }

    static int requiredVarintBytes(byte firstByte) {
        return 1 << ((firstByte >>> 6) & 0x03);
    }

    static final class DecodedMetadata {
        private static final DecodedMetadata EMPTY = new DecodedMetadata(0L, null, EMPTY_BYTES);

        private final long priority;
        private final Long group;
        private final byte[] openInfo;

        DecodedMetadata(long priority, Long group, byte[] openInfo) {
            this.priority = priority;
            this.group = group;
            this.openInfo = openInfo == null || openInfo.length == 0 ? EMPTY_BYTES : openInfo;
        }

        static DecodedMetadata empty() {
            return EMPTY;
        }

        boolean prioritySet() {
            return priority != 0L;
        }

        boolean groupEncoded() {
            return group != null;
        }

        boolean emptyMetadata() {
            return priority == 0L && group == null && openInfo.length == 0;
        }

        StreamMetadata toStreamMetadata() {
            if (emptyMetadata()) {
                return StreamMetadata.empty();
            }
            return new StreamMetadata(priority, group, openInfo);
        }

        long priority() {
            return priority;
        }

        Long group() {
            return group;
        }

        byte[] openInfo() {
            return openInfo;
        }
    }
}
