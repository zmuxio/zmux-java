package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class MetadataCodec {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private MetadataCodec() {
    }

    static List<Tlv> parseTlvs(byte[] source) throws IOException {
        List<Tlv> output = new ArrayList<>();
        FrameCodec.walkTlvs(source, 0, source.length, (type, value, offset, length) ->
                output.add(new Tlv(type, FrameCodec.copySlice(value, offset, length))));
        return output;
    }

    static byte[] buildOpenMetadataPrefix(long capabilities, Long priority, Long group, byte[] openInfo, long maxFramePayload) throws IOException {
        if (openInfo != null && openInfo.length > 0 && !Protocol.canCarryOpenInfo(capabilities)) {
            throw new OpenInfoUnavailableException();
        }
        if (!Protocol.supportsOpenMetadata(capabilities)) {
            return EMPTY_BYTES;
        }

        boolean includePriority = priority != null && Protocol.canCarryPriorityOnOpen(capabilities);
        boolean includeGroup = group != null && Protocol.canCarryGroupOnOpen(capabilities);
        boolean includeOpenInfo = openInfo != null && openInfo.length > 0;
        long metadataLength = 0L;
        if (includePriority) {
            metadataLength += tlvEncodedSize(Protocol.METADATA_STREAM_PRIORITY, Varint62.length(priority));
        }
        if (includeGroup) {
            metadataLength += tlvEncodedSize(Protocol.METADATA_STREAM_GROUP, Varint62.length(group));
        }
        if (includeOpenInfo) {
            metadataLength += tlvEncodedSize(Protocol.METADATA_OPEN_INFO, openInfo.length);
        }
        if (metadataLength == 0L) {
            return EMPTY_BYTES;
        }
        long payloadLength = Varint62.length(metadataLength) + metadataLength;
        if (payloadLength > maxFramePayload || payloadLength > Integer.MAX_VALUE) {
            throw new OpenMetadataTooLargeException();
        }

        byte[] payload = new byte[(int) payloadLength];
        int offset = Varint62.write(payload, 0, metadataLength);
        if (includePriority) {
            offset = writeVarintMetadata(payload, offset, Protocol.METADATA_STREAM_PRIORITY, priority);
        }
        if (includeGroup) {
            offset = writeVarintMetadata(payload, offset, Protocol.METADATA_STREAM_GROUP, group);
        }
        if (includeOpenInfo) {
            offset = writeTlv(payload, offset, Protocol.METADATA_OPEN_INFO, openInfo);
        }
        return exactPayload(payload, offset);
    }

    static byte[] buildPriorityUpdatePayload(long capabilities, Long priority, Long group, long maxPayload) throws IOException {
        if (priority == null && group == null) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "build priority update", "metadata update has no fields");
        }

        long payloadLength = Varint62.length(Protocol.EXT_PRIORITY_UPDATE);
        if (priority != null) {
            if (!Protocol.canCarryPriorityInUpdate(capabilities)) {
                throw FrameCodec.error(ErrorCode.PROTOCOL, "build priority update", "priority update unavailable");
            }
            payloadLength += tlvEncodedSize(Protocol.METADATA_STREAM_PRIORITY, Varint62.length(priority));
        }
        if (group != null) {
            if (!Protocol.canCarryGroupInUpdate(capabilities)) {
                throw FrameCodec.error(ErrorCode.PROTOCOL, "build priority update", "priority update unavailable");
            }
            payloadLength += tlvEncodedSize(Protocol.METADATA_STREAM_GROUP, Varint62.length(group));
        }
        if (payloadLength > maxPayload || payloadLength > Integer.MAX_VALUE) {
            throw new PriorityUpdateTooLargeException();
        }

        byte[] output = new byte[(int) payloadLength];
        int offset = Varint62.write(output, 0, Protocol.EXT_PRIORITY_UPDATE);
        if (priority != null) {
            offset = writeVarintMetadata(output, offset, Protocol.METADATA_STREAM_PRIORITY, priority);
        }
        if (group != null) {
            offset = writeVarintMetadata(output, offset, Protocol.METADATA_STREAM_GROUP, group);
        }
        return exactPayload(output, offset);
    }

    static FrameCodec.DataPayload parseDataPayload(byte[] payload, int flags) throws IOException {
        return parseDataPayload(payload, flags, true);
    }

    static FrameCodec.DataPayload parseDataPayloadView(byte[] payload, int flags) throws IOException {
        return parseDataPayload(payload, flags, false);
    }

    static FrameCodec.ParsedMetadata parseStreamMetadata(byte[] metadataBytes) throws IOException {
        return parseStreamMetadata(metadataBytes, 0, metadataBytes.length, true);
    }

    static FrameCodec.ParsedMetadata parseStreamMetadataView(byte[] metadataBytes) throws IOException {
        return parseStreamMetadata(metadataBytes, 0, metadataBytes.length, false);
    }

    static FrameCodec.ParsedPriorityUpdate parsePriorityUpdatePayload(byte[] payload) throws IOException {
        Varint62.Decoded subtype = Varint62.decode(payload, 0);
        if (subtype.value() != Protocol.EXT_PRIORITY_UPDATE) {
            return new FrameCodec.ParsedPriorityUpdate(false, false, 0L, false, null);
        }
        PriorityUpdateParseState state = new PriorityUpdateParseState();
        FrameCodec.walkTlvs(payload, subtype.length(), payload.length - subtype.length(), state::accept);
        return state.result();
    }

    static FrameCodec.GoAwayPayload parseGoAwayPayload(byte[] payload) throws IOException {
        int offset = 0;
        Varint62.Decoded bidi = Varint62.decode(payload, offset);
        offset += bidi.length();
        Varint62.Decoded uni = Varint62.decode(payload, offset);
        offset += uni.length();
        Varint62.Decoded code = Varint62.decode(payload, offset);
        offset += code.length();
        String reason = parseDiagReason(payload, offset, payload.length - offset);
        return new FrameCodec.GoAwayPayload(bidi.value(), uni.value(), code.value(), reason);
    }

    static byte[] buildGoAwayPayload(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason) throws IOException {
        return buildGoAwayPayload(lastAcceptedBidi, lastAcceptedUni, code, reason, Long.MAX_VALUE);
    }

    static byte[] buildGoAwayPayload(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason, long maxPayloadBytes) throws IOException {
        int baseLength = Varint62.length(lastAcceptedBidi)
                + Varint62.length(lastAcceptedUni)
                + Varint62.length(code);
        byte[] reasonBytes = encodeUtf8StrictOrEmpty(reason);
        int debugTextLength = debugTextValueLength(reasonBytes, baseLength, maxPayloadBytes);
        int payloadLength = baseLength + debugTextTlvLength(debugTextLength);
        byte[] output = new byte[payloadLength];
        int offset = Varint62.write(output, 0, lastAcceptedBidi);
        offset += Varint62.write(output, offset, lastAcceptedUni);
        offset += Varint62.write(output, offset, code);
        offset = writeDebugText(output, offset, reasonBytes, debugTextLength);
        return exactPayload(output, offset);
    }

    static FrameCodec.ErrorPayload parseErrorPayload(byte[] payload) throws IOException {
        Varint62.Decoded code = Varint62.decode(payload, 0);
        String reason = parseDiagReason(payload, code.length(), payload.length - code.length());
        return new FrameCodec.ErrorPayload(code.value(), reason);
    }

    static byte[] buildErrorPayload(long code, String reason, long maxPayloadBytes) throws IOException {
        int baseLength = Varint62.length(code);
        byte[] reasonBytes = encodeUtf8StrictOrEmpty(reason);
        int debugTextLength = debugTextValueLength(reasonBytes, baseLength, maxPayloadBytes);
        int payloadLength = baseLength + debugTextTlvLength(debugTextLength);
        byte[] output = new byte[payloadLength];
        int offset = Varint62.write(output, 0, code);
        offset = writeDebugText(output, offset, reasonBytes, debugTextLength);
        return exactPayload(output, offset);
    }

    static String parseDiagReason(byte[] payload) throws IOException {
        return parseDiagReason(payload, 0, payload.length);
    }

    private static String parseDiagReason(byte[] payload, int offset, int length) throws IOException {
        DiagReasonParseState state = new DiagReasonParseState();
        FrameCodec.walkTlvs(payload, offset, length, state::accept);
        if (!state.valid() || state.debugTextLength() == 0) {
            return "";
        }
        if (!isValidUtf8(state.debugTextSource(), state.debugTextOffset(), state.debugTextLength())) {
            return "";
        }
        return new String(
                state.debugTextSource(),
                state.debugTextOffset(),
                state.debugTextLength(),
                StandardCharsets.UTF_8
        );
    }

    private static FrameCodec.DataPayload parseDataPayload(byte[] payload, int flags, boolean copyOpenInfo) throws IOException {
        if ((flags & Protocol.FRAME_FLAG_OPEN_METADATA) == 0) {
            return new FrameCodec.DataPayload(false, false, 0L, null, EMPTY_BYTES, 0, 0, payload, 0, payload.length);
        }
        Varint62.Decoded metadataLength = Varint62.decode(payload, 0);
        int metadataOffset = metadataLength.length();
        if (metadataLength.value() > payload.length - metadataOffset) {
            throw FrameCodec.error(ErrorCode.FRAME_SIZE, "parse DATA payload", "open metadata overruns data payload");
        }
        int metadataLengthInt = (int) metadataLength.value();
        int appDataOffset = metadataOffset + metadataLengthInt;
        FrameCodec.ParsedMetadata metadata = parseStreamMetadata(payload, metadataOffset, metadataLengthInt, copyOpenInfo);
        return new FrameCodec.DataPayload(
                true,
                metadata.valid(),
                metadata.priority(),
                metadata.group(),
                metadata.openInfoBytes(),
                metadata.openInfoOffset(),
                metadata.openInfoLength(),
                payload,
                appDataOffset,
                payload.length - appDataOffset
        );
    }

    private static FrameCodec.ParsedMetadata parseStreamMetadata(byte[] source, int offset, int length, boolean copyOpenInfo) throws IOException {
        MetadataParseState state = new MetadataParseState(copyOpenInfo);
        FrameCodec.walkTlvs(source, offset, length, state::accept);
        return state.result();
    }

    private static int debugTextValueLength(byte[] bytes, int usedBytes, long maxPayloadBytes) throws IOException {
        if (bytes.length == 0) {
            return 0;
        }
        long availableBytes = maxPayloadBytes - usedBytes;
        if (availableBytes <= 0L) {
            return 0;
        }
        if (tlvEncodedSize(Protocol.DIAG_DEBUG_TEXT, bytes.length) <= availableBytes) {
            return bytes.length;
        }
        int maxPrefixLength = longestPrefixThatFits(bytes.length, availableBytes);
        if (maxPrefixLength <= 0) {
            return 0;
        }
        return utf8SafePrefixLength(bytes, maxPrefixLength);
    }

    private static int debugTextTlvLength(int valueLength) throws IOException {
        return valueLength <= 0 ? 0 : (int) tlvEncodedSize(Protocol.DIAG_DEBUG_TEXT, valueLength);
    }

    private static int writeDebugText(byte[] output, int offset, byte[] bytes, int length) throws IOException {
        if (length <= 0) {
            return offset;
        }
        offset += Varint62.write(output, offset, Protocol.DIAG_DEBUG_TEXT);
        offset += Varint62.write(output, offset, length);
        System.arraycopy(bytes, 0, output, offset, length);
        return offset + length;
    }

    private static long tlvEncodedSize(long type, int valueLength) throws IOException {
        return Varint62.length(type) + Varint62.length(valueLength) + valueLength;
    }

    private static int longestPrefixThatFits(int totalLength, long availableBytes) throws IOException {
        int low = 0;
        int high = totalLength;
        while (low < high) {
            int mid = low + ((high - low + 1) >>> 1);
            if (tlvEncodedSize(Protocol.DIAG_DEBUG_TEXT, mid) <= availableBytes) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return low;
    }

    private static int utf8SafePrefixLength(byte[] bytes, int maxLength) {
        int length = Math.min(maxLength, bytes.length);
        if (length <= 0 || length >= bytes.length) {
            return Math.max(length, 0);
        }
        int start = length - 1;
        while (start > 0 && isUtf8ContinuationByte(bytes[start])) {
            start--;
        }
        int codePointLength = utf8CodePointLength(bytes[start]);
        if (codePointLength <= 0) {
            return start;
        }
        return start + codePointLength <= length ? length : start;
    }

    private static boolean isUtf8ContinuationByte(byte value) {
        return (value & 0xC0) == 0x80;
    }

    private static int utf8CodePointLength(byte leadByte) {
        int value = leadByte & 0xFF;
        if ((value & 0x80) == 0) {
            return 1;
        }
        if ((value & 0xE0) == 0xC0) {
            return 2;
        }
        if ((value & 0xF0) == 0xE0) {
            return 3;
        }
        if ((value & 0xF8) == 0xF0) {
            return 4;
        }
        return -1;
    }

    private static byte[] encodeUtf8StrictOrEmpty(String value) {
        if (value == null || value.isEmpty()) {
            return EMPTY_BYTES;
        }
        if (!isWellFormedUtf16(value)) {
            return EMPTY_BYTES;
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static boolean isWellFormedUtf16(String value) {
        for (int index = 0; index < value.length(); index++) {
            char ch = value.charAt(index);
            if (Character.isHighSurrogate(ch)) {
                if (index + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(index + 1))) {
                    return false;
                }
                index++;
            } else if (Character.isLowSurrogate(ch)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isValidUtf8(byte[] value, int offset, int length) {
        int cursor = offset;
        int limit = offset + length;
        while (cursor < limit) {
            int b1 = value[cursor++] & 0xff;
            if (b1 < 0x80) {
                continue;
            }
            if (b1 < 0xc2) {
                return false;
            }
            if (b1 < 0xe0) {
                if (cursor >= limit || !isUtf8Continuation(value[cursor++] & 0xff)) {
                    return false;
                }
                continue;
            }
            if (b1 < 0xf0) {
                if (cursor + 1 >= limit) {
                    return false;
                }
                int b2 = value[cursor++] & 0xff;
                int b3 = value[cursor++] & 0xff;
                if (!isUtf8Continuation(b2) || !isUtf8Continuation(b3)) {
                    return false;
                }
                if (b1 == 0xe0 && b2 < 0xa0) {
                    return false;
                }
                if (b1 == 0xed && b2 >= 0xa0) {
                    return false;
                }
                continue;
            }
            if (b1 >= 0xf5 || cursor + 2 >= limit) {
                return false;
            }
            int b2 = value[cursor++] & 0xff;
            int b3 = value[cursor++] & 0xff;
            int b4 = value[cursor++] & 0xff;
            if (!isUtf8Continuation(b2) || !isUtf8Continuation(b3) || !isUtf8Continuation(b4)) {
                return false;
            }
            if (b1 == 0xf0 && b2 < 0x90) {
                return false;
            }
            if (b1 == 0xf4 && b2 >= 0x90) {
                return false;
            }
        }
        return true;
    }

    private static boolean isUtf8Continuation(int value) {
        return (value & 0xc0) == 0x80;
    }

    private static int writeVarintMetadata(byte[] output, int offset, long type, long value) throws IOException {
        offset += Varint62.write(output, offset, type);
        offset += Varint62.write(output, offset, Varint62.length(value));
        return offset + Varint62.write(output, offset, value);
    }

    private static int writeTlv(byte[] output, int offset, long type, byte[] value) throws IOException {
        byte[] effectiveValue = value == null ? EMPTY_BYTES : value;
        offset += Varint62.write(output, offset, type);
        offset += Varint62.write(output, offset, effectiveValue.length);
        System.arraycopy(effectiveValue, 0, output, offset, effectiveValue.length);
        return offset + effectiveValue.length;
    }

    private static byte[] exactPayload(byte[] output, int offset) {
        if (offset != output.length) {
            throw new IllegalStateException("metadata payload length mismatch");
        }
        return output;
    }

    private static final class MetadataParseState {
        private final boolean copyOpenInfo;
        private boolean valid = true;
        private boolean seenPriority;
        private boolean seenGroup;
        private boolean seenOpenInfo;
        private long priority;
        private Long group;
        private byte[] openInfoBytes = EMPTY_BYTES;
        private int openInfoOffset;
        private int openInfoLength;

        private MetadataParseState(boolean copyOpenInfo) {
            this.copyOpenInfo = copyOpenInfo;
        }

        private void accept(long type, byte[] source, int offset, int length) throws IOException {
            if (!valid) {
                return;
            }
            if (type == Protocol.METADATA_STREAM_PRIORITY) {
                if (seenPriority) {
                    invalidate();
                    return;
                }
                seenPriority = true;
                priority = FrameCodec.parseMetadataVarint(source, offset, length, "parse metadata");
                return;
            }
            if (type == Protocol.METADATA_STREAM_GROUP) {
                if (seenGroup) {
                    invalidate();
                    return;
                }
                seenGroup = true;
                group = FrameCodec.parseMetadataVarint(source, offset, length, "parse metadata");
                return;
            }
            if (type == Protocol.METADATA_OPEN_INFO) {
                if (seenOpenInfo) {
                    invalidate();
                    return;
                }
                seenOpenInfo = true;
                if (copyOpenInfo) {
                    openInfoBytes = FrameCodec.copySlice(source, offset, length);
                    openInfoOffset = 0;
                    openInfoLength = openInfoBytes.length;
                    return;
                }
                openInfoBytes = length == 0 ? EMPTY_BYTES : source;
                openInfoOffset = length == 0 ? 0 : offset;
                openInfoLength = length;
            }
        }

        private void invalidate() {
            valid = false;
            priority = 0L;
            group = null;
            openInfoBytes = EMPTY_BYTES;
            openInfoOffset = 0;
            openInfoLength = 0;
        }

        private FrameCodec.ParsedMetadata result() {
            if (!valid) {
                return new FrameCodec.ParsedMetadata(false, 0L, null, EMPTY_BYTES, 0, 0);
            }
            return new FrameCodec.ParsedMetadata(true, priority, group, openInfoBytes, openInfoOffset, openInfoLength);
        }
    }

    private static final class PriorityUpdateParseState {
        private boolean valid = true;
        private boolean seenPriority;
        private boolean seenGroup;
        private long priority;
        private Long group;

        private void accept(long type, byte[] source, int offset, int length) throws IOException {
            if (!valid) {
                return;
            }
            if (type == Protocol.METADATA_STREAM_PRIORITY) {
                if (seenPriority) {
                    invalidate();
                    return;
                }
                seenPriority = true;
                priority = FrameCodec.parseMetadataVarint(source, offset, length, "parse priority update");
                return;
            }
            if (type == Protocol.METADATA_STREAM_GROUP) {
                if (seenGroup) {
                    invalidate();
                    return;
                }
                seenGroup = true;
                group = FrameCodec.parseMetadataVarint(source, offset, length, "parse priority update");
            }
        }

        private void invalidate() {
            valid = false;
            priority = 0L;
            group = null;
        }

        private FrameCodec.ParsedPriorityUpdate result() {
            if (!valid) {
                return new FrameCodec.ParsedPriorityUpdate(false, false, 0L, false, null);
            }
            return new FrameCodec.ParsedPriorityUpdate(true, seenPriority, priority, seenGroup, group);
        }
    }

    private static final class DiagReasonParseState {
        private boolean valid = true;
        private boolean seenDebugText;
        private boolean seenRetryAfterMillis;
        private boolean seenOffendingStreamId;
        private boolean seenOffendingFrameType;
        private byte[] debugTextSource = EMPTY_BYTES;
        private int debugTextOffset;
        private int debugTextLength;

        private void accept(long type, byte[] source, int offset, int length) {
            if (!valid) {
                return;
            }
            if (type == Protocol.DIAG_DEBUG_TEXT) {
                if (seenDebugText) {
                    invalidate();
                    return;
                }
                seenDebugText = true;
                debugTextSource = source;
                debugTextOffset = offset;
                debugTextLength = length;
                return;
            }
            if (type == Protocol.DIAG_RETRY_AFTER_MILLIS) {
                if (seenRetryAfterMillis) {
                    invalidate();
                    return;
                }
                seenRetryAfterMillis = true;
                return;
            }
            if (type == Protocol.DIAG_OFFENDING_STREAM_ID) {
                if (seenOffendingStreamId) {
                    invalidate();
                    return;
                }
                seenOffendingStreamId = true;
                return;
            }
            if (type == Protocol.DIAG_OFFENDING_FRAME_TYPE) {
                if (seenOffendingFrameType) {
                    invalidate();
                    return;
                }
                seenOffendingFrameType = true;
            }
        }

        private void invalidate() {
            valid = false;
            debugTextSource = EMPTY_BYTES;
            debugTextOffset = 0;
            debugTextLength = 0;
        }

        private boolean valid() {
            return valid;
        }

        private byte[] debugTextSource() {
            return debugTextSource;
        }

        private int debugTextOffset() {
            return debugTextOffset;
        }

        private int debugTextLength() {
            return debugTextLength;
        }
    }
}
