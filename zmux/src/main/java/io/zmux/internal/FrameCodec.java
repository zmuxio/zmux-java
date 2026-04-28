package io.zmux.internal;

import io.zmux.*;

import java.io.*;
import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class FrameCodec {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private FrameCodec() {
    }

    public static Frame readFrame(InputStream input, Limits limits) throws IOException {
        return FrameEnvelopeCodec.readFrame(input, limits);
    }

    public static Decoder decoder(InputStream input) {
        return new Decoder(input);
    }

    static byte[] readInputBytes(InputStream input, int length) throws IOException {
        Objects.requireNonNull(input, "input");
        if (length < 0) {
            throw new IndexOutOfBoundsException("length < 0");
        }
        if (length == 0) {
            return EMPTY_BYTES;
        }
        byte[] bytes = new byte[length];
        int offset = 0;
        while (offset < length) {
            int remaining = length - offset;
            int read = input.read(bytes, offset, remaining);
            if (read == -1) {
                break;
            }
            validateReadProgress(read, remaining);
            if (read == 0) {
                int one = input.read();
                if (one < 0) {
                    break;
                }
                bytes[offset++] = (byte) one;
                continue;
            }
            offset += read;
        }
        return offset == length ? bytes : Arrays.copyOf(bytes, offset);
    }

    private static void validateReadProgress(int read, int requested) throws IOException {
        if (read < 0 || read > requested) {
            throw new IOException("input read reported invalid progress");
        }
    }

    public static void writeFrame(OutputStream output, Frame frame, Limits limits) throws IOException {
        FrameEnvelopeCodec.writeFrame(output, frame, limits);
    }

    static void writeFrame(OutputStream output,
                           Frame frame,
                           byte[] payloadPrefix,
                           byte[] payloadBytes,
                           int payloadOffset,
                           int payloadLength,
                           Limits limits) throws IOException {
        FrameEnvelopeCodec.writeFrame(output, frame, payloadPrefix, payloadBytes, payloadOffset, payloadLength, limits);
    }

    static void writeFrame(OutputStream output,
                           Frame frame,
                           byte[] payloadPrefix,
                           byte[][] payloadParts,
                           int payloadPartIndex,
                           int payloadPartOffset,
                           int payloadLength,
                           Limits limits) throws IOException {
        FrameEnvelopeCodec.writeFrame(output, frame, payloadPrefix, payloadParts, payloadPartIndex, payloadPartOffset, payloadLength, limits);
    }

    static void writeFrame(GatheringByteChannel output, Frame frame, Limits limits) throws IOException {
        FrameEnvelopeCodec.writeFrame(output, frame, limits);
    }

    static void writeFrame(GatheringByteChannel output,
                           Frame frame,
                           byte[] payloadPrefix,
                           byte[] payloadBytes,
                           int payloadOffset,
                           int payloadLength,
                           Limits limits) throws IOException {
        FrameEnvelopeCodec.writeFrame(output, frame, payloadPrefix, payloadBytes, payloadOffset, payloadLength, limits);
    }

    static void writeFrame(GatheringByteChannel output,
                           Frame frame,
                           byte[] payloadPrefix,
                           byte[][] payloadParts,
                           int payloadPartIndex,
                           int payloadPartOffset,
                           int payloadLength,
                           Limits limits) throws IOException {
        FrameEnvelopeCodec.writeFrame(output, frame, payloadPrefix, payloadParts, payloadPartIndex, payloadPartOffset, payloadLength, limits);
    }

    public static Preface readPreface(InputStream input) throws IOException {
        return PrefaceCodec.readPreface(input);
    }

    public static void writePreface(OutputStream output, Preface preface) throws IOException {
        PrefaceCodec.writePreface(output, preface);
    }

    static void writePreface(OutputStream output, Preface preface, ZmuxConfig config) throws IOException {
        PrefaceCodec.writePreface(output, preface, config);
    }

    public static Negotiated negotiate(Preface local, Preface peer) throws IOException {
        return PrefaceCodec.negotiate(local, peer);
    }

    public static Role[] resolveRoles(Role localRole, long localNonce, Role peerRole, long peerNonce) throws IOException {
        return PrefaceCodec.resolveRoles(localRole, localNonce, peerRole, peerNonce);
    }

    public static byte[] marshalSettings(Settings settings) throws IOException {
        return PrefaceCodec.marshalSettings(settings);
    }

    public static Settings parseSettings(byte[] source) throws IOException {
        return PrefaceCodec.parseSettings(source);
    }

    public static List<Tlv> parseTlvs(byte[] source) throws IOException {
        return MetadataCodec.parseTlvs(source);
    }

    public static void appendTlv(ByteArrayOutputStream output, long type, byte[] value) throws IOException {
        Varint62.write(output, type);
        Varint62.write(output, value.length);
        output.write(value);
    }

    public static byte[] buildOpenMetadataPrefix(long capabilities, Long priority, Long group, byte[] openInfo, long maxFramePayload) throws IOException {
        return MetadataCodec.buildOpenMetadataPrefix(capabilities, priority, group, openInfo, maxFramePayload);
    }

    public static byte[] buildPriorityUpdatePayload(long capabilities, Long priority, Long group, long maxPayload) throws IOException {
        return MetadataCodec.buildPriorityUpdatePayload(capabilities, priority, group, maxPayload);
    }

    public static DataPayload parseDataPayload(byte[] payload, int flags) throws IOException {
        return MetadataCodec.parseDataPayload(payload, flags);
    }

    public static DataPayload parseDataPayloadView(byte[] payload, int flags) throws IOException {
        return MetadataCodec.parseDataPayloadView(payload, flags);
    }

    public static ParsedMetadata parseStreamMetadata(byte[] metadataBytes) throws IOException {
        return MetadataCodec.parseStreamMetadata(metadataBytes);
    }

    public static ParsedMetadata parseStreamMetadataView(byte[] metadataBytes) throws IOException {
        return MetadataCodec.parseStreamMetadataView(metadataBytes);
    }

    public static ParsedPriorityUpdate parsePriorityUpdatePayload(byte[] payload) throws IOException {
        return MetadataCodec.parsePriorityUpdatePayload(payload);
    }

    public static GoAwayPayload parseGoAwayPayload(byte[] payload) throws IOException {
        return MetadataCodec.parseGoAwayPayload(payload);
    }

    public static byte[] buildGoAwayPayload(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason) throws IOException {
        return MetadataCodec.buildGoAwayPayload(lastAcceptedBidi, lastAcceptedUni, code, reason);
    }

    public static byte[] buildGoAwayPayload(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason, long maxPayloadBytes) throws IOException {
        return MetadataCodec.buildGoAwayPayload(lastAcceptedBidi, lastAcceptedUni, code, reason, maxPayloadBytes);
    }

    public static ErrorPayload parseErrorPayload(byte[] payload) throws IOException {
        return MetadataCodec.parseErrorPayload(payload);
    }

    public static byte[] buildErrorPayload(long code, String reason, long maxPayloadBytes) throws IOException {
        return MetadataCodec.buildErrorPayload(code, reason, maxPayloadBytes);
    }

    public static String parseDiagReason(byte[] payload) throws IOException {
        return MetadataCodec.parseDiagReason(payload);
    }

    static void walkTlvs(byte[] source, int offset, int length, TlvVisitor visitor) throws IOException {
        RangeChecks.checkFromIndexSize(offset, length, source.length);
        int cursor = offset;
        int limit = offset + length;
        while (cursor < limit) {
            Varint62.Decoded type = Varint62.decode(source, cursor);
            cursor += type.length();
            Varint62.Decoded valueLength = Varint62.decode(source, cursor);
            cursor += valueLength.length();
            if (valueLength.value() > limit - cursor) {
                throw error(ErrorCode.PROTOCOL, "parse tlv", "tlv value overruns containing payload");
            }
            int valueLengthInt = (int) valueLength.value();
            visitor.accept(type.value(), source, cursor, valueLengthInt);
            cursor += valueLengthInt;
        }
    }

    static byte[] copySlice(byte[] source, int offset, int length) {
        if (length == 0) {
            return EMPTY_BYTES;
        }
        return Arrays.copyOfRange(source, offset, offset + length);
    }

    private static byte[] normalizeSliceBytes(byte[] bytes) {
        return bytes == null ? EMPTY_BYTES : bytes;
    }

    private static int normalizedSliceOffset(byte[] bytes, int offset, int length) {
        if (length <= 0) {
            return 0;
        }
        RangeChecks.checkFromIndexSize(offset, length, bytes.length);
        return offset;
    }

    private static int normalizedSliceLength(int length) {
        return Math.max(0, length);
    }

    static long parseMetadataVarint(byte[] source, int offset, int length, String operation) throws IOException {
        Varint62.Decoded decoded = Varint62.decode(source, offset);
        if (decoded.length() != length) {
            throw error(ErrorCode.PROTOCOL, operation, "invalid metadata varint");
        }
        return decoded.value();
    }

    static int checkedLength(long value, ErrorCode code, String operation, String message) throws IOException {
        if (value < 0L || value > Integer.MAX_VALUE) {
            throw error(code, operation, message);
        }
        return (int) value;
    }

    static ZmuxException error(ErrorCode code, String operation, String message) {
        return error(code, operation, message, null);
    }

    static ZmuxException error(ErrorCode code, String operation, String message, Throwable cause) {
        return new ZmuxException(
                code.code(),
                operation,
                message,
                cause,
                ZmuxErrorScope.SESSION,
                codecErrorSource(operation),
                codecErrorDirection(operation),
                ZmuxTerminationKind.UNKNOWN
        );
    }

    private static ZmuxErrorSource codecErrorSource(String operation) {
        if (operation == null || operation.isEmpty()) {
            return ZmuxErrorSource.UNKNOWN;
        }
        if (operation.startsWith("read ") || operation.startsWith("parse ")) {
            return ZmuxErrorSource.REMOTE;
        }
        if (operation.startsWith("write ")
                || operation.startsWith("marshal ")
                || operation.startsWith("build ")) {
            return ZmuxErrorSource.LOCAL;
        }
        return ZmuxErrorSource.UNKNOWN;
    }

    private static ZmuxErrorDirection codecErrorDirection(String operation) {
        if (operation == null || operation.isEmpty()) {
            return ZmuxErrorDirection.BOTH;
        }
        if (operation.startsWith("read ") || operation.startsWith("parse ")) {
            return ZmuxErrorDirection.READ;
        }
        if (operation.startsWith("write ")
                || operation.startsWith("marshal ")
                || operation.startsWith("build ")) {
            return ZmuxErrorDirection.WRITE;
        }
        return ZmuxErrorDirection.BOTH;
    }

    @FunctionalInterface
    interface TlvVisitor {
        void accept(long type, byte[] source, int offset, int length) throws IOException;
    }

    public static final class Frame {
        private final FrameType type;
        private final int flags;
        private final long streamId;
        private final byte[] payload;

        public Frame(FrameType type, int flags, long streamId, byte[] payload) {
            this.type = type;
            this.flags = flags;
            this.streamId = streamId;
            this.payload = payload;
        }

        public FrameType type() {
            return type;
        }

        public int flags() {
            return flags;
        }

        public long streamId() {
            return streamId;
        }

        public byte[] payload() {
            return payload;
        }
    }

    public static final class DataPayload {
        private static final byte[] EMPTY_BYTES = new byte[0];

        private final boolean hasMetadata;
        private final boolean metadataValid;
        private final long priority;
        private final Long group;
        private final byte[] openInfoBytes;
        private final int openInfoOffset;
        private final int openInfoLength;
        private final byte[] appData;
        private final int appDataOffset;
        private final int appDataLength;

        DataPayload(boolean hasMetadata,
                    boolean metadataValid,
                    long priority,
                    Long group,
                    byte[] openInfoBytes,
                    int openInfoOffset,
                    int openInfoLength,
                    byte[] appData,
                    int appDataOffset,
                    int appDataLength) {
            this.hasMetadata = hasMetadata;
            this.metadataValid = metadataValid;
            this.priority = priority;
            this.group = group;
            this.openInfoBytes = normalizeSliceBytes(openInfoBytes);
            this.openInfoOffset = normalizedSliceOffset(this.openInfoBytes, openInfoOffset, openInfoLength);
            this.openInfoLength = normalizedSliceLength(openInfoLength);
            this.appData = normalizeSliceBytes(appData);
            this.appDataOffset = normalizedSliceOffset(this.appData, appDataOffset, appDataLength);
            this.appDataLength = normalizedSliceLength(appDataLength);
        }

        public boolean hasMetadata() {
            return hasMetadata;
        }

        public boolean metadataValid() {
            return metadataValid;
        }

        public long priority() {
            return priority;
        }

        public Long group() {
            return group;
        }

        public byte[] openInfo() {
            if (openInfoLength == 0) {
                return EMPTY_BYTES;
            }
            return Arrays.copyOfRange(openInfoBytes, openInfoOffset, openInfoOffset + openInfoLength);
        }

        public byte[] appData() {
            if (appDataLength == 0) {
                return EMPTY_BYTES;
            }
            if (appDataOffset == 0 && appDataLength == appData.length) {
                return appData;
            }
            return Arrays.copyOfRange(appData, appDataOffset, appDataOffset + appDataLength);
        }

        byte[] appDataBytes() {
            return appData;
        }

        int appDataOffset() {
            return appDataOffset;
        }

        int appDataLength() {
            return appDataLength;
        }
    }

    public static final class ParsedMetadata {
        private static final byte[] EMPTY_BYTES = new byte[0];

        private final boolean valid;
        private final long priority;
        private final Long group;
        private final byte[] openInfoBytes;
        private final int openInfoOffset;
        private final int openInfoLength;

        ParsedMetadata(boolean valid,
                       long priority,
                       Long group,
                       byte[] openInfoBytes,
                       int openInfoOffset,
                       int openInfoLength) {
            this.valid = valid;
            this.priority = priority;
            this.group = group;
            this.openInfoBytes = normalizeSliceBytes(openInfoBytes);
            this.openInfoOffset = normalizedSliceOffset(this.openInfoBytes, openInfoOffset, openInfoLength);
            this.openInfoLength = normalizedSliceLength(openInfoLength);
        }

        public boolean valid() {
            return valid;
        }

        public long priority() {
            return priority;
        }

        public Long group() {
            return group;
        }

        public byte[] openInfo() {
            if (openInfoLength == 0) {
                return EMPTY_BYTES;
            }
            return Arrays.copyOfRange(openInfoBytes, openInfoOffset, openInfoOffset + openInfoLength);
        }

        byte[] openInfoBytes() {
            return openInfoBytes;
        }

        int openInfoOffset() {
            return openInfoOffset;
        }

        int openInfoLength() {
            return openInfoLength;
        }
    }

    public static final class ParsedPriorityUpdate {
        private final boolean valid;
        private final boolean hasPriority;
        private final long priority;
        private final boolean hasGroup;
        private final Long group;

        public ParsedPriorityUpdate(boolean valid, boolean hasPriority, long priority, boolean hasGroup, Long group) {
            this.valid = valid;
            this.hasPriority = hasPriority;
            this.priority = priority;
            this.hasGroup = hasGroup;
            this.group = group;
        }

        public boolean valid() {
            return valid;
        }

        public boolean hasPriority() {
            return hasPriority;
        }

        public long priority() {
            return priority;
        }

        public boolean hasGroup() {
            return hasGroup;
        }

        public Long group() {
            return group;
        }
    }

    public static final class GoAwayPayload {
        private final long lastAcceptedBidi;
        private final long lastAcceptedUni;
        private final long code;
        private final String reason;

        public GoAwayPayload(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason) {
            this.lastAcceptedBidi = lastAcceptedBidi;
            this.lastAcceptedUni = lastAcceptedUni;
            this.code = code;
            this.reason = reason;
        }

        public long lastAcceptedBidi() {
            return lastAcceptedBidi;
        }

        public long lastAcceptedUni() {
            return lastAcceptedUni;
        }

        public long code() {
            return code;
        }

        public String reason() {
            return reason;
        }
    }

    public static final class ErrorPayload {
        private final long code;
        private final String reason;

        public ErrorPayload(long code, String reason) {
            this.code = code;
            this.reason = reason;
        }

        public long code() {
            return code;
        }

        public String reason() {
            return reason;
        }
    }

    public static final class Decoder {
        private static final int DEFAULT_BUFFER_SIZE = 8192;

        private final InputStream input;
        private final byte[] buffer;
        private int position;
        private int limit;

        private Decoder(InputStream input) {
            this.input = Objects.requireNonNull(input, "input");
            this.buffer = new byte[DEFAULT_BUFFER_SIZE];
        }

        public Preface readPreface() throws IOException {
            return PrefaceCodec.readPreface(this);
        }

        public Frame readFrame(Limits limits) throws IOException {
            return FrameEnvelopeCodec.readFrame(this, limits);
        }

        int readByte() throws IOException {
            if (position >= limit) {
                refill();
            }
            return buffer[position++] & 0xff;
        }

        byte[] readBytesExact(int length) throws IOException {
            if (length == 0) {
                return EMPTY_BYTES;
            }
            byte[] bytes = new byte[length];
            readFully(bytes, 0, length);
            return bytes;
        }

        void readFully(byte[] dst, int offset, int length) throws IOException {
            RangeChecks.checkFromIndexSize(offset, length, dst.length);
            int remaining = length;
            int cursor = offset;
            while (remaining > 0) {
                int buffered = limit - position;
                if (buffered > 0) {
                    int copy = Math.min(buffered, remaining);
                    System.arraycopy(buffer, position, dst, cursor, copy);
                    position += copy;
                    cursor += copy;
                    remaining -= copy;
                    continue;
                }
                if (remaining >= buffer.length) {
                    int read = readDirect(dst, cursor, remaining);
                    cursor += read;
                    remaining -= read;
                    continue;
                }
                refill();
            }
        }

        private void refill() throws IOException {
            position = 0;
            limit = readDirect(buffer, 0, buffer.length);
        }

        private int readDirect(byte[] dst, int offset, int length) throws IOException {
            int read = input.read(dst, offset, length);
            if (read != -1) {
                validateReadProgress(read, length);
            }
            if (read > 0) {
                return read;
            }
            if (read < 0) {
                throw new EOFException();
            }
            int single = input.read();
            if (single < 0) {
                throw new EOFException();
            }
            dst[offset] = (byte) single;
            return 1;
        }
    }
}
