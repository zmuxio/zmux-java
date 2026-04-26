package io.zmux.internal;

import io.zmux.ErrorCode;
import io.zmux.FrameType;
import io.zmux.Limits;
import io.zmux.Protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;
import java.util.Objects;

final class FrameEnvelopeCodec {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final long MAX_INBOUND_FRAME_HEADER_OVERHEAD = 9L;
    private static final int GATHER_SCRATCH_RETAIN_FACTOR = 4;
    private static final int MIN_GATHER_SCRATCH_HINT = 32;
    private static final int GATHER_INLINE_PREFIX_BYTES = 256;
    private static final int GATHER_INLINE_CONTROL_PAYLOAD_BYTES = 128;

    private FrameEnvelopeCodec() {
    }

    static FrameCodec.Frame readFrame(InputStream input, Limits limits) throws IOException {
        Limits normalized = limits.normalize();
        Varint62.Decoded frameLength = readValidatedFrameLength(input, normalized);
        int code = input.read();
        if (code < 0) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "read frame", "truncated frame");
        }
        FrameType type = parseFrameType(code & 0x1f);
        int flags = code & 0xe0;
        Varint62.Decoded streamIdDecoded = Varint62.read(input);
        long payloadLength = validatedPayloadLength(normalized, type, frameLength.value(), streamIdDecoded.length());
        byte[] payload = FrameCodec.readInputBytes(input, FrameCodec.checkedLength(
                payloadLength,
                ErrorCode.FRAME_SIZE,
                "read frame",
                "payload exceeds Java implementation limit"
        ));
        if (payload.length != payloadLength) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "read frame", "truncated frame payload");
        }
        FrameCodec.Frame frame = new FrameCodec.Frame(type, flags, streamIdDecoded.value(), payload);
        validateFrame(frame, normalized, true);
        return frame;
    }

    static FrameCodec.Frame readFrame(FrameCodec.Decoder input, Limits limits) throws IOException {
        Limits normalized = limits.normalize();
        Varint62.Decoded frameLength = readValidatedFrameLength(input, normalized);
        int code = input.readByte();
        FrameType type = parseFrameType(code & 0x1f);
        int flags = code & 0xe0;
        Varint62.Decoded streamIdDecoded = Varint62.read(input);
        long payloadLength = validatedPayloadLength(normalized, type, frameLength.value(), streamIdDecoded.length());
        byte[] payload = input.readBytesExact(FrameCodec.checkedLength(
                payloadLength,
                ErrorCode.FRAME_SIZE,
                "read frame",
                "payload exceeds Java implementation limit"
        ));
        FrameCodec.Frame frame = new FrameCodec.Frame(type, flags, streamIdDecoded.value(), payload);
        validateFrame(frame, normalized, true);
        return frame;
    }

    static InboundFrame readInboundFrame(FrameCodec.Decoder input,
                                         Limits limits,
                                         InboundPayloadPool payloadPool) throws IOException {
        Limits normalized = limits.normalize();
        Varint62.Decoded frameLength = readValidatedFrameLength(input, normalized);
        int code = input.readByte();
        FrameType type = parseFrameType(code & 0x1f);
        int flags = code & 0xe0;
        Varint62.Decoded streamIdDecoded = Varint62.read(input);
        long payloadLength = validatedPayloadLength(normalized, type, frameLength.value(), streamIdDecoded.length());
        int payloadLengthInt = FrameCodec.checkedLength(
                payloadLength,
                ErrorCode.FRAME_SIZE,
                "read frame",
                "payload exceeds Java implementation limit"
        );
        InboundPayloadPool.Handle handle = type == FrameType.DATA && payloadPool != null
                ? payloadPool.acquire(payloadLengthInt)
                : null;
        byte[] payload = payloadLengthInt == 0
                ? EMPTY_BYTES
                : handle == null ? new byte[payloadLengthInt] : handle.bytes();
        boolean success = false;
        try {
            input.readFully(payload, 0, payloadLengthInt);
            FrameCodec.Frame frame = new FrameCodec.Frame(type, flags, streamIdDecoded.value(), payload);
            validateFrame(frame, normalized, true);
            success = true;
            return new InboundFrame(type, flags, streamIdDecoded.value(), payload, handle);
        } finally {
            if (!success && handle != null) {
                handle.release();
            }
        }
    }

    static void writeFrame(OutputStream output, FrameCodec.Frame frame, Limits limits) throws IOException {
        writeFrame(output, frame, null, frame.payload(), 0, frame.payload().length, limits);
    }

    static void writeFrame(OutputStream output,
                           FrameCodec.Frame frame,
                           byte[] payloadPrefix,
                           byte[] payloadBytes,
                           int payloadOffset,
                           int payloadLength,
                           Limits limits) throws IOException {
        Limits normalized = limits.normalize();
        byte[] prefix = payloadPrefix == null ? EMPTY_BYTES : payloadPrefix;
        byte[] payload = payloadBytes == null ? EMPTY_BYTES : payloadBytes;
        RangeChecks.checkFromIndexSize(payloadOffset, payloadLength, payload.length);
        long encodedPayloadLength = encodedPayloadLength(prefix.length, payloadLength);
        validateOutboundFrame(frame, normalized, encodedPayloadLength, prefix.length > 0);
        long frameLength = frameLength(frame.streamId(), encodedPayloadLength);
        Varint62.write(output, frameLength);
        output.write(frame.type().code() | frame.flags());
        Varint62.write(output, frame.streamId());
        if (prefix.length > 0) {
            output.write(prefix);
        }
        if (payloadLength > 0) {
            output.write(payload, payloadOffset, payloadLength);
        }
    }

    static void writeFrame(OutputStream output,
                           FrameCodec.Frame frame,
                           byte[] payloadPrefix,
                           byte[][] payloadParts,
                           int payloadPartIndex,
                           int payloadPartOffset,
                           int payloadLength,
                           Limits limits) throws IOException {
        Limits normalized = limits.normalize();
        byte[] prefix = payloadPrefix == null ? EMPTY_BYTES : payloadPrefix;
        validatePayloadParts(payloadParts, payloadPartIndex, payloadPartOffset, payloadLength);
        long encodedPayloadLength = encodedPayloadLength(prefix.length, payloadLength);
        validateOutboundFrame(frame, normalized, encodedPayloadLength, prefix.length > 0 || payloadLength > 0);
        long frameLength = frameLength(frame.streamId(), encodedPayloadLength);
        Varint62.write(output, frameLength);
        output.write(frame.type().code() | frame.flags());
        Varint62.write(output, frame.streamId());
        if (prefix.length > 0) {
            output.write(prefix);
        }
        writePayloadParts(output, payloadParts, payloadPartIndex, payloadPartOffset, payloadLength);
    }

    static void writeFrame(GatheringByteChannel output, FrameCodec.Frame frame, Limits limits) throws IOException {
        writeFrame(output, frame, null, frame.payload(), 0, frame.payload().length, limits);
    }

    static void writeFrame(GatheringByteChannel output,
                           FrameCodec.Frame frame,
                           byte[] payloadPrefix,
                           byte[] payloadBytes,
                           int payloadOffset,
                           int payloadLength,
                           Limits limits) throws IOException {
        GatherScratch scratch = new GatherScratch();
        appendFrame(scratch, frame, payloadPrefix, payloadBytes, payloadOffset, payloadLength, limits);
        writeGatheredBuffers(output, scratch);
    }

    static void writeFrame(GatheringByteChannel output,
                           FrameCodec.Frame frame,
                           byte[] payloadPrefix,
                           byte[][] payloadParts,
                           int payloadPartIndex,
                           int payloadPartOffset,
                           int payloadLength,
                           Limits limits) throws IOException {
        GatherScratch scratch = new GatherScratch();
        appendFrame(scratch, frame, payloadPrefix, payloadParts, payloadPartIndex, payloadPartOffset, payloadLength, limits);
        writeGatheredBuffers(output, scratch);
    }

    static int gatherHeaderBytes(long frameLength, long streamId) throws IOException {
        return Varint62.length(frameLength) + 1 + Varint62.length(streamId);
    }

    static int gatherBufferCount(byte[] payloadPrefix, int payloadLength) {
        int count = 1;
        if (payloadPrefix != null && payloadPrefix.length > 0) {
            count++;
        }
        if (payloadLength > 0) {
            count++;
        }
        return count;
    }

    static int gatherBufferCount(byte[] payloadPrefix,
                                 byte[][] payloadParts,
                                 int payloadPartIndex,
                                 int payloadPartOffset,
                                 int payloadLength) {
        return 1
                + ((payloadPrefix != null && payloadPrefix.length > 0) ? 1 : 0)
                + countPayloadPartBuffers(payloadParts, payloadPartIndex, payloadPartOffset, payloadLength);
    }

    static int gatherInlineBytes(FrameCodec.Frame frame, byte[] payloadPrefix, int payloadLength) {
        byte[] prefix = payloadPrefix == null ? EMPTY_BYTES : payloadPrefix;
        int inlinePrefixLength = inlinePrefixLength(prefix);
        return inlinePrefixLength + inlinePayloadLength(frame, prefix.length, inlinePrefixLength, payloadLength);
    }

    static long appendFrame(GatherScratch scratch,
                            FrameCodec.Frame frame,
                            byte[] payloadPrefix,
                            byte[] payloadBytes,
                            int payloadOffset,
                            int payloadLength,
                            Limits limits) throws IOException {
        Limits normalized = limits.normalize();
        byte[] prefix = payloadPrefix == null ? EMPTY_BYTES : payloadPrefix;
        byte[] payload = payloadBytes == null ? EMPTY_BYTES : payloadBytes;
        RangeChecks.checkFromIndexSize(payloadOffset, payloadLength, payload.length);
        long encodedPayloadLength = encodedPayloadLength(prefix.length, payloadLength);
        validateOutboundFrame(frame, normalized, encodedPayloadLength, prefix.length > 0);
        long frameLength = frameLength(frame.streamId(), encodedPayloadLength);
        int headerBytes = gatherHeaderBytes(frameLength, frame.streamId());
        int inlinePrefixLength = inlinePrefixLength(prefix);
        int inlinePayloadLength = inlinePayloadLength(frame, prefix.length, inlinePrefixLength, payloadLength);
        int leadingBytes = headerBytes + inlinePrefixLength + inlinePayloadLength;
        scratch.ensureCapacity(
                leadingBytes,
                1 + (prefix.length > inlinePrefixLength ? 1 : 0)
                        + (payloadLength > inlinePayloadLength ? 1 : 0)
        );
        appendGatherHeaderAndInline(
                scratch,
                frame,
                frameLength,
                headerBytes,
                prefix,
                inlinePrefixLength,
                payload,
                payloadOffset,
                inlinePayloadLength
        );
        if (prefix.length > inlinePrefixLength) {
            scratch.addBuffer(ByteBuffer.wrap(prefix));
        }
        if (payloadLength > inlinePayloadLength) {
            scratch.addBuffer(ByteBuffer.wrap(
                    payload,
                    payloadOffset + inlinePayloadLength,
                    payloadLength - inlinePayloadLength
            ));
        }
        return frameLength + Varint62.length(frameLength);
    }

    static long appendFrame(GatherScratch scratch,
                            FrameCodec.Frame frame,
                            byte[] payloadPrefix,
                            byte[][] payloadParts,
                            int payloadPartIndex,
                            int payloadPartOffset,
                            int payloadLength,
                            Limits limits) throws IOException {
        Limits normalized = limits.normalize();
        byte[] prefix = payloadPrefix == null ? EMPTY_BYTES : payloadPrefix;
        int payloadBufferCount = validatePayloadPartsAndCountBuffers(
                payloadParts,
                payloadPartIndex,
                payloadPartOffset,
                payloadLength
        );
        long encodedPayloadLength = encodedPayloadLength(prefix.length, payloadLength);
        validateOutboundFrame(frame, normalized, encodedPayloadLength, prefix.length > 0 || payloadLength > 0);
        long frameLength = frameLength(frame.streamId(), encodedPayloadLength);
        int headerBytes = gatherHeaderBytes(frameLength, frame.streamId());
        int inlinePrefixLength = inlinePrefixLength(prefix);
        int leadingBytes = headerBytes + inlinePrefixLength;
        scratch.ensureCapacity(
                leadingBytes,
                1 + (prefix.length > inlinePrefixLength ? 1 : 0) + payloadBufferCount
        );
        appendGatherHeaderAndInline(
                scratch,
                frame,
                frameLength,
                headerBytes,
                prefix,
                inlinePrefixLength,
                EMPTY_BYTES,
                0,
                0
        );
        if (prefix.length > inlinePrefixLength) {
            scratch.addBuffer(ByteBuffer.wrap(prefix));
        }
        appendPayloadPartBuffers(scratch, payloadParts, payloadPartIndex, payloadPartOffset, payloadLength);
        return frameLength + Varint62.length(frameLength);
    }

    static void writeGatheredBuffers(GatheringByteChannel output, GatherScratch scratch) throws IOException {
        if (scratch == null || scratch.bufferCount() == 0) {
            return;
        }
        try {
            while (hasRemaining(scratch.buffers(), scratch.bufferCount())) {
                long wrote = output.write(scratch.buffers(), 0, scratch.bufferCount());
                if (wrote <= 0L) {
                    throw FrameCodec.error(ErrorCode.INTERNAL, "write frame", "gathering frame write made no progress");
                }
            }
        } finally {
            scratch.clear();
        }
    }

    private static long inboundPayloadLimit(FrameType frameType, Limits limits) {
        switch (frameType) {
            case DATA:
                return limits.maxFramePayload();
            case MAX_DATA:
            case STOP_SENDING:
            case PING:
            case PONG:
            case BLOCKED:
            case RESET:
            case ABORT:
            case GOAWAY:
            case CLOSE:
                return limits.maxControlPayloadBytes();
            case EXT:
                return limits.maxExtensionPayloadBytes();
            default:
                throw new IllegalStateException("unexpected frame type: " + frameType);
        }
    }

    private static long maxInboundFrameLength(Limits limits) {
        long maxPayload = Math.max(
                limits.maxFramePayload(),
                Math.max(limits.maxControlPayloadBytes(), limits.maxExtensionPayloadBytes())
        );
        if (maxPayload > Protocol.MAX_VARINT62 - MAX_INBOUND_FRAME_HEADER_OVERHEAD) {
            return Protocol.MAX_VARINT62;
        }
        return maxPayload + MAX_INBOUND_FRAME_HEADER_OVERHEAD;
    }

    private static FrameType parseFrameType(int code) throws IOException {
        try {
            return FrameType.fromCode(code);
        } catch (IllegalArgumentException error) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "read frame", "invalid frame type: " + code, error);
        }
    }

    private static void validateFrame(FrameCodec.Frame frame, Limits limits, boolean inbound) throws IOException {
        validateFlags(frame.type(), frame.flags());
        validateFrameScope(frame);
        if (frame.type() == FrameType.DATA) {
            if (inbound && frame.payload().length > limits.maxFramePayload()) {
                throw FrameCodec.error(ErrorCode.FRAME_SIZE, "validate DATA payload", "payload exceeds configured limit");
            }
            if ((frame.flags() & Protocol.FRAME_FLAG_OPEN_METADATA) != 0) {
                FrameCodec.parseDataPayloadView(frame.payload(), frame.flags());
            }
            return;
        }
        validateNonDataFrame(frame, inboundPayloadLimit(frame.type(), limits), frame.payload().length, false);
    }

    static long encodedPayloadLength(int prefixLength, int payloadLength) {
        return Math.max(0L, prefixLength) + Math.max(0L, payloadLength);
    }

    static long frameLength(long streamId, long encodedPayloadLength) throws IOException {
        long overhead = 1L + Varint62.length(streamId);
        if (encodedPayloadLength > Protocol.MAX_VARINT62 - overhead) {
            throw FrameCodec.error(ErrorCode.FRAME_SIZE, "write frame", "frame exceeds varint62 length limit");
        }
        return overhead + encodedPayloadLength;
    }

    private static void validateOutboundFrame(FrameCodec.Frame frame, Limits limits, long encodedPayloadLength, boolean segmented) throws IOException {
        validateFlags(frame.type(), frame.flags());
        validateFrameScope(frame);
        if (frame.type() == FrameType.DATA) {
            if (encodedPayloadLength > limits.maxFramePayload()) {
                throw FrameCodec.error(ErrorCode.FRAME_SIZE, "validate DATA payload", "payload exceeds configured limit");
            }
            if ((frame.flags() & Protocol.FRAME_FLAG_OPEN_METADATA) != 0) {
                FrameCodec.parseDataPayloadView(frame.payload(), frame.flags());
            }
            return;
        }
        validateNonDataFrame(frame, inboundPayloadLimit(frame.type(), limits), encodedPayloadLength, segmented);
    }

    private static void validatePayloadParts(byte[][] payloadParts,
                                             int payloadPartIndex,
                                             int payloadPartOffset,
                                             int payloadLength) {
        validatePayloadPartsAndCountBuffers(payloadParts, payloadPartIndex, payloadPartOffset, payloadLength);
    }

    private static int validatePayloadPartsAndCountBuffers(byte[][] payloadParts,
                                                           int payloadPartIndex,
                                                           int payloadPartOffset,
                                                           int payloadLength) {
        if (payloadLength < 0) {
            throw new IndexOutOfBoundsException("payloadLength < 0");
        }
        if (payloadLength == 0) {
            return 0;
        }
        Objects.requireNonNull(payloadParts, "payloadParts");
        if (payloadPartIndex < 0 || payloadPartIndex >= payloadParts.length) {
            throw new IndexOutOfBoundsException("payloadPartIndex");
        }
        int index = payloadPartIndex;
        int offset = payloadPartOffset;
        int remaining = payloadLength;
        int count = 0;
        while (remaining > 0 && index < payloadParts.length) {
            byte[] part = Objects.requireNonNull(payloadParts[index], "payloadParts[" + index + "]");
            RangeChecks.checkFromIndexSize(offset, 0, part.length);
            if (offset >= part.length) {
                index++;
                offset = 0;
                continue;
            }
            int take = Math.min(part.length - offset, remaining);
            remaining -= take;
            count++;
            index++;
            offset = 0;
        }
        if (remaining != 0) {
            throw new IndexOutOfBoundsException("payload parts underrun");
        }
        return count;
    }

    private static void writePayloadParts(OutputStream output,
                                          byte[][] payloadParts,
                                          int payloadPartIndex,
                                          int payloadPartOffset,
                                          int payloadLength) throws IOException {
        if (payloadLength <= 0) {
            return;
        }
        int index = payloadPartIndex;
        int offset = payloadPartOffset;
        int remaining = payloadLength;
        while (remaining > 0 && index < payloadParts.length) {
            byte[] part = payloadParts[index];
            if (offset >= part.length) {
                index++;
                offset = 0;
                continue;
            }
            int take = Math.min(part.length - offset, remaining);
            output.write(part, offset, take);
            remaining -= take;
            index++;
            offset = 0;
        }
        if (remaining != 0) {
            throw new IndexOutOfBoundsException("payload parts underrun");
        }
    }

    private static int countPayloadPartBuffers(byte[][] payloadParts,
                                               int payloadPartIndex,
                                               int payloadPartOffset,
                                               int payloadLength) {
        if (payloadLength <= 0) {
            return 0;
        }
        int index = payloadPartIndex;
        int offset = payloadPartOffset;
        int remaining = payloadLength;
        int count = 0;
        while (remaining > 0 && index < payloadParts.length) {
            byte[] part = payloadParts[index];
            if (offset >= part.length) {
                index++;
                offset = 0;
                continue;
            }
            int take = Math.min(part.length - offset, remaining);
            if (take > 0) {
                count++;
                remaining -= take;
            }
            index++;
            offset = 0;
        }
        return count;
    }

    private static void appendPayloadPartBuffers(GatherScratch scratch,
                                                 byte[][] payloadParts,
                                                 int payloadPartIndex,
                                                 int payloadPartOffset,
                                                 int payloadLength) {
        int index = payloadPartIndex;
        int offset = payloadPartOffset;
        int remaining = payloadLength;
        while (remaining > 0 && index < payloadParts.length) {
            byte[] part = payloadParts[index];
            if (offset >= part.length) {
                index++;
                offset = 0;
                continue;
            }
            int take = Math.min(part.length - offset, remaining);
            if (take > 0) {
                scratch.addBuffer(ByteBuffer.wrap(part, offset, take));
                remaining -= take;
            }
            index++;
            offset = 0;
        }
        if (remaining != 0) {
            throw new IndexOutOfBoundsException("payload parts underrun");
        }
    }

    private static void appendGatherHeaderAndInline(GatherScratch scratch,
                                                    FrameCodec.Frame frame,
                                                    long frameLength,
                                                    int headerBytes,
                                                    byte[] prefix,
                                                    int inlinePrefixLength,
                                                    byte[] payload,
                                                    int payloadOffset,
                                                    int inlinePayloadLength) throws IOException {
        int leadingBytes = headerBytes + inlinePrefixLength + inlinePayloadLength;
        int headerOffset = scratch.reserveHeader(leadingBytes);
        int cursor = headerOffset;
        byte[] headerArena = scratch.headerArena();
        cursor += Varint62.write(headerArena, cursor, frameLength);
        headerArena[cursor] = (byte) (frame.type().code() | frame.flags());
        cursor += 1;
        cursor += Varint62.write(headerArena, cursor, frame.streamId());
        if (inlinePrefixLength > 0) {
            System.arraycopy(prefix, 0, headerArena, cursor, inlinePrefixLength);
            cursor += inlinePrefixLength;
        }
        if (inlinePayloadLength > 0) {
            System.arraycopy(payload, payloadOffset, headerArena, cursor, inlinePayloadLength);
        }
        scratch.addBuffer(ByteBuffer.wrap(headerArena, headerOffset, leadingBytes));
    }

    private static int inlinePrefixLength(byte[] prefix) {
        int prefixLength = prefix == null ? 0 : prefix.length;
        return prefixLength <= GATHER_INLINE_PREFIX_BYTES ? prefixLength : 0;
    }

    private static int inlinePayloadLength(FrameCodec.Frame frame,
                                           int prefixLength,
                                           int inlinePrefixLength,
                                           int payloadLength) {
        if (payloadLength <= 0 || payloadLength > GATHER_INLINE_CONTROL_PAYLOAD_BYTES) {
            return 0;
        }
        if (prefixLength > 0 && inlinePrefixLength != prefixLength) {
            return 0;
        }
        return frame.type() == FrameType.DATA ? 0 : payloadLength;
    }

    private static Varint62.Decoded readValidatedFrameLength(InputStream input, Limits normalized) throws IOException {
        return validateFrameLength(Varint62.read(input), normalized);
    }

    private static Varint62.Decoded readValidatedFrameLength(FrameCodec.Decoder input, Limits normalized) throws IOException {
        return validateFrameLength(Varint62.read(input), normalized);
    }

    private static Varint62.Decoded validateFrameLength(Varint62.Decoded frameLength, Limits normalized) throws IOException {
        if (frameLength.value() < 2) {
            throw FrameCodec.error(ErrorCode.FRAME_SIZE, "read frame", "frame too short");
        }
        if (frameLength.value() > maxInboundFrameLength(normalized)) {
            throw FrameCodec.error(ErrorCode.FRAME_SIZE, "read frame", "frame exceeds configured limit");
        }
        return frameLength;
    }

    private static long validatedPayloadLength(Limits normalized,
                                               FrameType type,
                                               long frameLength,
                                               int streamIdLength) throws IOException {
        long payloadLength = frameLength - 1 - streamIdLength;
        if (payloadLength < 0) {
            throw FrameCodec.error(ErrorCode.FRAME_SIZE, "read frame", "frame too short");
        }
        long payloadLimit = inboundPayloadLimit(type, normalized);
        if (payloadLength > payloadLimit) {
            throw FrameCodec.error(ErrorCode.FRAME_SIZE, "read frame", "payload exceeds configured limit");
        }
        return payloadLength;
    }

    private static void validateNonDataFrame(FrameCodec.Frame frame,
                                             long payloadLimit,
                                             long actualPayloadLength,
                                             boolean segmented) throws IOException {
        if (segmented) {
            throw FrameCodec.error(ErrorCode.INTERNAL, "write frame", "segmented payloads are only supported for DATA");
        }
        if (actualPayloadLength > payloadLimit) {
            throw FrameCodec.error(ErrorCode.FRAME_SIZE, "validate payload", "payload exceeds configured limit");
        }
        validateNonDataFramePayload(frame);
    }

    private static void validateNonDataFramePayload(FrameCodec.Frame frame) throws IOException {
        switch (frame.type()) {
            case MAX_DATA:
            case BLOCKED: {
                Varint62.Decoded decoded = Varint62.decode(frame.payload(), 0);
                if (decoded.length() != frame.payload().length) {
                    throw FrameCodec.error(ErrorCode.PROTOCOL, "validate " + frame.type(), "unexpected trailing bytes");
                }
                break;
            }
            case PING:
            case PONG:
                if (frame.payload().length < 8) {
                    throw FrameCodec.error(ErrorCode.FRAME_SIZE, "validate ping/pong", "frame too short");
                }
                break;
            case STOP_SENDING:
            case RESET:
            case ABORT:
            case CLOSE:
                FrameCodec.parseErrorPayload(frame.payload());
                break;
            case GOAWAY:
                FrameCodec.parseGoAwayPayload(frame.payload());
                break;
            case EXT:
                if (frame.payload().length == 0) {
                    throw FrameCodec.error(ErrorCode.PROTOCOL, "validate EXT payload", "truncated varint62");
                }
                long extType = Varint62.decode(frame.payload(), 0).value();
                if (extType == Protocol.EXT_PRIORITY_UPDATE) {
                    if (frame.streamId() == 0L) {
                        throw FrameCodec.error(ErrorCode.PROTOCOL, "validate EXT payload", "PRIORITY_UPDATE requires non-zero stream_id");
                    }
                    FrameCodec.parsePriorityUpdatePayload(frame.payload());
                }
                break;
            default:
                break;
        }
    }

    private static boolean hasRemaining(ByteBuffer[] buffers, int length) {
        for (int i = 0; i < length; ++i) {
            ByteBuffer buffer = buffers[i];
            if (buffer != null && buffer.hasRemaining()) {
                return true;
            }
        }
        return false;
    }

    private static void validateFlags(FrameType type, int flags) throws IOException {
        if (type == FrameType.DATA) {
            int allowed = Protocol.FRAME_FLAG_OPEN_METADATA | Protocol.FRAME_FLAG_FIN;
            if ((flags & ~allowed) != 0) {
                throw FrameCodec.error(ErrorCode.PROTOCOL, "validate flags", "invalid flags for frame type");
            }
            return;
        }
        if (flags != 0) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "validate flags", "invalid flags for frame type");
        }
    }

    private static void validateFrameScope(FrameCodec.Frame frame) throws IOException {
        switch (frame.type()) {
            case DATA:
            case STOP_SENDING:
            case RESET:
            case ABORT:
                if (frame.streamId() == 0L) {
                    throw FrameCodec.error(ErrorCode.PROTOCOL, "validate frame scope", frame.type() + " requires non-zero stream_id");
                }
                break;
            case PING:
            case PONG:
            case GOAWAY:
            case CLOSE:
                if (frame.streamId() != 0L) {
                    throw FrameCodec.error(ErrorCode.PROTOCOL, "validate frame scope", frame.type() + " requires stream_id = 0");
                }
                break;
            default:
                break;
        }
    }

    private static int batchScratchRetainLimit(int hint) {
        int normalizedHint = Math.max(MIN_GATHER_SCRATCH_HINT, hint);
        if (normalizedHint > Integer.MAX_VALUE / GATHER_SCRATCH_RETAIN_FACTOR) {
            return Integer.MAX_VALUE;
        }
        return normalizedHint * GATHER_SCRATCH_RETAIN_FACTOR;
    }

    private static boolean batchScratchOversized(int retainedCap, int hint) {
        if (retainedCap <= 0) {
            return false;
        }
        return retainedCap > batchScratchRetainLimit(hint);
    }

    static final class GatherScratch {
        private byte[] headerArena = EMPTY_BYTES;
        private ByteBuffer[] buffers = new ByteBuffer[0];
        private int headerUsed;
        private int bufferCount;

        void reset(int headerCapHint, int bufferCapHint) {
            if (batchScratchOversized(headerArena.length, headerCapHint)) {
                headerArena = EMPTY_BYTES;
            }
            if (batchScratchOversized(buffers.length, bufferCapHint)) {
                buffers = new ByteBuffer[0];
            }
            clear();
        }

        void ensureCapacity(int additionalHeaderBytes, int additionalBuffers) {
            ensureHeaderCapacity(headerUsed + Math.max(0, additionalHeaderBytes));
            ensureBufferCapacity(bufferCount + Math.max(0, additionalBuffers));
        }

        int reserveHeader(int length) {
            int offset = headerUsed;
            headerUsed += Math.max(0, length);
            return offset;
        }

        void addBuffer(ByteBuffer buffer) {
            buffers[bufferCount++] = buffer;
        }

        ByteBuffer[] buffers() {
            return buffers;
        }

        int bufferCount() {
            return bufferCount;
        }

        byte[] headerArena() {
            return headerArena;
        }

        void clear() {
            if (bufferCount > 0) {
                Arrays.fill(buffers, 0, bufferCount, null);
            }
            headerUsed = 0;
            bufferCount = 0;
        }

        private void ensureHeaderCapacity(int required) {
            if (headerArena.length >= required) {
                return;
            }
            int newCapacity = Math.max(required, Math.max(32, headerArena.length * 2));
            headerArena = Arrays.copyOf(headerArena, newCapacity);
        }

        private void ensureBufferCapacity(int required) {
            if (buffers.length >= required) {
                return;
            }
            int newCapacity = Math.max(required, Math.max(16, buffers.length * 2));
            buffers = Arrays.copyOf(buffers, newCapacity);
        }
    }

    static final class InboundFrame {
        private final FrameType type;
        private final int flags;
        private final long streamId;
        private final byte[] payload;
        private InboundPayloadPool.Handle payloadHandle;

        private InboundFrame(FrameType type,
                             int flags,
                             long streamId,
                             byte[] payload,
                             InboundPayloadPool.Handle payloadHandle) {
            this.type = type;
            this.flags = flags;
            this.streamId = streamId;
            this.payload = payload;
            this.payloadHandle = payloadHandle;
        }

        FrameType type() {
            return type;
        }

        int flags() {
            return flags;
        }

        long streamId() {
            return streamId;
        }

        byte[] payload() {
            return payload;
        }

        int storageBytes() {
            return payloadHandle == null ? payload.length : payloadHandle.storageBytes();
        }

        Runnable detachPayloadRelease() {
            if (payloadHandle == null) {
                return null;
            }
            InboundPayloadPool.Handle detached = payloadHandle;
            payloadHandle = null;
            return detached::release;
        }

        FrameCodec.Frame asFrame() {
            return new FrameCodec.Frame(type, flags, streamId, payload);
        }

        void release() {
            if (payloadHandle == null) {
                return;
            }
            InboundPayloadPool.Handle handle = payloadHandle;
            payloadHandle = null;
            handle.release();
        }
    }
}
