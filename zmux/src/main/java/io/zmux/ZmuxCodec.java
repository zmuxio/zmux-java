package io.zmux;

import io.zmux.internal.FrameCodec;
import io.zmux.internal.Varint62;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class ZmuxCodec {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private ZmuxCodec() {
    }

    public static int varintLength(long value) throws IOException {
        return Varint62.length(value);
    }

    public static void writeVarint(OutputStream output, long value) throws IOException {
        Varint62.write(Objects.requireNonNull(output, "output"), value);
    }

    public static byte[] appendVarint(byte[] prefix, long value) throws IOException {
        int prefixLength = prefix == null ? 0 : prefix.length;
        int valueLength = varintLength(value);
        byte[] output = new byte[checkedArrayLength("append varint", prefixLength, valueLength)];
        int offset = copyPrefix(output, prefix, prefixLength);
        Varint62.write(output, offset, value);
        return output;
    }

    public static byte[] encodeVarint(long value) throws IOException {
        return Varint62.encode(value);
    }

    public static DecodedVarint parseVarint(byte[] source) throws IOException {
        return parseVarint(source, 0);
    }

    public static DecodedVarint parseVarint(byte[] source, int offset) throws IOException {
        Varint62.Decoded decoded = Varint62.decode(Objects.requireNonNull(source, "source"), offset);
        return new DecodedVarint(decoded.value(), decoded.length());
    }

    public static DecodedVarint readVarint(InputStream input) throws IOException {
        Varint62.Decoded decoded = Varint62.read(Objects.requireNonNull(input, "input"));
        return new DecodedVarint(decoded.value(), decoded.length());
    }

    public static void writeTlv(ByteArrayOutputStream output, long type, byte[] value) throws IOException {
        FrameCodec.appendTlv(Objects.requireNonNull(output, "output"), type, value == null ? EMPTY_BYTES : value);
    }

    public static byte[] appendTlv(byte[] prefix, long type, byte[] value) throws IOException {
        byte[] effectiveValue = value == null ? EMPTY_BYTES : value;
        int prefixLength = prefix == null ? 0 : prefix.length;
        int typeLength = varintLength(type);
        int lengthLength = varintLength(effectiveValue.length);
        int outputLength = checkedArrayLength("append tlv", prefixLength, (long) typeLength + lengthLength + effectiveValue.length);
        byte[] output = new byte[outputLength];
        int offset = copyPrefix(output, prefix, prefixLength);
        offset += Varint62.write(output, offset, type);
        offset += Varint62.write(output, offset, effectiveValue.length);
        if (effectiveValue.length > 0) {
            System.arraycopy(effectiveValue, 0, output, offset, effectiveValue.length);
        }
        return output;
    }

    public static List<Tlv> parseTlvs(byte[] source) throws IOException {
        return Collections.unmodifiableList(FrameCodec.parseTlvs(source == null ? EMPTY_BYTES : source));
    }

    public static ParsedFrame parseFrame(byte[] source, Limits limits) throws IOException {
        Objects.requireNonNull(source, "source");
        ByteArrayInputStream input = new ByteArrayInputStream(source);
        Frame frame = fromInternal(FrameCodec.readFrame(input, effectiveLimits(limits)));
        return new ParsedFrame(frame, source.length - input.available());
    }

    public static Frame readFrame(InputStream input, Limits limits) throws IOException {
        return fromInternal(FrameCodec.readFrame(Objects.requireNonNull(input, "input"), effectiveLimits(limits)));
    }

    public static void writeFrame(OutputStream output, Frame frame, Limits limits) throws IOException {
        FrameCodec.writeFrame(Objects.requireNonNull(output, "output"), toInternal(frame), effectiveLimits(limits));
    }

    public static Preface parsePreface(byte[] source) throws IOException {
        Objects.requireNonNull(source, "source");
        ByteArrayInputStream input = new ByteArrayInputStream(source);
        Preface preface = readPreface(input);
        if (input.available() != 0) {
            throw new ZmuxException(
                    ErrorCode.PROTOCOL.code(),
                    "parse preface",
                    "unexpected trailing bytes after preface",
                    ZmuxErrorScope.SESSION,
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ,
                    ZmuxTerminationKind.UNKNOWN
            );
        }
        return preface;
    }

    public static Preface readPreface(InputStream input) throws IOException {
        return FrameCodec.readPreface(Objects.requireNonNull(input, "input"));
    }

    public static void writePreface(OutputStream output, Preface preface) throws IOException {
        FrameCodec.writePreface(Objects.requireNonNull(output, "output"), Objects.requireNonNull(preface, "preface"));
    }

    public static Negotiated negotiatePrefaces(Preface local, Preface peer) throws IOException {
        return FrameCodec.negotiate(Objects.requireNonNull(local, "local"), Objects.requireNonNull(peer, "peer"));
    }

    private static Limits effectiveLimits(Limits limits) {
        return limits == null ? Settings.defaults().limits() : limits;
    }

    private static int copyPrefix(byte[] output, byte[] prefix, int prefixLength) {
        if (prefixLength > 0) {
            System.arraycopy(prefix, 0, output, 0, prefixLength);
        }
        return prefixLength;
    }

    private static int checkedArrayLength(String operation, int prefixLength, long suffixLength) throws IOException {
        long outputLength = (long) prefixLength + suffixLength;
        if (outputLength > Integer.MAX_VALUE) {
            throw new ZmuxException(
                    ErrorCode.PROTOCOL.code(),
                    operation,
                    "encoded output exceeds maximum Java array length",
                    ZmuxErrorScope.SESSION,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    ZmuxTerminationKind.UNKNOWN
            );
        }
        return (int) outputLength;
    }

    private static Frame fromInternal(FrameCodec.Frame frame) {
        return new Frame(frame.type(), frame.flags(), frame.streamId(), frame.payload());
    }

    private static FrameCodec.Frame toInternal(Frame frame) {
        Objects.requireNonNull(frame, "frame");
        return new FrameCodec.Frame(frame.type(), frame.flags(), frame.streamId(), frame.payloadView());
    }
}
