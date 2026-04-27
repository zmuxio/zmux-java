package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.GatheringByteChannel;
import java.util.List;
import java.util.Objects;

final class SessionWriterTransport {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final int MAX_BATCH_FRAMES = 32;
    private static final int MIN_GATHER_PAYLOAD_BYTES = 16 << 10;
    private static final int MAX_GATHER_SEGMENTS = 64;
    private static final int MIN_GATHER_PAYLOAD_BYTES_PER_SEGMENT = 1024;
    private static final int MAX_RETAINED_ENCODED_BATCH_BYTES =
            (int) Math.min(Integer.MAX_VALUE, MAX_BATCH_FRAMES * Settings.defaults().maxFramePayload());
    private final Owner owner;
    private byte[] encodedBatchScratch = EMPTY_BYTES;
    private FrameEnvelopeCodec.GatherScratch gatherScratch;

    SessionWriterTransport(Owner owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    private static IOException transportWriteFailure(IOException error) {
        if (ZmuxErrors.details(error) != null) {
            return error;
        }
        return new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "write",
                "zmux: transport write failed",
                error,
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.TRANSPORT,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    private static boolean hasPayloadParts(SessionRuntime.OutboundFrame outboundFrame) {
        return outboundFrame.payloadParts() != null
                && outboundFrame.payloadParts().length > 0
                && outboundFrame.payloadLength() > 0;
    }

    private static long outboundPayloadLength(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return 0L;
        }
        int prefixLength = outboundFrame.payloadPrefix() == null ? 0 : outboundFrame.payloadPrefix().length;
        return SessionRuntime.saturatingAdd(prefixLength, Math.max(0, outboundFrame.payloadLength()));
    }

    private static long encodedFrameBytes(SessionRuntime.OutboundFrame outboundFrame) {
        if (outboundFrame == null) {
            return 0L;
        }
        long payloadLength = SessionWriterTransport.outboundPayloadLength(outboundFrame);
        long frameLength = SessionRuntime.saturatingAdd(
                1L + SessionWriterTransport.safeVarintLength(outboundFrame.frame().streamId()),
                payloadLength
        );
        return SessionRuntime.saturatingAdd(frameLength, SessionWriterTransport.safeVarintLength(frameLength));
    }

    private static int encodedBatchBytes(List<SessionRuntime.OutboundFrame> batch) throws IOException {
        long total = 0L;
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            total = SessionRuntime.saturatingAdd(total, SessionWriterTransport.encodedFrameBytes(outboundFrame));
            if (total > Integer.MAX_VALUE) {
                throw FrameCodec.error(
                        ErrorCode.FRAME_SIZE,
                        "write batch",
                        "encoded batch exceeds Java implementation limit"
                );
            }
        }
        return (int) total;
    }

    private static long batchPayloadBytes(List<SessionRuntime.OutboundFrame> batch) {
        long total = 0L;
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            total = SessionRuntime.saturatingAdd(total, SessionWriterTransport.outboundPayloadLength(outboundFrame));
        }
        return total;
    }

    private static int gatherSegmentCount(List<SessionRuntime.OutboundFrame> batch, int maxSegments) {
        if (maxSegments < 0) {
            return -1;
        }
        int segments = 0;
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            int frameSegments;
            if (SessionWriterTransport.hasPayloadParts(outboundFrame)) {
                frameSegments = FrameEnvelopeCodec.gatherBufferCount(
                        outboundFrame.payloadPrefix(),
                        outboundFrame.payloadParts(),
                        outboundFrame.payloadPartIndex(),
                        outboundFrame.payloadPartOffset(),
                        outboundFrame.payloadLength()
                );
            } else {
                frameSegments = FrameEnvelopeCodec.gatherBufferCount(
                        outboundFrame.payloadPrefix(),
                        outboundFrame.payloadLength()
                );
            }
            if (frameSegments > maxSegments - segments) {
                return -1;
            }
            segments += frameSegments;
        }
        return segments;
    }

    private static boolean gatherSegmentDensityOk(long payloadBytes, int segmentCount) {
        return segmentCount > 0 && payloadBytes / segmentCount >= MIN_GATHER_PAYLOAD_BYTES_PER_SEGMENT;
    }

    private static boolean useGatheredBatch(long payloadBytes, int segmentCount) {
        return payloadBytes >= MIN_GATHER_PAYLOAD_BYTES
                && gatherSegmentDensityOk(payloadBytes, segmentCount);
    }

    private static int safeVarintLength(long value) {
        try {
            return Varint62.length(value);
        } catch (ZmuxException invalid) {
            return 8;
        }
    }

    long writeBatch(List<SessionRuntime.OutboundFrame> batch) throws IOException {
        try {
            return this.writeBatchInternal(batch);
        } catch (IOException error) {
            throw SessionWriterTransport.transportWriteFailure(error);
        }
    }

    private long writeBatchInternal(List<SessionRuntime.OutboundFrame> batch) throws IOException {
        OutputStream output = this.owner.output();
        Limits limits = this.owner.limits();
        if (batch.isEmpty()) {
            return 0L;
        }
        return this.writeEncodedBatch(output, batch, limits);
    }

    private long writeEncodedBatch(OutputStream output,
                                   List<SessionRuntime.OutboundFrame> batch,
                                   Limits limits) throws IOException {
        int batchBytes = SessionWriterTransport.encodedBatchBytes(batch);
        GatheringByteChannel gatheringOutput = this.owner.gatheringOutput();
        if (gatheringOutput != null && gatheringOutput.isOpen()) {
            int gatherSegments = SessionWriterTransport.gatherSegmentCount(batch, MAX_GATHER_SEGMENTS);
            if (gatherSegments > 0 && SessionWriterTransport.useGatheredBatch(
                    SessionWriterTransport.batchPayloadBytes(batch),
                    gatherSegments
            )) {
                return this.writeGatheredBatch(output, gatheringOutput, batch, limits, batchBytes, gatherSegments);
            }
        }
        byte[] encoded = this.encodedBatchBuffer(batchBytes);
        int cursor = 0;
        try {
            for (SessionRuntime.OutboundFrame outboundFrame : batch) {
                if (SessionWriterTransport.hasPayloadParts(outboundFrame)) {
                    cursor += FrameEnvelopeCodec.appendFrame(
                            encoded,
                            cursor,
                            outboundFrame.frame(),
                            outboundFrame.payloadPrefix(),
                            outboundFrame.payloadParts(),
                            outboundFrame.payloadPartIndex(),
                            outboundFrame.payloadPartOffset(),
                            outboundFrame.payloadLength(),
                            limits
                    );
                } else {
                    cursor += FrameEnvelopeCodec.appendFrame(
                            encoded,
                            cursor,
                            outboundFrame.frame(),
                            outboundFrame.payloadPrefix(),
                            outboundFrame.payloadBytes(),
                            outboundFrame.payloadOffset(),
                            outboundFrame.payloadLength(),
                            limits
                    );
                }
            }
            if (cursor != batchBytes) {
                throw FrameCodec.error(ErrorCode.INTERNAL, "write batch", "encoded batch length mismatch");
            }
            output.write(encoded, 0, cursor);
        } finally {
            this.trimEncodedBatchBuffer();
        }
        output.flush();
        return batchBytes;
    }

    private long writeGatheredBatch(OutputStream output,
                                    GatheringByteChannel gatheringOutput,
                                    List<SessionRuntime.OutboundFrame> batch,
                                    Limits limits,
                                    int batchBytes,
                                    int gatherSegments) throws IOException {
        FrameEnvelopeCodec.GatherScratch scratch = this.gatherScratch();
        scratch.reset(Math.min(batchBytes, MAX_RETAINED_ENCODED_BATCH_BYTES), gatherSegments);
        boolean handingOffScratch = false;
        try {
            long encoded = 0L;
            for (SessionRuntime.OutboundFrame outboundFrame : batch) {
                if (SessionWriterTransport.hasPayloadParts(outboundFrame)) {
                    encoded = SessionRuntime.saturatingAdd(encoded, FrameEnvelopeCodec.appendFrame(
                            scratch,
                            outboundFrame.frame(),
                            outboundFrame.payloadPrefix(),
                            outboundFrame.payloadParts(),
                            outboundFrame.payloadPartIndex(),
                            outboundFrame.payloadPartOffset(),
                            outboundFrame.payloadLength(),
                            limits
                    ));
                } else {
                    encoded = SessionRuntime.saturatingAdd(encoded, FrameEnvelopeCodec.appendFrame(
                            scratch,
                            outboundFrame.frame(),
                            outboundFrame.payloadPrefix(),
                            outboundFrame.payloadBytes(),
                            outboundFrame.payloadOffset(),
                            outboundFrame.payloadLength(),
                            limits
                    ));
                }
            }
            if (encoded != batchBytes) {
                throw FrameCodec.error(ErrorCode.INTERNAL, "write batch", "gathered batch length mismatch");
            }
            handingOffScratch = true;
            FrameEnvelopeCodec.writeGatheredBuffers(gatheringOutput, scratch);
        } finally {
            if (!handingOffScratch) {
                scratch.clear();
            }
        }
        output.flush();
        return batchBytes;
    }

    private byte[] encodedBatchBuffer(int length) {
        if (encodedBatchScratch.length < length) {
            encodedBatchScratch = new byte[length];
        }
        return encodedBatchScratch;
    }

    private void trimEncodedBatchBuffer() {
        if (encodedBatchScratch.length > MAX_RETAINED_ENCODED_BATCH_BYTES) {
            encodedBatchScratch = EMPTY_BYTES;
        }
    }

    private FrameEnvelopeCodec.GatherScratch gatherScratch() {
        if (gatherScratch == null) {
            gatherScratch = new FrameEnvelopeCodec.GatherScratch();
        }
        return gatherScratch;
    }

    interface Owner {
        OutputStream output();

        GatheringByteChannel gatheringOutput();

        Limits limits();
    }
}
