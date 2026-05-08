package io.zmux.runtime;

import io.zmux.ErrorCode;
import io.zmux.Settings;
import io.zmux.ZmuxErrorDirection;
import io.zmux.ZmuxErrorScope;
import io.zmux.ZmuxErrorSource;
import io.zmux.ZmuxErrors;
import io.zmux.ZmuxException;
import io.zmux.ZmuxTerminationKind;
import io.zmux.protocol.Frame;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameEnvelopeCodec;
import io.zmux.protocol.Limits;
import io.zmux.protocol.Varint62;
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
    private final BatchMetrics batchMetrics = new BatchMetrics();
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

    private static long outboundPayloadLength(SessionRuntime.OutboundFrame outboundFrame) throws IOException {
        requireOutboundFrame(outboundFrame);
        if (outboundFrame.payloadLength() < 0) {
            throw FrameCodec.error(ErrorCode.INTERNAL, "write batch", "negative outbound payload length");
        }
        int prefixLength = outboundFrame.payloadPrefix() == null ? 0 : outboundFrame.payloadPrefix().length;
        return (long) prefixLength + outboundFrame.payloadLength();
    }

    private static long encodedFrameBytes(SessionRuntime.OutboundFrame outboundFrame, long payloadLength) throws IOException {
        FrameCodec.Frame frame = requireOutboundFrame(outboundFrame);
        long frameLength = FrameEnvelopeCodec.frameLength(frame.streamId(), payloadLength);
        return frameLength + Varint62.length(frameLength);
    }

    private static boolean gatherSegmentDensityOk(long payloadBytes, int segmentCount) {
        return segmentCount > 0 && payloadBytes / segmentCount >= MIN_GATHER_PAYLOAD_BYTES_PER_SEGMENT;
    }

    private static boolean useGatheredBatch(long payloadBytes, int segmentCount) {
        return payloadBytes >= MIN_GATHER_PAYLOAD_BYTES
                && gatherSegmentDensityOk(payloadBytes, segmentCount);
    }

    private static FrameCodec.Frame requireOutboundFrame(SessionRuntime.OutboundFrame outboundFrame) throws IOException {
        if (outboundFrame == null || outboundFrame.frame() == null) {
            throw FrameCodec.error(ErrorCode.INTERNAL, "write batch", "queued outbound frame is missing");
        }
        return outboundFrame.frame();
    }

    private static int gatherSegmentCount(SessionRuntime.OutboundFrame outboundFrame) {
        if (SessionWriterTransport.hasPayloadParts(outboundFrame)) {
            return FrameEnvelopeCodec.gatherBufferCount(
                    outboundFrame.payloadPrefix(),
                    outboundFrame.payloadParts(),
                    outboundFrame.payloadPartIndex(),
                    outboundFrame.payloadPartOffset(),
                    outboundFrame.payloadLength()
            );
        }
        return FrameEnvelopeCodec.gatherBufferCount(
                outboundFrame.payloadPrefix(),
                outboundFrame.payloadLength()
        );
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
        GatheringByteChannel gatheringOutput = this.owner.gatheringOutput();
        boolean gatherCandidate = gatheringOutput != null && gatheringOutput.isOpen();
        BatchMetrics metrics = this.batchMetrics(batch, gatherCandidate);
        if (gatherCandidate) {
            if (metrics.gatherSegments > 0
                    && SessionWriterTransport.useGatheredBatch(metrics.payloadBytes, metrics.gatherSegments)) {
                return this.writeGatheredBatch(output, gatheringOutput, batch, limits, metrics.encodedBytes, metrics.gatherSegments);
            }
        }
        byte[] encoded = this.encodedBatchBuffer(metrics.encodedBytes);
        int cursor = 0;
        try {
            int size = batch.size();
            for (int i = 0; i < size; ++i) {
                SessionRuntime.OutboundFrame outboundFrame = batch.get(i);
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
            if (cursor != metrics.encodedBytes) {
                throw FrameCodec.error(ErrorCode.INTERNAL, "write batch", "encoded batch length mismatch");
            }
            output.write(encoded, 0, cursor);
        } finally {
            this.trimEncodedBatchBuffer();
        }
        output.flush();
        return metrics.encodedBytes;
    }

    private BatchMetrics batchMetrics(List<SessionRuntime.OutboundFrame> batch,
                                      boolean gatherCandidate) throws IOException {
        BatchMetrics metrics = this.batchMetrics;
        metrics.reset();
        long encodedBytes = 0L;
        long payloadBytes = 0L;
        int gatherSegments = 0;
        int size = batch.size();
        for (int i = 0; i < size; ++i) {
            SessionRuntime.OutboundFrame outboundFrame = batch.get(i);
            long payloadLength = SessionWriterTransport.outboundPayloadLength(outboundFrame);
            encodedBytes = SessionRuntime.saturatingAdd(
                    encodedBytes,
                    SessionWriterTransport.encodedFrameBytes(outboundFrame, payloadLength)
            );
            if (encodedBytes > Integer.MAX_VALUE) {
                throw FrameCodec.error(
                        ErrorCode.FRAME_SIZE,
                        "write batch",
                        "encoded batch exceeds Java implementation limit"
                );
            }
            if (!gatherCandidate) {
                continue;
            }
            if (gatherSegments < 0) {
                continue;
            }
            payloadBytes = SessionRuntime.saturatingAdd(payloadBytes, payloadLength);
            int frameSegments = SessionWriterTransport.gatherSegmentCount(outboundFrame);
            if (frameSegments > MAX_GATHER_SEGMENTS - gatherSegments) {
                gatherSegments = -1;
            } else {
                gatherSegments += frameSegments;
            }
        }
        metrics.encodedBytes = (int) encodedBytes;
        metrics.payloadBytes = payloadBytes;
        metrics.gatherSegments = gatherSegments;
        return metrics;
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
            int size = batch.size();
            for (int i = 0; i < size; ++i) {
                SessionRuntime.OutboundFrame outboundFrame = batch.get(i);
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

    private static final class BatchMetrics {
        private int encodedBytes;
        private long payloadBytes;
        private int gatherSegments;

        private void reset() {
            encodedBytes = 0;
            payloadBytes = 0L;
            gatherSegments = 0;
        }
    }
}
