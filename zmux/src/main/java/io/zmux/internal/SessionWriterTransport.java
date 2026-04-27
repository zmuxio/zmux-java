package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.GatheringByteChannel;
import java.util.List;
import java.util.Objects;

final class SessionWriterTransport {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final int MAX_RETAINED_ENCODED_BATCH_BYTES = 1 << 20;
    private static final int MIN_GATHER_BATCH_PAYLOAD_BYTES = 16 << 10;
    private static final int MIN_GATHER_PAYLOAD_BYTES_PER_BUFFER = 1024;
    private static final int MAX_GATHER_BATCH_BUFFERS = 64;
    private final Owner owner;
    private byte[] encodedBatchScratch = EMPTY_BYTES;
    private final FrameEnvelopeCodec.GatherScratch gatherScratch = new FrameEnvelopeCodec.GatherScratch();

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

    private static GatherPlan gatherPlan(List<SessionRuntime.OutboundFrame> batch) throws IOException {
        int headerBytes = 0;
        int bufferCount = 0;
        long payloadBytes = 0L;
        for (SessionRuntime.OutboundFrame outboundFrame : batch) {
            int payloadLength = Math.max(0, outboundFrame.payloadLength());
            byte[] prefix = outboundFrame.payloadPrefix();
            long encodedPayloadLength = SessionWriterTransport.outboundPayloadLength(outboundFrame);
            payloadBytes = SessionRuntime.saturatingAdd(payloadBytes, encodedPayloadLength);
            long frameLength = FrameEnvelopeCodec.frameLength(outboundFrame.frame().streamId(), encodedPayloadLength);
            int frameHeaderBytes = FrameEnvelopeCodec.gatherHeaderBytes(frameLength, outboundFrame.frame().streamId());
            int frameInlineBytes = FrameEnvelopeCodec.gatherInlineBytes(outboundFrame.frame(), prefix, payloadLength);
            headerBytes = SessionWriterTransport.saturatingIntAdd(
                    headerBytes,
                    SessionWriterTransport.saturatingIntAdd(frameHeaderBytes, frameInlineBytes)
            );
            int frameBufferCount = SessionWriterTransport.hasPayloadParts(outboundFrame)
                    ? FrameEnvelopeCodec.gatherBufferCount(
                            prefix,
                            outboundFrame.payloadParts(),
                            outboundFrame.payloadPartIndex(),
                            outboundFrame.payloadPartOffset(),
                            payloadLength
                    )
                    : FrameEnvelopeCodec.gatherBufferCount(prefix, payloadLength);
            bufferCount = SessionWriterTransport.saturatingIntAdd(bufferCount, frameBufferCount);
        }
        return new GatherPlan(headerBytes, bufferCount, payloadBytes);
    }

    private static int safeVarintLength(long value) {
        try {
            return Varint62.length(value);
        } catch (ZmuxException invalid) {
            return 8;
        }
    }

    private static int saturatingIntAdd(int left, int right) {
        if (right <= 0) {
            return left;
        }
        if (left > Integer.MAX_VALUE - right) {
            return Integer.MAX_VALUE;
        }
        return left + right;
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
        GatheringByteChannel gatheringOutput = this.owner.gatheringOutput();
        if (gatheringOutput != null && gatheringOutput.isOpen()) {
            Long gatheredBytes = this.tryWriteGatheredBatch(gatheringOutput, batch, limits);
            if (gatheredBytes != null) {
                output.flush();
                return gatheredBytes;
            }
        }
        return this.writeEncodedBatch(output, batch, limits);
    }

    private Long tryWriteGatheredBatch(GatheringByteChannel output,
                                       List<SessionRuntime.OutboundFrame> batch,
                                       Limits limits) throws IOException {
        GatherPlan plan = SessionWriterTransport.gatherPlan(batch);
        if (!plan.shouldGather()) {
            return null;
        }
        long batchBytes = 0L;
        this.gatherScratch.reset(plan.headerBytes(), plan.bufferCount());
        try {
            for (SessionRuntime.OutboundFrame outboundFrame : batch) {
                if (SessionWriterTransport.hasPayloadParts(outboundFrame)) {
                    batchBytes = SessionRuntime.saturatingAdd(
                            batchBytes,
                            FrameEnvelopeCodec.appendFrame(
                                    this.gatherScratch,
                                    outboundFrame.frame(),
                                    outboundFrame.payloadPrefix(),
                                    outboundFrame.payloadParts(),
                                    outboundFrame.payloadPartIndex(),
                                    outboundFrame.payloadPartOffset(),
                                    outboundFrame.payloadLength(),
                                    limits
                            )
                    );
                } else {
                    batchBytes = SessionRuntime.saturatingAdd(
                            batchBytes,
                            FrameEnvelopeCodec.appendFrame(
                                    this.gatherScratch,
                                    outboundFrame.frame(),
                                    outboundFrame.payloadPrefix(),
                                    outboundFrame.payloadBytes(),
                                    outboundFrame.payloadOffset(),
                                    outboundFrame.payloadLength(),
                                    limits
                            )
                    );
                }
            }
            FrameEnvelopeCodec.writeGatheredBuffers(output, this.gatherScratch);
            return batchBytes;
        } finally {
            this.gatherScratch.clear();
        }
    }

    private long writeEncodedBatch(OutputStream output,
                                   List<SessionRuntime.OutboundFrame> batch,
                                   Limits limits) throws IOException {
        int batchBytes = SessionWriterTransport.encodedBatchBytes(batch);
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

    interface Owner {
        OutputStream output();

        GatheringByteChannel gatheringOutput();

        Limits limits();
    }

    private static final class GatherPlan {
        private final int headerBytes;
        private final int bufferCount;
        private final long payloadBytes;

        private GatherPlan(int headerBytes, int bufferCount, long payloadBytes) {
            this.headerBytes = Math.max(0, headerBytes);
            this.bufferCount = Math.max(0, bufferCount);
            this.payloadBytes = Math.max(0L, payloadBytes);
        }

        int headerBytes() {
            return headerBytes;
        }

        int bufferCount() {
            return bufferCount;
        }

        boolean shouldGather() {
            return payloadBytes >= MIN_GATHER_BATCH_PAYLOAD_BYTES
                    && bufferCount > 0
                    && bufferCount <= MAX_GATHER_BATCH_BUFFERS
                    && payloadBytes / bufferCount >= MIN_GATHER_PAYLOAD_BYTES_PER_BUFFER;
        }
    }
}
