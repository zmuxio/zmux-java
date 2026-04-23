package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.GatheringByteChannel;
import java.util.List;
import java.util.Objects;

final class SessionWriterTransport {
    private final Owner owner;
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

    private static int safeVarintLength(long value) {
        try {
            return Varint62.length(value);
        } catch (ZmuxException invalid) {
            return 8;
        }
    }

    private static int saturatingAddInt(int current, int addend) {
        if (addend <= 0) {
            return current;
        }
        if (current >= Integer.MAX_VALUE - addend) {
            return Integer.MAX_VALUE;
        }
        return current + addend;
    }

    long writeBatch(List<SessionRuntime.OutboundFrame> batch) throws IOException {
        try {
            return this.writeBatchInternal(batch);
        } catch (IOException error) {
            throw SessionWriterTransport.transportWriteFailure(error);
        }
    }

    private long writeBatchInternal(List<SessionRuntime.OutboundFrame> batch) throws IOException {
        long batchBytes = 0L;
        OutputStream output = this.owner.output();
        GatheringByteChannel gatheringOutput = this.owner.gatheringOutput();
        Limits limits = this.owner.limits();
        if (gatheringOutput != null) {
            output.flush();
            int bufferCountHint = 0;
            int headerBytesHint = 0;
            for (SessionRuntime.OutboundFrame outboundFrame : batch) {
                long payloadLength = SessionWriterTransport.outboundPayloadLength(outboundFrame);
                long frameLength = SessionRuntime.saturatingAdd(
                        1L + SessionWriterTransport.safeVarintLength(outboundFrame.frame().streamId()),
                        payloadLength
                );
                int frameHeaderBytes = FrameEnvelopeCodec.gatherHeaderBytes(frameLength, outboundFrame.frame().streamId());
                if (SessionWriterTransport.hasPayloadParts(outboundFrame)) {
                    headerBytesHint = SessionWriterTransport.saturatingAddInt(
                            headerBytesHint,
                            frameHeaderBytes + FrameEnvelopeCodec.gatherInlineBytes(
                                    outboundFrame.frame(),
                                    outboundFrame.payloadPrefix(),
                                    0
                            )
                    );
                    bufferCountHint = SessionWriterTransport.saturatingAddInt(
                            bufferCountHint,
                            FrameEnvelopeCodec.gatherBufferCount(
                                    outboundFrame.payloadPrefix(),
                                    outboundFrame.payloadParts(),
                                    outboundFrame.payloadPartIndex(),
                                    outboundFrame.payloadPartOffset(),
                                    outboundFrame.payloadLength()
                            )
                    );
                } else {
                    headerBytesHint = SessionWriterTransport.saturatingAddInt(
                            headerBytesHint,
                            frameHeaderBytes + FrameEnvelopeCodec.gatherInlineBytes(
                                    outboundFrame.frame(),
                                    outboundFrame.payloadPrefix(),
                                    outboundFrame.payloadLength()
                            )
                    );
                    bufferCountHint = SessionWriterTransport.saturatingAddInt(
                            bufferCountHint,
                            FrameEnvelopeCodec.gatherBufferCount(outboundFrame.payloadPrefix(), outboundFrame.payloadLength())
                    );
                }
            }
            this.gatherScratch.reset(headerBytesHint, bufferCountHint);
            for (SessionRuntime.OutboundFrame outboundFrame : batch) {
                if (SessionWriterTransport.hasPayloadParts(outboundFrame)) {
                    batchBytes = SessionRuntime.saturatingAdd(batchBytes, FrameEnvelopeCodec.appendFrame(
                            this.gatherScratch,
                            outboundFrame.frame(),
                            outboundFrame.payloadPrefix(),
                            outboundFrame.payloadParts(),
                            outboundFrame.payloadPartIndex(),
                            outboundFrame.payloadPartOffset(),
                            outboundFrame.payloadLength(),
                            limits
                    ));
                } else {
                    batchBytes = SessionRuntime.saturatingAdd(batchBytes, FrameEnvelopeCodec.appendFrame(
                            this.gatherScratch,
                            outboundFrame.frame(),
                            outboundFrame.payloadPrefix(),
                            outboundFrame.payloadBytes(),
                            outboundFrame.payloadOffset(),
                            outboundFrame.payloadLength(),
                            limits
                    ));
                }
            }
            FrameEnvelopeCodec.writeGatheredBuffers(gatheringOutput, this.gatherScratch);
        } else {
            for (SessionRuntime.OutboundFrame outboundFrame : batch) {
                if (SessionWriterTransport.hasPayloadParts(outboundFrame)) {
                    FrameCodec.writeFrame(
                            output,
                            outboundFrame.frame(),
                            outboundFrame.payloadPrefix(),
                            outboundFrame.payloadParts(),
                            outboundFrame.payloadPartIndex(),
                            outboundFrame.payloadPartOffset(),
                            outboundFrame.payloadLength(),
                            limits
                    );
                } else {
                    FrameCodec.writeFrame(
                            output,
                            outboundFrame.frame(),
                            outboundFrame.payloadPrefix(),
                            outboundFrame.payloadBytes(),
                            outboundFrame.payloadOffset(),
                            outboundFrame.payloadLength(),
                            limits
                    );
                }
                batchBytes = SessionRuntime.saturatingAdd(batchBytes, SessionWriterTransport.encodedFrameBytes(outboundFrame));
            }
        }
        output.flush();
        return batchBytes;
    }

    interface Owner {
        OutputStream output();

        GatheringByteChannel gatheringOutput();

        Limits limits();
    }
}
