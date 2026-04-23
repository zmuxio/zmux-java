package io.zmux.internal;

import io.zmux.FrameType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class SessionWriterBatchCollector {
    private final SessionWriterCoordinator.Owner owner;
    private final SessionWriterBatchOrderer batchOrderer;
    private final ArrayList<SessionRuntime.OutboundFrame> collectBatch;
    private final int maxBatchFrames;

    SessionWriterBatchCollector(SessionWriterCoordinator.Owner owner,
                                SessionWriterBatchOrderer batchOrderer,
                                ArrayList<SessionRuntime.OutboundFrame> collectBatch,
                                int maxBatchFrames) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.batchOrderer = Objects.requireNonNull(batchOrderer, "batchOrderer");
        this.collectBatch = Objects.requireNonNull(collectBatch, "collectBatch");
        this.maxBatchFrames = maxBatchFrames;
    }

    SessionRuntime.ReadyBatch collectReadyBatchStateLocked(boolean orderOrdinary, boolean trackWriterHeld)
            throws IOException {
        this.owner.expireStopSendingGracefulDrainsLocked();

        this.collectBatch.clear();
        this.drainUrgentBatchLocked(this.collectBatch, trackWriterHeld);
        if (!this.collectBatch.isEmpty()) {
            return new SessionRuntime.ReadyBatch(this.collectBatch, false, 0L, 0L);
        }

        long costLimit = this.ordinaryBatchCostLimitLocked();
        long batchCost = this.drainOrdinaryBatchLocked(this.collectBatch, costLimit, 0L, trackWriterHeld);
        List<SessionRuntime.OutboundFrame> batch = orderOrdinary
                ? this.batchOrderer.orderOrdinary(this.collectBatch)
                : this.collectBatch;
        return new SessionRuntime.ReadyBatch(batch, !batch.isEmpty(), batchCost, costLimit);
    }

    long drainOrdinaryBatchLocked(List<SessionRuntime.OutboundFrame> batch,
                                  long costLimit,
                                  long batchCost,
                                  boolean trackWriterHeld) throws IOException {
        long effectiveCostLimit = costLimit > 0L ? costLimit : Long.MAX_VALUE;
        while (batch.size() < this.maxBatchFrames && batchCost < effectiveCostLimit) {
            boolean progressed = false;

            SessionRuntime.OutboundFrame advisoryBefore = this.pollAdvisoryOutboundLocked();
            if (advisoryBefore != null) {
                this.owner.addBatchFrameLocked(batch, advisoryBefore, trackWriterHeld);
                batchCost = SessionRuntime.saturatingAdd(batchCost, SessionWriterBatchPolicy.outboundBatchCost(advisoryBefore));
                progressed = true;
                if (advisoryBefore.frame().type() == FrameType.CLOSE
                        || batch.size() >= this.maxBatchFrames
                        || batchCost >= effectiveCostLimit) {
                    return batchCost;
                }
            }

            SessionRuntime.OutboundFrame data = this.owner.pollQueuedOutboundLocked(this.owner.dataQueue());
            if (data != null) {
                this.owner.addBatchFrameLocked(batch, data, trackWriterHeld);
                batchCost = SessionRuntime.saturatingAdd(batchCost, SessionWriterBatchPolicy.outboundBatchCost(data));
                progressed = true;
                if (data.frame().type() == FrameType.CLOSE
                        || batch.size() >= this.maxBatchFrames
                        || batchCost >= effectiveCostLimit) {
                    return batchCost;
                }
            }

            SessionRuntime.OutboundFrame advisoryAfter = this.pollAdvisoryOutboundLocked();
            if (advisoryAfter != null) {
                this.owner.addBatchFrameLocked(batch, advisoryAfter, trackWriterHeld);
                batchCost = SessionRuntime.saturatingAdd(batchCost, SessionWriterBatchPolicy.outboundBatchCost(advisoryAfter));
                progressed = true;
                if (advisoryAfter.frame().type() == FrameType.CLOSE
                        || batch.size() >= this.maxBatchFrames
                        || batchCost >= effectiveCostLimit) {
                    return batchCost;
                }
            }

            if (!progressed) {
                return batchCost;
            }
        }
        return batchCost;
    }

    void clearRetainedBatchRefs() {
        this.collectBatch.clear();
        this.batchOrderer.clearRetainedBatchRefs();
    }

    private void drainUrgentBatchLocked(ArrayList<SessionRuntime.OutboundFrame> batch, boolean trackWriterHeld) {
        while (batch.size() < this.maxBatchFrames) {
            SessionRuntime.OutboundFrame outboundFrame = this.owner.pollQueuedOutboundLocked(this.owner.urgentQueue());
            if (outboundFrame == null) {
                break;
            }
            this.owner.addBatchFrameLocked(batch, outboundFrame, trackWriterHeld);
        }
        this.batchOrderer.orderUrgent(batch);
    }

    private long ordinaryBatchCostLimitLocked() {
        return SessionWriterBatchPolicy.ordinaryBatchCostLimit(
                this.owner.peerSettings(),
                this.owner.sendRateEstimateLocked(),
                this.maxBatchFrames
        );
    }

    private SessionRuntime.OutboundFrame pollAdvisoryOutboundLocked() throws IOException {
        while (true) {
            StreamRuntime streamRuntime = this.owner.advisoryQueue().pollFirst();
            if (streamRuntime == null) {
                return null;
            }

            streamRuntime.clearPriorityUpdateQueuedLocked();
            if (!streamRuntime.hasPendingPriorityUpdateLocked()) {
                continue;
            }
            SessionRuntime.OutboundFrame outboundFrame = this.owner.takePendingPriorityUpdateForBatchLocked(streamRuntime);
            if (outboundFrame != null) {
                return outboundFrame;
            }
        }
    }
}
