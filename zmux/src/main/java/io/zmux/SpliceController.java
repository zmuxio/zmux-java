package io.zmux;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/** A running splice with one set of controls for each forwarding direction. */
public final class SpliceController {
    /** Why a unit was refused before admission. */
    public enum Rejection { INVALID_PARAMETER, SPLICE_BUDGET, PAUSED, CLOSED, CANCELLED }

    /** An injection first receives Accepted or Rejected. Only accepted units acquire an identity. */
    public sealed interface Receipt permits Accepted, Rejected, Submitted, Dropped, DirectionEnded {}
    /** The identity assigned to an admitted injection. */
    public record Accepted(long generation, long sequence) implements Receipt {}
    /** An injection that failed admission. */
    public record Rejected(Rejection reason) implements Receipt {}
    /** The accepted prefix and number of items handed to the target. */
    public record Submitted(Accepted identity, long acceptedBytes, int enqueuedItems) implements Receipt {}
    /** An admitted injection discarded before submission. */
    public record Dropped(Accepted identity, SpliceDirection.Drop reason) implements Receipt {}
    /** The final disposition of an injection when the direction ended. */
    public record DirectionEnded(Accepted identity, SpliceEnd reason, long acceptedBytes, int enqueuedItems) implements Receipt {}
    /** The independent admission and final disposition of an injection. */
    public record Injection(CompletionStage<Receipt> admission, CompletionStage<Receipt> disposition) {}

    /** A committed controller state change. */
    public sealed interface Event permits HooksReplaced, PauseAcknowledged, DirectionFinished {}
    /** The generation whose hooks are now installed. */
    public record HooksReplaced(long generation) implements Event {}
    /** The token of the pause acknowledged by the worker. */
    public record PauseAcknowledged(long token) implements Event {}
    /** The final reason the direction stopped. */
    public record DirectionFinished(SpliceEnd reason) implements Event {}

    /** Commands addressed to one forwarding direction. */
    public static final class Direction {
        private final SplicePolicy policy;

        Direction(SplicePolicy policy) { this.policy = policy; }

        /** Reports the reason this direction stopped submitting reliable data. */
        public CompletionStage<SpliceEnd> finished() { return policy.finished(); }

        /** Reads the current counters and the terminal decision, if one has been committed. */
        public CompletionStage<SpliceDirection> snapshot() { return policy.snapshot.get(); }

        public SpliceSettlement settlement() { return policy.settlement; }

        /** Installs a new chain at the next admission boundary and reports its generation. */
        public CompletionStage<Long> replaceHooks(SpliceHooks hooks) { return policy.replaceHooks(hooks); }

        /** Confirms a pause when the reliable writer reaches a safe boundary. */
        public CompletionStage<Long> pause() { return policy.pause(); }

        /** Resumes the specified pause generation; false if the generation is stale. */
        public CompletionStage<Boolean> resume(long generation) { return policy.resume(generation); }

        /** Injects reliable bytes into a byte splice. Copies the remaining bytes on acceptance for processing. */
        public Injection injectStream(ByteBuffer bytes) {
            return policy.inject(SpliceUnit.Shape.STREAM, -1, -1, bytes);
        }

        /** Injects an opaque reliable capsule into a packet splice. */
        public Injection injectCapsule(long type, ByteBuffer value) {
            return policy.inject(SpliceUnit.Shape.CAPSULE, type, -1, value);
        }

        /** Injects a reliable DATAGRAM capsule into a packet splice. */
        public Injection injectDatagramCapsule(long context, ByteBuffer payload) {
            return policy.inject(SpliceUnit.Shape.CAPSULE, io.zmux.internal.wire.Capsule.DATAGRAM, context, payload);
        }

        /** Injects a stream datagram. */
        public Injection injectDatagram(long context, ByteBuffer payload) {
            return policy.inject(SpliceUnit.Shape.DATAGRAM, -1, context, payload);
        }
    }

    private final SplicePolicy aToB, bToA;
    private final SpliceAbort abort;
    private final ZmuxFuture<SpliceResult> result;

    SpliceController(SplicePolicy aToB, SplicePolicy bToA, SpliceAbort abort, ZmuxFuture<SpliceResult> result) {
        this.aToB = aToB;
        this.bToA = bToA;
        this.abort = abort;
        this.result = result;
    }

    public Direction aToB() { return new Direction(aToB); }
    public Direction bToA() { return new Direction(bToA); }
    public boolean abort(long code) { return abort.abort(code); }
    public CompletionStage<SpliceResult> result() { return result.minimalCompletionStage(); }
}
