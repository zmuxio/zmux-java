package io.zmux.internal.core;

import io.zmux.Direction;
import io.zmux.DirectionHealth;
import io.zmux.DirectionHealth.ProofKind;
import io.zmux.DirectionHealth.Reason;
import io.zmux.DirectionHealth.State;
import io.zmux.JoinTimeoutException;
import io.zmux.PipeHealth;
import io.zmux.PipeHealthChange;
import io.zmux.PipeId;

import java.time.Duration;
import java.util.OptionalLong;

/** Projects the protocol's direction state and records evidence of the current healthy interval. */
final class HealthProjection {
    private static final long OBSERVE_FOR = 10 * Time.SECOND;
    private final long settleFor;
    private final java.util.function.Consumer<Pipe> refresh;
    private final TimerHeap timers = new TimerHeap();

    HealthProjection(long settleFor, java.util.function.Consumer<Pipe> refresh) {
        this.settleFor = settleFor;
        this.refresh = refresh;
    }
    static final DirectionHealth ABSENT = new DirectionHealth(
            State.ABSENT, Reason.NONE, 0, false, false, OptionalLong.empty(), 0, ProofKind.NONE);

    private static final class Evidence {
        private boolean running;
        private long stableSince;
        private long generation, probeEnd, sendStart;
        private ProofKind proof = ProofKind.NONE;

        void update(boolean healthy, long now) {
            if (running() == healthy) return;
            proof = ProofKind.NONE;
            probeEnd = 0;
            running = healthy;
            if (healthy) {
                stableSince = now;
                generation++;
            }
        }

        boolean running() { return running; }
        OptionalLong stableSince() { return running ? OptionalLong.of(stableSince) : OptionalLong.empty(); }
        long settledAt(long settleFor) {
            assert proven() && running : "proof belongs to an ongoing healthy interval";
            return Time.add(stableSince, settleFor);
        }
        boolean proven() { return proof != ProofKind.NONE; }
        boolean needsProbe() { return running() && !proven() && probeEnd == 0; }

        void prove(ProofKind kind) {
            if (running() && !proven()) {
                proof = kind;
                probeEnd = 0;
            }
        }

        void ping(long end) { if (needsProbe()) probeEnd = end; }

        boolean acknowledged(long position) {
            if (probeEnd == 0 || position < probeEnd) return false;
            prove(ProofKind.PROBE);
            return true;
        }
    }

    static final class Slot {
        private PipeHealth snapshot;
        private TimerHeap.Timer expiry;
        private final Evidence send = new Evidence(), receive = new Evidence();
        private boolean held;
        private boolean recoveryProved;
        void recoveredByProbe() { recoveryProved = true; }
        boolean outboundHeld() { return held; }
        private Reason fault = Reason.NONE;

        PipeHealth snapshot() { return snapshot; }
        boolean needsReceiveRecord() { return !receive.proven(); }
        void sendProven() { send.prove(ProofKind.PROBE); }
        void fault(Reason reason) { fault = reason; }

        long sendGeneration() { return send.generation; }

        boolean currentSend(long end) { return send.running() && end > send.sendStart; }

        boolean needsProbe() { return send.running() && (send.needsProbe() || receive.needsProbe()); }

        /** The probe must have been sent during the observed direction's healthy interval. */
        void ping(long end) {
            send.ping(end);
            receive.ping(end);
        }

        boolean acknowledged(long position, boolean returnedOnPair) {
            boolean sendProven = send.acknowledged(position);
            boolean receiveProven = returnedOnPair && receive.acknowledged(position);
            return sendProven || receiveProven;
        }
    }

    private long sequence;

    PipeHealth sync(Pipe pipe, long now, boolean attached, boolean closing, boolean outboundUsable,
                    boolean verifiedReceiveRecord, java.util.function.Consumer<Event> events) {
        Attach a = pipe.mp();
        Slot slot = a.projection;
        PipeHealth before = slot.snapshot;
        boolean healthy = attached && !closing && !a.health.retired() && !a.prob.reval() && !a.health.draining();
        boolean sendActive = a.out() == Attach.OUT_ACTIVE && (a.topologyExcluded() & Action.DIR_OUT) == 0;
        if (healthy && sendActive && !slot.send.running() && pipe.out() != null) {
            slot.send.sendStart = pipe.out().snd.so();
        }
        slot.send.update(healthy && sendActive, now);
        if (slot.recoveryProved) {
            slot.send.prove(ProofKind.PROBE);
            slot.recoveryProved = false;
        }
        slot.receive.update(healthy && a.in() == Attach.IN_OPEN && (a.topologyExcluded() & Action.DIR_IN) == 0, now);
        if (verifiedReceiveRecord) slot.receive.prove(ProofKind.VERIFIED_RECORD);
        var joined = a.joinedAt();
        var attachedAt = before == null ? OptionalLong.empty() : before.attachedAt();
        if (attached && attachedAt.isEmpty()) attachedAt = OptionalLong.of(now);
        DirectionHealth prevSend = before == null ? ABSENT : before.send();
        DirectionHealth prevReceive = before == null ? ABSENT : before.receive();
        boolean observeJoin = joined.isPresent() && now < Time.add(joined.getAsLong(), OBSERVE_FOR);
        var send = project(pipe, true, prevSend, now, observeJoin, attached, closing, outboundUsable, slot.send);
        var receive = project(pipe, false, prevReceive, now, observeJoin, attached, closing, outboundUsable, slot.receive);
        if (prevSend.canCarry() && !slot.send.running()) slot.held = true;
        if (send.settled()) slot.held = false;
        long seq = before == null ? 0 : before.seq();
        var cause = a.health.failCause();
        if (before != null && send == prevSend && receive == prevReceive && attached == before.peerAttached()
                && joined.equals(before.joinedAt()) && cause == before.failure()) return before;
        Duration age = joined.isEmpty() ? Duration.ZERO : sinceJoin(now, joined.getAsLong());
        if (!send.equals(prevSend)) {
            seq = ++sequence;
            events.accept(Event.health(new PipeHealthChange(seq, PipeId.of(pipe.id), Direction.OUTBOUND, prevSend, send,
                    send.reason(), now, age, cause)));
        }
        if (!receive.equals(prevReceive)) {
            seq = ++sequence;
            events.accept(Event.health(new PipeHealthChange(seq, PipeId.of(pipe.id), Direction.INBOUND, prevReceive, receive,
                    receive.reason(), now, age, cause)));
        }
        slot.snapshot = new PipeHealth(seq, send, receive, attached, joined, attachedAt, age, cause);
        return slot.snapshot;
    }

    static Duration sinceJoin(long now, long joinedAt) {
        return Duration.ofNanos(Math.max(0, Time.sub(now, joinedAt)));
    }

    private DirectionHealth project(Pipe pipe, boolean out, DirectionHealth before, long now, boolean observeJoin,
                                           boolean attached, boolean closing, boolean outboundUsable, Evidence evidence) {
        if (out ? pipe.out() == null : pipe.in() == null) return ABSENT;
        Attach a = pipe.mp();
        Slot slot = a.projection;
        State state;
        Reason reason;
        boolean usable = false, settled = false;
        if (closing) {
            state = State.ENDED;
            reason = Reason.SESSION_CLOSING;
        } else if (a.health.retired() || (out ? a.out() == Attach.OUT_CLOSED || a.out() == Attach.OUT_FAILED : a.in() == Attach.IN_ENDED)) {
            state = State.ENDED;
            if (a.health.failCause() instanceof JoinTimeoutException) reason = Reason.JOIN_TIMEOUT;
            else if (a.health.draining()) reason = Reason.PLANNED_DRAIN;
            else if (a.health.resLimited()) reason = Reason.CANCELLED;
            else reason = slot.fault == Reason.NONE ? Reason.CARRIER_ERROR : slot.fault;
        } else if (a.health.draining()) {
            state = State.DRAINING;
            reason = Reason.PLANNED_DRAIN;
        } else if (!attached || a.handshaking()) {
            state = State.JOINING;
            reason = Reason.AWAIT_ATTACHMENT;
        } else if ((a.topologyExcluded() & (out ? Action.DIR_OUT : Action.DIR_IN)) != 0) {
            state = State.UNAVAILABLE;
            reason = Reason.TOPOLOGY_EXCLUDED;
        } else if (out && a.out() == Attach.OUT_SUSPECT) {
            state = State.UNAVAILABLE;
            reason = slot.fault == Reason.NONE ? Reason.ACK_STALL : slot.fault;
        } else if (a.prob.reval()) {
            state = State.UNKNOWN;
            reason = Reason.REVALIDATE;
        } else {
            usable = !out || outboundUsable;
            boolean service = usable || (a.ret.penalized() && !a.prob.probOn());
            settled = service && evidence.proven() && now >= evidence.settledAt(settleFor);
            state = evidence.proven() && service ? State.USABLE : State.UNKNOWN;
            reason = out && a.prob.probOn() ? Reason.PROBATION
                    : evidence.proven() ? Reason.PROGRESS
                    : observeJoin ? Reason.OBSERVE_JOIN : Reason.IDLE;
        }
        var previousSince = before.stableSince();
        boolean sameInterval = evidence.running() ? previousSince.isPresent() && previousSince.getAsLong() == evidence.stableSince
                : previousSince.isEmpty();
        if (state == before.state() && reason == before.reason() && usable == before.canCarry() && settled == before.settled()
                && evidence.generation == before.evidenceGeneration() && evidence.proof == before.proofKind()
                && sameInterval) return before;
        return new DirectionHealth(state, reason, now, usable, settled, evidence.stableSince(), evidence.generation, evidence.proof);
    }

    void registerDeadline(Pipe pipe) {
        Slot slot = pipe.mp().projection;
        long deadline = pipe.mp().retired() ? Time.NEVER : deadline(pipe);
        if (slot.expiry == null && deadline != Time.NEVER) {
            slot.expiry = new TimerHeap.Timer() {
                @Override public void fire(long now) { refresh.accept(pipe); }
            };
        }
        if (slot.expiry != null) timers.schedule(slot.expiry, deadline);
    }

    void onTimeout(long now) { timers.fireDue(now); }
    long nextDeadline() { return timers.earliest(); }
    void closeTimers() { timers.clear(); }

    private long deadline(Pipe pipe) {
        Slot slot = pipe.mp().projection;
        PipeHealth s = slot.snapshot;
        if (s == null) return Time.NEVER;
        long deadline = Time.NEVER;
        if (s.send().reason() == Reason.OBSERVE_JOIN || s.receive().reason() == Reason.OBSERVE_JOIN)
            deadline = Time.add(s.joinedAt().orElse(Time.NEVER), OBSERVE_FOR);
        if (slot.send.proven() && s.send().state() == State.USABLE && !s.send().settled())
            deadline = Math.min(deadline, slot.send.settledAt(settleFor));
        if (slot.receive.proven() && s.receive().canCarry() && !s.receive().settled())
            deadline = Math.min(deadline, slot.receive.settledAt(settleFor));
        return deadline;
    }
}
