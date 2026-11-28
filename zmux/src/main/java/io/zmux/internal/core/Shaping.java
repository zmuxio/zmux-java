package io.zmux.internal.core;

import java.time.Duration;

import io.zmux.shaping.Capability;
import io.zmux.shaping.DatagramView;
import io.zmux.shaping.RuntimeReason;
import io.zmux.shaping.UnitPlan;
import java.util.EnumMap;
import io.zmux.shaping.Event;
import io.zmux.shaping.EventKind;
import io.zmux.shaping.RecordPlan;
import io.zmux.shaping.RecordView;
import io.zmux.shaping.Effects;
import io.zmux.shaping.Shaper;
import io.zmux.shaping.ShaperFactory;
import io.zmux.shaping.ShapingLimits;
import io.zmux.shaping.ShapingMeters;
import io.zmux.shaping.ShapingStats;
import io.zmux.shaping.WireAccount;
import java.util.Set;

/**
 * Record shaping: a {@link Shaper} per outbound pipe proposes record sizes and timing; this
 * class executes the proposals within the protocol's limits and the configured bounds.
 * The hot path skips this state when neither policy nor accounting has been installed. Owner thread only.
 */
final class Shaping {
    /** What shaping needs of the session. */
    interface Port {
        long now();
        /** The peer's max_record_size, at most 65535. */
        int peerRecordLimit();
        int preambleLimit();
        /** Whether the peer is a zmux endpoint. */
        boolean peerZmux();
        boolean standaloneRoom(int earlier);
        void markStandalone();
        /** The session's own bytes sent: no padding, injections or copies. */
        long earnBytes();
        /** Stream data waits to be scheduled. */
        boolean dataWaiting();
        boolean coverEligible(Pipe p);
        /**
         * When a shaping delay of p's next record ends because the record carries a pending PIPE_ACK: the
         * acknowledgement's deadline, zmux_max_ack_delay after the first ack-eliciting byte it reports
         * (MULTIPATH.md Section 6.3). The peer's PTO and so its T_suspect count on that bound; held longer, the
         * peer's working pipe turns suspect at every exchange. {@link Time#NEVER} when p carries none.
         */
        long ackRelease(Pipe p);
    }

    /** Drivers arm no gate timer earlier than this. */
    static final long GRANULARITY = Time.MILLISECOND;

    private final Port port;
    private final PipeTable pipes;
    private final WireMeter account;
    /** Effective policy and host restrictions. Delay is measured in nanoseconds. */
    private boolean noPadding, noPause, noInjection;
    private long maxDelay;
    private final OverheadLedger ledger;
    private ShapingLimits.Effective effective;
    private record JoinHold(long start, long due, long generation, java.util.function.BooleanSupplier live,
                            Runnable release) {}
    private final java.util.LinkedHashMap<Object, JoinHold> handshakeHolds = new java.util.LinkedHashMap<>();
    private ShaperFactory factory;
    private java.util.function.Consumer<Event> observation;
    /** Bumped by setShaper: a pipe's instance of an older generation is replaced. */
    private long gen;
    // Counters.
    private long stallSpent, maxAddedDelay, injectionAccepted, injectionRejected, acceptedBytes, sentBytes, clamped, faults, reasonChanges;
    private final EnumMap<RuntimeReason, Long> rejections = new EnumMap<>(RuntimeReason.class);
    private final EnumMap<RuntimeReason, Long> reasons = new EnumMap<>(RuntimeReason.class);
    /** The account starts accruing when its first parameters take effect. */
    Shaping(Port port, PipeTable pipes, ShapingLimits.Effective inForce, long origin, long start) {
        this.port = port;
        this.pipes = pipes;
        account = new WireMeter(us(start));
        ledger = new OverheadLedger(origin, port::now, inForce);
        take(inForce);
        account.set(us(start), inForce.wire());
    }

    private void take(ShapingLimits.Effective l) {
        effective = l;
        ledger.configure(l);
        noPadding = !l.padding();
        noPause = !l.pause();
        noInjection = !l.injection();
        maxDelay = Time.nanos(l.maxAddedDelay());
    }

    /** Replaces limits after validation and tightens outstanding holds in place. */
    void limits(ShapingLimits.Effective inForce) {
        long changedAt = port.now();
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps == null || !ps.active()) continue;
            ps.settleHeld();
            if (ps.recordDelayLane != null) ps.advanceRecordDelay(changedAt);
            if (ps.nativeHold != null) ps.nativeHold.advance(changedAt, ps.nativeReady);
        }
        take(inForce);
        account.set(us(changedAt), inForce.wire());
        handshakeHolds.replaceAll((key, h) -> {
            long updated = Math.max(changedAt, Time.add(h.start, noPause ? 0 : maxDelay));
            // A policy change can shorten a hold, even after its original deadline.
            return new JoinHold(h.start, Math.min(h.due, updated), h.generation, h.live, h.release);
        });
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps == null || !ps.active()) continue;
            if (noPause) {
                ps.hold = Long.MIN_VALUE;
                ps.rateAsked = 0;
                ps.bucket.rate(0);
            } else if (ps.rateAsked != 0) {
                ps.setRate();
            }
            long now = port.now();
            if (ps.recordDelayLane != null) {
                ps.recordReady = Math.min(ps.recordReady, Time.add(now,
                        boundedDelay(Long.MAX_VALUE, ps.recordDelayLane.shapeDelay.spent)));
            }
            if (ps.nativeItem != null) {
                ps.nativeReady = Math.min(ps.nativeReady, Time.add(now,
                        boundedDelay(Long.MAX_VALUE, ps.nativeItem.delay.spent)));
            }
            ps.releaseCoverReservation();
            if (ps.injLen > 0) ps.reserveCover(0);
        }
        pipes.wakeWriters();
    }

    boolean exact() { return ledger.exact(); }

    long profileRoom() { return ledger.room(); }

    void unknownProgress() {
        if (exact()) return; // A late receipt can already be outside every window.
        long now = port.now();
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps == null) continue;
            ps.settleHeld();
            ps.releaseCoverReservation();
            if (ps.nativeReservation != null) {
                ps.nativeReservation.release();
                ps.nativeReservation = null;
            }
            if (ps.nativeItem != null) ps.nativeReady = Math.min(ps.nativeReady, now);
            if (ps.recordDelayLane != null) ps.recordReady = Math.min(ps.recordReady, now);
        }
        pipes.wakeWriters();
    }

    OverheadLedger.Reservation reserve(long extra) { return ledger.reserve(extra); }

    OverheadLedger.Reservation submit(long extra) {
        var receipt = reserve(extra);
        receipt.commit();
        return receipt;
    }

    OverheadLedger.HandshakeReservation submitHandshake(int size, int fixed, HandshakePlans.Padding padding, boolean atomic) {
        return ledger.new HandshakeReservation(size, fixed, padding, atomic);
    }

    void earn(long bytes) {
        if (bytes != 0) account.accrue(us(port.now()), bytes);
    }

    /** Microseconds of the session clock. */
    private static long us(long t) { return Math.max(t, 0) / 1000; }

    /** A decision was reduced to a limit. */
    void clamp() { clip(RuntimeReason.CLIPPED_ROOM); }

    void clip(RuntimeReason reason) {
        clamped = WireMeter.addSat(clamped, 1);
        reason(reason);
    }

    void reason(RuntimeReason reason) {
        reasons.merge(reason, 1L, WireMeter::addSat);
        reasonChanges++;
    }

    /** Projects the account at the session clock without changing its balance. */
    ShapingMeters meters() { return meters(maxAddedDelay); }

    ShapingMeters snapshotMeters() { return meters(maxConsumedDelay()); }

    private ShapingMeters meters(long delay) {
        return new ShapingMeters(account.left(us(port.now())), -1L, account.spent(), stallSpent / 1000,
                port.earnBytes(), ledger.snapshot(), Duration.ofNanos(delay), gen);
    }

    long signature() {
        return account.spent() + acceptedBytes + sentBytes + injectionRejected + clamped + stallSpent
                + faults + maxAddedDelay + port.earnBytes() + ledger.revision() + reasonChanges;
    }

    ShapingStats stats(long handshakeFaults, long handshakeUnplanned) {
        return new ShapingStats(account.spent(), Duration.ofNanos(stallSpent), Duration.ofNanos(maxConsumedDelay()),
                port.earnBytes(), ledger.snapshot(), acceptedBytes, sentBytes, injectionAccepted, injectionRejected, rejections, reasons, clamped,
                WireMeter.addSat(faults, handshakeFaults), handshakeUnplanned);
    }

    private long maxConsumedDelay() {
        long now = port.now();
        long max = maxAddedDelay;
        for (JoinHold hold : handshakeHolds.values()) max = Math.max(max, Math.max(0, Math.min(now, releaseAt(hold)) - hold.start));
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps == null) continue;
            if (ps.heldSince != Time.NEVER) max = Math.max(max, elapsedHold(ps.gateLane.shapeDelay, ps.heldSince, ps.gateCheck, now));
            if (ps.recordDatagrams != null) max = Math.max(max, ps.recordDatagrams.projected(now, ps.recordDeadline()));
            else if (ps.recordDelayLane != null && !ps.recordDelayLane.tx.notWritable())
                max = Math.max(max, elapsedHold(ps.recordDelayLane.shapeDelay, ps.recordStart, ps.recordDeadline(), now));
            if (ps.nativeHold != null) max = Math.max(max, ps.nativeHold.projected(now, ps.nativeReady()));
        }
        return max;
    }

    /** Per-unit wait history carried through layout changes and policy replacement. */
    static final class Delay {
        private long spent, start, end;

        long additional(long from, long until) {
            long overlap = Math.max(0, Math.min(until, end) - Math.max(from, start));
            return Math.max(0, until - from - overlap);
        }

        void reset() { spent = start = end = 0; }
    }

    private void accountWait(Delay delay, long from, long until) {
        if (until <= from) return;
        long added = delay.additional(from, until);
        if (delay.spent != 0 && from <= delay.end && until >= delay.start) {
            delay.start = Math.min(delay.start, from);
            delay.end = Math.max(delay.end, until);
        } else {
            delay.start = from;
            delay.end = until;
        }
        delay.spent = Time.add(delay.spent, added);
        maxAddedDelay = Math.max(maxAddedDelay, delay.spent);
        addStall(added);
    }

    private static long elapsedHold(Delay delay, long start, long release, long now) {
        return Time.add(delay.spent, delay.additional(start, Math.min(now, release)));
    }

    /** A single gate accounts for its selected datagram and any ordinarily eligible followers. */
    final class DatagramHold {
        private Datagrams.NativeWork head;
        private Datagrams.SendFlow flow;
        private java.util.function.Predicate<Datagrams.NativeWork> eligible;
        private long since, due;
        private boolean closed;

        DatagramHold(Datagrams.NativeWork head, Datagrams.SendFlow flow,
                     java.util.function.Predicate<Datagrams.NativeWork> eligible, long since, long due) {
            this.head = head;
            this.flow = flow;
            this.eligible = eligible;
            this.since = since;
            this.due = due;
            if (flow != null) {
                if (flow.hold() != null) flow.hold().close();
                flow.hold(this);
            }
        }

        private boolean eligible(Datagrams.NativeWork item) {
            return eligible == null || eligible.test(item);
        }

        private long release(Datagrams.NativeWork item, long t) {
            if (!eligible(item)) return t;
            return Math.min(t, Math.min(item.expiry(),
                    Time.add(Math.max(since, item.eligibleSince()), Math.max(0, maxDelay - item.delay.spent))));
        }

        long deadline(long limit) {
            long t = Math.min(due, limit);
            if (closed || !eligible(head)) return t;
            t = release(head, t);
            if (flow != null) for (Datagrams.Out out : flow.queue) {
                if (out != head) t = release(out, t);
            }
            return t;
        }

        long projected(long now, long limit) {
            if (closed || !eligible(head)) return 0;
            long t = deadline(limit);
            long max = elapsedHold(head.delay, Math.max(since, head.eligibleSince()), t, now);
            if (flow != null) for (Datagrams.Out out : flow.queue) {
                if (out != head && eligible(out)) max = Math.max(max,
                        elapsedHold(out.delay, Math.max(since, out.eligibleSince()), t, now));
            }
            return max;
        }

        void advance(long now) { advance(now, Time.NEVER); }

        void advance(long now, long limit) {
            if (closed) return;
            due = deadline(limit);
            long end = Math.min(now, due);
            if (eligible(head)) {
                accountWait(head.delay, Math.max(since, head.eligibleSince()), end);
                if (flow != null) for (Datagrams.Out out : flow.queue) {
                    if (out != head && eligible(out)) accountWait(out.delay, Math.max(since, out.eligibleSince()), end);
                }
            }
            since = now;
        }

        void close() { close(Time.NEVER); }

        void close(long limit) {
            if (closed) return;
            advance(port.now(), limit);
            closed = true;
            if (flow != null && flow.hold() == this) flow.hold(null);
            head = null;
            flow = null;
            eligible = null;
        }
    }

    boolean handshakeFits(io.zmux.carrier.HandshakeCost cost) {
        return cost.units().stream().allMatch(unit -> unit.addedDelayNanos() <= maxDelay
                && (!noPause || unit.addedDelayNanos() == 0) && (!noPadding || unit.extraBytes() == 0));
    }

    void transferHandshake(io.zmux.carrier.HandshakeCost cost, long observedNanoTime) {
        for (var unit : cost.units()) {
            long age = Math.max(0, observedNanoTime - unit.dispatchNanoTime());
            ledger.transferHandshake(unit.baselineBytes(), unit.extraBytes(), age);
            addStall(unit.addedDelayNanos());
            maxAddedDelay = Math.max(maxAddedDelay, unit.addedDelayNanos());
        }
    }

    long generation() { return gen; }

    void holdJoin(Object key, long wait, java.util.function.BooleanSupplier live, Runnable release) {
        handshakeHolds.put(key, new JoinHold(port.now(), Time.add(port.now(), wait), gen, live, release));
    }

    void cancelJoinHold(Object key) {
        JoinHold hold = handshakeHolds.remove(key);
        if (hold != null) settleJoinHold(hold, port.now());
    }

    private void settleJoinHold(JoinHold hold, long now) {
        long elapsed = Math.max(0, Math.min(now, releaseAt(hold)) - hold.start);
        maxAddedDelay = Math.max(maxAddedDelay, elapsed);
        addStall(elapsed);
    }

    private long releaseAt(JoinHold hold) {
        return hold.generation == gen && !noPause && exact()
                ? hold.due : Math.min(hold.due, port.now());
    }

    private void releaseJoins(long now) {
        if (handshakeHolds.isEmpty()) return;
        for (Object key : java.util.List.copyOf(handshakeHolds.keySet())) {
            JoinHold hold = handshakeHolds.get(key);
            if (hold == null || releaseAt(hold) > now) continue;
            handshakeHolds.remove(key);
            settleJoinHold(hold, now);
            if (hold.live.getAsBoolean()) hold.release.run();
        }
    }

    boolean noPadding() { return noPadding; }
    boolean peerZmux() { return port.peerZmux(); }

    private void validate(io.zmux.shaping.GateRequirement gates, ShapingLimits.Effective bounds) {
        if (!gates.fits(bounds.maxAddedDelay().toNanos() / 1000, port.preambleLimit())) {
            throw new IllegalArgumentException("conflict: rate/burst recovery exceeds max_added_delay");
        }
    }

    void validateLimits(ShapingLimits.Effective bounds) {
        if (factory != null) validate(factory.gateRequirement(), bounds);
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps != null && ps.active()) validate(ps.sh.gateRequirement(), bounds);
        }
    }

    private static Shaper create(ShaperFactory f, Pipe p) {
        return f == null ? null : f.create(new io.zmux.shaping.ShapedPipe(io.zmux.PipeId.of(p.id),
                p.carrier(), writeOverhead(p), p.variant()));
    }

    private PipeShape prepare(Pipe p, Shaper x, long generation, ShapingLimits.Effective bounds) {
        PipeShape ps = new PipeShape(p, x, generation);
        if (x != null) {
            validate(x.gateRequirement(), bounds);
            Set<Capability> caps = Set.copyOf(x.capabilities());
            ps.plan = caps.contains(Capability.PLAN);
            ps.datagram = caps.contains(Capability.DATAGRAM_PLAN);
            ps.outbound = caps.contains(Capability.OUTBOUND);
            ps.includeControl = caps.contains(Capability.INCLUDE_CONTROL);
            ps.streams = caps.contains(Capability.STREAMS);
            ps.budget = caps.contains(Capability.BUDGET);
        }
        return ps;
    }

    void setFactory(ShaperFactory f, ShapingLimits.Effective bounds, Runnable publish) {
        if (f != null) validate(f.gateRequirement(), bounds);
        var prepared = new java.util.ArrayList<PipeShape>();
        for (Pipe p : pipes.all()) {
            if (available(p)) prepared.add(prepare(p, create(f, p), gen + 1, bounds));
        }
        publish.run();
        factory = f;
        gen++;
        for (Pipe p : pipes.all()) retire(p);
        limits(bounds);
        for (PipeShape ps : prepared) activate(ps);
        pipes.wakeWriters();
    }

    boolean setPipe(long id, Shaper x, Runnable publish) {
        Pipe p = pipes.get(id);
        if (p == null || p.out() == null) return false;
        PipeShape ps = prepare(p, x, gen, effective);
        publish.run();
        retire(p);
        activate(ps);
        pipes.wakeWriters(id);
        return true;
    }

    private static boolean available(Pipe p) {
        return p.out() != null && !p.out().failed() && (p.mp() == null || !p.mp().handshaking());
    }

    PipeShape of(Pipe p) {
        if (!available(p)) return null;
        PipeShape ps = p.shape();
        if (ps == null || ps.gen != gen) {
            try {
                ps = prepare(p, create(factory, p), gen, effective);
            } catch (RuntimeException | AssertionError invalid) {
                faults++;
                ps = new PipeShape(p, null, gen);
            }
            activate(ps);
        }
        return ps.active() ? ps : null;
    }

    /** The write overhead of p's outbound carrier (lane 0). */
    private static int writeOverhead(Pipe p) {
        var out = p.out();
        return out == null || out.lanes.isEmpty() ? 0 : out.lanes.getFirst().st.info().overhead(true);
    }

    private static void retire(Pipe p) {
        PipeShape old = p.shape();
        if (old != null && old.active()) {
            old.event(EventKind.REPLACED, 0);
            old.cancelNative(RuntimeReason.STALE_GENERATION);
            old.reset();
        }
    }

    private static void activate(PipeShape ps) {
        ps.p.shape(ps);
        if (ps.active()) ps.event(EventKind.START, 0);
    }

    private long boundedDelay(long asked) { return boundedDelay(asked, 0); }

    private long boundedDelay(long asked, long prior) {
        if (noPause || !exact()) return 0;
        return Math.clamp(asked, 0, Math.max(0, maxDelay - prior));
    }

    /** Bounds a pause and meters the granted wait. */
    long pause(long asked) { return pause(asked, 0); }

    private long pause(long asked, long prior) {
        long delay = boundedDelay(asked, prior);
        if (delay < asked) clip(noPause ? RuntimeReason.DISABLED : RuntimeReason.OVERHEAD_LIMIT);
        return delay;
    }

    private void addStall(long d) { stallSpent = d > Long.MAX_VALUE - stallSpent ? Long.MAX_VALUE : stallSpent + d; }

    /**
     * H_p, the record hold of failure detection: the longest a record of p without urgent frames may be held between building and
     * handing to the carrier, as bounded now; 0 unshaped by plans, or when nothing bounds one wait.
     */
    long maxHold(Pipe p) {
        PipeShape ps = p.shape();
        if (ps == null || ps.sh == null || !ps.plan || noPause) return 0;
        return boundedDelay(maxDelay);
    }

    /** Whether the pipe's gate holds stream data now. */
    static boolean held(Pipe p, Pipe.OutLane lane, long now) {
        PipeShape ps = p.shape();
        return ps != null && ps.active() && ps.held(lane, now);
    }

    /** The planned record of p waits for its delay: nothing goes out on the pipe but closing frames. */
    static boolean delaying(Pipe p, long now) {
        PipeShape ps = p.shape();
        return ps != null && ps.sh != null && ps.delaying(now);
    }

    /** A due injection may be placed on the pipe's reliable lane 0. */
    boolean wants(Pipe p, long lane) {
        PipeShape ps = p.shape();
        return lane == 0 && ps != null && ps.injDue && port.coverEligible(p);
    }

    static boolean deferred(PipeShape ps) { return ps.injDeferred && ps.injDue; }

    void inject(RecordWriter w) {
        PipeShape ps = w.shape();
        if (ps == null || !wants(ps.p, w.b().lane()) || !w.canCover()) return;
        ps.releaseCoverReservation();
        if (w.cover()) return;
        ps.reserveCover(w.added());
        ps.injDeferred = true;
    }

    RuntimeReason coverRejection(int bytes, int cost, long room, boolean limited, int earlier) {
        if (noInjection || noPadding) return RuntimeReason.DISABLED;
        if (limited && !port.standaloneRoom(earlier)) return RuntimeReason.RATE_LIMIT;
        if (bytes <= 0) return RuntimeReason.CLIPPED_ROOM;
        if (cost > room) return RuntimeReason.OVERHEAD_LIMIT;
        if (cost > account.left(us(port.now()))) return RuntimeReason.INSUFFICIENT_COVER_CREDIT;
        return null;
    }

    void commit(RecordWriter w) {
        if (w.b().bytes() != 0) w.b().overhead(submit(w.added()));
        var result = w.coverResult();
        if (result == null) return;
        PipeShape ps = result.shape();
        EventKind kind;
        if (result.rejection() == null) {
            account.charge(us(port.now()), result.cost());
            if (result.standalone() && !port.peerZmux()) port.markStandalone();
            injectionAccepted = WireMeter.addSat(injectionAccepted, 1);
            acceptedBytes = WireMeter.addSat(acceptedBytes, result.bytes());
            kind = EventKind.INJECTION_ACCEPTED;
        } else {
            injectionRejected = WireMeter.addSat(injectionRejected, 1);
            rejections.merge(result.rejection(), 1L, WireMeter::addSat);
            reason(result.rejection());
            kind = EventKind.INJECTION_REJECTED;
        }
        ps.deliver(kind, 0, result.bytes(), 0, null, false, false, result.id(), result.cost(), result.rejection());
    }

    void observe(java.util.function.Consumer<Event> observer) { observation = observer; }

    void coverSent(int bytes) { sentBytes = WireMeter.addSat(sentBytes, bytes); }

    void completeCover(PipeShape origin, long lane, long id, int bytes, RuntimeReason result) {
        if (result != null) reason(result);
        if (origin.active() && origin.p.shape() == origin && pipes.get(origin.p.id) == origin.p) {
            origin.deliver(EventKind.INJECTION_CARRIER_COMPLETED, lane, bytes, 0, Event.Unit.RECORD, false, false, id, 0, result);
        } else if (observation != null) observation.accept(new Event(EventKind.INJECTION_CARRIER_COMPLETED, origin.pid,
                lane, port.now(), bytes, 0, meters(), Event.Unit.RECORD, false, false, id, 0, result));
    }

    void outbound(PipeShape ps, long lane, int length, Event.Unit unit, boolean data, boolean control) {
        if (ps == null || !ps.wantsOutbound() || (!data && !(control && ps.includeControl))
                || ps.p.shape() != ps || pipes.get(ps.p.id) != ps.p || !port.coverEligible(ps.p)) return;
        ps.deliver(EventKind.OUTBOUND, lane, length, 0, unit, data, control, 0, 0, null);
    }

    /** The earliest shaping timer. */
    long deadline() {
        long t = Time.NEVER;
        for (JoinHold hold : handshakeHolds.values()) t = Math.min(t, releaseAt(hold));
        boolean waiting = port.dataWaiting();
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps == null || ps.sh == null) continue;
            t = Math.min(t, ps.wakeAt);
            if (ps.nativeWake) t = Math.min(t, ps.nativeReady());
            if (ps.injLen > 0 && !ps.injDue) t = Math.min(t, ps.injAt);
            if (ps.recordWake) t = Math.min(t, ps.recordDeadline());
            if (ps.wake && waiting) {
                t = Math.min(t, ps.gateCheck);
            }
        }
        return t;
    }

    private static long unsignedMin(long a, long b) { return Long.compareUnsigned(a, b) < 0 ? a : b; }

    /** Runs the due shaping timers. */
    void timeout(long now) {
        releaseJoins(now);
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps == null || ps.sh == null) continue;
            long at = ps.wakeAt;
            if (ps.nativeWake && ps.nativeReady() <= now) {
                ps.nativeWake = false;
                pipes.wakeWriters(p.id);
            }
            if (at <= now) {
                ps.wakeAt = Time.NEVER;
                ps.event(EventKind.TIMER, 0);
                if (!ps.active()) continue;
            }
            if (ps.injLen > 0 && !ps.injDue && ps.injAt <= now) {
                ps.injDue = true;
                pipes.wakeWriters(p.id);
            }
            if (ps.recordWake && ps.recordDeadline() <= now) {
                ps.recordWake = false;
                pipes.wakeWriters(p.id);
            }
            if (ps.wake && ps.gateCheck <= now && !ps.held(ps.gateLane, now)) {
                ps.wake = false;
                pipes.wakeWriters(p.id);
            }
        }
    }

    /** Ends shaping of a pipe whose outbound direction failed. */
    static void pipeDown(Pipe p) {
        PipeShape ps = p.shape();
        if (ps != null && ps.sh != null) {
            ps.event(EventKind.PIPE_FAILED, 0);
            ps.reset();
        }
    }

    /** Reports a stream event to the pipes that asked for it. */
    void stream(EventKind k, long id) {
        for (Pipe p : pipes.all()) {
            PipeShape ps = p.shape();
            if (ps != null && ps.sh != null && ps.streams) ps.event(k, id);
        }
    }

    /** The frames bytes of a record of n frames bytes planned to these lengths on the carrier. */
    static int framesFor(int n, boolean sizeField, int over, int padTo, int padMultiple, int pad) {
        int l = WireLength.wireOf(n, sizeField, over);
        int t = UnitPlan.target(l, padTo, padMultiple, pad);
        return t <= l ? n : Math.max(WireLength.framesOf(t, sizeField, over), n);
    }

    /** The shaping state of one outbound pipe. */
    final class PipeShape {
        final Pipe p;
        /** The pipe ID the shaper sees, made once. */
        final io.zmux.PipeId pid;
        private Shaper sh;
        final long gen;
        private boolean plan, datagram, outbound, includeControl, streams, budget;
        private long seq;
        /** The plan applies to the next record. */
        private boolean planned;
        final RecordPlan rp = new RecordPlan();
        private final Effects effects = new Effects();
        private final UnitPlan nativePlan = new UnitPlan();
        private Datagrams.NativeWork nativeItem;
        private Datagrams direction;
        private DatagramHold nativeHold, recordDatagrams;
        private Datagrams.ChannelUse nativeChannel;
        private OverheadLedger.Reservation nativeReservation;
        private long nativeReady;
        private boolean nativeWake, recordWake;
        private long recordStart, recordReady;
        private Pipe.OutLane recordDelayLane;
        /** How the plan's lengths read in frames bytes: the size field, the write overhead, the largest frames length. */
        private boolean sizeField;
        private int over;
        /** Stream data waits until then. */
        private long hold = Long.MIN_VALUE;
        /** The latest end of a pause asked for: an answer repeats it, and a limit may have shortened that pause. */
        private Pipe.OutLane gateLane;
        /** The plan's padTo as asked, on the carrier. */
        private int wirePadTo;
        /** The rate as answered; the bucket holds the rate in force. */
        private long rateAsked;
        private final TokenBucket bucket = new TokenBucket();
        /** The rate gate holds data since then; NEVER: not held. */
        private long heldSince = Time.NEVER;
        /** A writer waits for hold or tokens. */
        private boolean wake;
        /** The minimum check time for the current wait. */
        private long gateCheck;
        private long injAt = Time.NEVER;
        private long injId;
        private OverheadLedger.Reservation coverReservation;
        private int injLen; // frame bytes, zero when no injection is pending
        private boolean injDue;
        /** The due injection waits for the next batch. */
        private boolean injDeferred;
        private long wakeAt = Time.NEVER;
        /** The pipe's weight in scheduler views; 0: none. */
        private long weight;

        PipeShape(Pipe p, Shaper sh, long gen) { this.p = p; this.sh = sh; this.gen = gen; pid = io.zmux.PipeId.of(p.id); }

        Shaping owner() { return Shaping.this; }

        /** The plan applies to the record being built. */
        boolean planned() { return planned; }

        /** An instance is installed. */
        boolean active() { return sh != null; }

        /** The weight in scheduler views; 0: none. */
        long weight() { return weight; }

        /** Drops the instance and every pending item. */
        void reset() {
            settleHeld();
            cancelNative(RuntimeReason.CANCELLED);
            finishRecordDelay();
            sh = null;
            planned = false;
            hold = Long.MIN_VALUE;
            rateAsked = 0;
            bucket.rate(0);
            bucket.burst(0);
            heldSince = Time.NEVER;
            wake = false;
            clearInjection();
            wakeAt = Time.NEVER;
            weight = 0;
        }

        private void fault() {
            faults++;
            reset();
        }

        void stop(boolean notify) {
            if (sh == null) return;
            if (notify) event(EventKind.STOPPED, 0);
            reset();
        }

        void event(EventKind kind, long stream) {
            deliver(kind, 0, 0, stream, null, false, false, 0, 0, null);
        }

        void deliver(EventKind kind, long lane, int length, long stream, Event.Unit unit, boolean data,
                     boolean control, long id, int cost, RuntimeReason rejection) {
            Shaper x = sh;
            if (x == null) return;
            Effects e = effects;
            e.clear();
            try {
                Event event = new Event(kind, pid, lane, port.now(), length, stream,
                        budget ? meters() : ShapingMeters.ZERO, unit, data, control, id, cost, rejection);
                if (observation != null) observation.accept(event);
                x.onEvent(event, e);
            } catch (RuntimeException | AssertionError failed) {
                fault();
                return;
            }
            if (kind == EventKind.REPLACED || kind == EventKind.STOPPED || kind == EventKind.PIPE_FAILED) return;
            apply(e, kind);
        }

        void releaseCoverReservation() {
            if (coverReservation != null) coverReservation.release();
            coverReservation = null;
        }

        void reserveCover(int encodingExtra) {
            var lane = p.out().lane(0);
            if (lane == null || noPadding || noInjection || !port.coverEligible(p)) return;
            var info = lane.st.info();
            int frames = Math.min(injLen, port.peerRecordLimit());
            long cost = (long) frames + info.overhead(true)
                    + (info.framing() == Framing.BYTES ? io.zmux.internal.wire.VarInt.length(frames) : 0);
            if (cost <= profileRoom() - encodingExtra && cost <= account.left(us(port.now()))) coverReservation = ledger.reserve(cost);
        }

        void clearInjection() {
            releaseCoverReservation();
            injId = 0;
            injLen = 0;
            injAt = Time.NEVER;
            injDue = injDeferred = false;
        }

        int injectionLength() { return injLen; }
        long injectionId() { return injId; }

        boolean wantsOutbound() { return active() && outbound; }
        boolean serialNative() { return active() && (datagram || outbound); }

        void cancelIntents() {
            cancelNative(RuntimeReason.CANCELLED);
            clearInjection();
            wakeAt = Time.NEVER;
        }

        void apply(Effects e, EventKind origin) {
            if (e.isEmpty()) return;
            long now = port.now();
            if (e.isInjectSet()) {
                if (e.isInjectNone() || e.injectId() == 0 || e.injectLength() <= 0) {
                    clearInjection();
                } else if ((origin == EventKind.OUTBOUND || origin == EventKind.TIMER) && port.coverEligible(p)) {
                    clearInjection();
                    injId = e.injectId();
                    injLen = e.injectLength();
                    injAt = e.injectAtNanos();
                    injDue = injAt <= now;
                    injDeferred = false;
                    reserveCover(0);
                }
            }
            if (e.isGateSet() && !noPause) {
                hold = Math.max(hold, e.gatePauseUntilNanos());
                // Both are unsigned: what lies above a bound takes the bound.
                long r = unsignedMin(e.gateRate(), WireAccount.MAX_RATE);
                long b = TokenBucket.burstFloor(e.gateBurst(), port.peerRecordLimit());
                if (r != rateAsked || b != bucket.burst()) {
                    if (r != 0 && e.gateBurst() != 0 && Long.compareUnsigned(e.gateBurst(), RecordPlan.MIN_MAX_LENGTH) < 0) clamped++;
                    settleHeld();
                    rateAsked = r;
                    bucket.burst(b);
                    setRate();
                    bucket.reset(us(now));
                }
            }
            if (e.isWakeSet()) {
                wakeAt = e.isWakeNone() ? Time.NEVER : e.wakeAtNanos();
                if (wakeAt <= now) wakeAt = Time.add(now, GRANULARITY);
            }
            if (e.isWeightSet() && e.weight() >= 0) weight = e.weight();
            if (e.isStopRequested()) stop(true);
            pipes.wakeWriters(p.id);
        }

        /** Counts the time the rate gate has held data so far; the gate changes now. */
        private void settleHeld() {
            if (heldSince != Time.NEVER && gateLane != null) accountWait(gateLane.shapeDelay, heldSince, Math.min(port.now(), gateCheck));
            heldSince = Time.NEVER;
            wake = false;
        }

        private void setRate() {
            if (rateAsked != bucket.rate()) {
                bucket.fill(us(port.now()));
                settleHeld();
                bucket.rate(rateAsked);
            }
        }

        /** The planned record waits for its delay: it has not passed, and the record carries no PIPE_ACK whose deadline has. */
        boolean delaying(long now) {
            return planned && recordDelayLane != null && now < recordDeadline();
        }

        long recordDeadline() {
            long limit = Math.min(recordReady, port.ackRelease(p));
            return recordDatagrams == null ? limit : recordDatagrams.deadline(limit);
        }

        boolean held(Pipe.OutLane lane, long now) {
            if (gateLane != lane) settleHeld();
            gateLane = lane;
            if (lane.tx.notWritable() || lane.tx.writing()) {
                settleHeld();
                return false;
            }
            if (bucket.rate() > 0) bucket.fill(us(now));
            long release = hold;
            if (bucket.rate() > 0 && bucket.held()) {
                long at = bucket.release();
                release = Math.max(release, at < 0 ? Time.NEVER : Time.mul(at, Time.MICROSECOND));
            }
            if (release <= now || noPause || !exact()) {
                settleHeld();
                return false;
            }
            if (heldSince == Time.NEVER) heldSince = now;
            gateCheck = Math.min(release, Time.add(heldSince, boundedDelay(Long.MAX_VALUE, gateLane.shapeDelay.spent)));
            wake = now < gateCheck;
            if (!wake) settleHeld();
            return wake;
        }

        int dataFit(RecordWriter w, int room) {
            if (room <= 0) return room;
            Pipe.OutLane lane = writerLane(w);
            if (held(lane, port.now())) return 0;
            if (bucket.rate() == 0 || !exact() || lane.shapeDelay.spent >= maxDelay) return room;
            return Math.clamp(bucket.tokens(), 0, room);
        }

        void spent(long n) {
            if (bucket.rate() <= 0 || !exact()) return;
            // Once the deadline opens the gate, exempt bytes cannot create debt for the next unit.
            if (gateLane != null && gateLane.shapeDelay.spent >= maxDelay) n = Math.clamp(bucket.tokens(), 0, n);
            bucket.take(n);
        }

        /** Plans the record the writer opens next; false when the instance ended. */
        boolean prep(RecordWriter w) {
            if (sh == null) return false;
            if (!planned) {
                rp.clear();
                if (plan) {
                    try {
                        sh.plan(new RecordView(pid, w.b().lane(), seq, w.recMax(), writeStart(w), port.now(), budget ? meters() : ShapingMeters.ZERO), rp);
                    } catch (RuntimeException | AssertionError e) {
                        fault();
                        return false;
                    }
                }
                if (rp.maxLength() > 0 || rp.isFlush()) clip(RuntimeReason.UNSUPPORTED_CAPABILITY);
                planned = true;
                wirePadTo = rp.padTo();
                fit(w, true);
                int r = WireLength.wireOf(w.recMax(), sizeField, over);
                if (rp.padMultiple() > r) { rp.padMultiple(r); clamp(); }
                if (rp.pad() > r) { rp.pad(r); clamp(); }
                if (!rp.effects().isEmpty()) {
                    apply(rp.effects(), null);
                    if (sh == null) return false;
                }
                if (rp.delayNanos() > 0) {
                    long now = port.now();
                    Pipe.OutLane lane = writerLane(w);
                    long prior = lane.shapeDelay.spent;
                    if (heldSince != Time.NEVER && gateLane == lane) {
                        prior += lane.shapeDelay.additional(heldSince, Math.min(now, gateCheck));
                    }
                    long wait = pause(rp.delayNanos(), prior);
                    if (wait > 0) {
                        recordDelayLane = lane;
                        recordStart = now;
                        recordReady = Time.add(now, wait);
                        recordWake = true;
                    }
                }
            }
            // A record planned in the middle of a write may open at the start of a later one, or the reverse.
            else if (over != overHere(w)) fit(w, false);
            if (delaying(port.now())) w.holdRecord();
            return true;
        }

        /** The write overhead of a record w opens now. */
        int overHere(RecordWriter w) { return laneInfo(w).overhead(w.recs() == 0); }

        private boolean writeStart(RecordWriter w) {
            return w.recs() == 0 || laneInfo(w).writesEachRecord();
        }

        private Pipe.OutLane writerLane(RecordWriter w) {
            var lane = p.out().lane(w.b().lane());
            assert lane != null : "the writer has an outbound lane";
            return lane;
        }

        private CarrierInfo laneInfo(RecordWriter w) {
            return writerLane(w).st.info();
        }

        /** Reads the plan's lengths for a record w opens now; count: its clamps are counted, once for a record. */
        private void fit(RecordWriter w, boolean count) {
            sizeField = w.framing() == Framing.BYTES;
            over = overHere(w);
            int padTo = wirePadTo;
            // No padding target goes beyond a record, so that the targets cannot overflow.
            int r = WireLength.wireOf(w.recMax(), sizeField, over);
            if (padTo > r) {
                padTo = r;
                if (count) clamp();
            }
            rp.padTo(padTo);
        }

        /** The frames bytes the plan gives a record of n frames bytes. */
        int target(int n) {
            return framesFor(n, sizeField, over, rp.padTo(), rp.padMultiple(), rp.pad());
        }

        Datagrams.NativeWork nativeItem() { return nativeItem; }
        long nativeReady() { return nativeHold == null ? nativeReady : nativeHold.deadline(nativeReady); }

        void nativeRoom(Datagrams.ChannelUse next) {
            if (nativeChannel.equals(next)) return;
            nativeHold.advance(port.now(), nativeReady);
            nativeChannel = next;
        }

        void clearNative() {
            if (nativeItem == null) return;
            nativeHold.close(nativeReady);
            nativeHold = null;
            nativeChannel = null;
            direction = null;
            if (nativeItem instanceof Datagrams.Out out) out.releaseReservation(this);
            if (nativeReservation != null) {
                nativeReservation.release();
                nativeReservation = null;
            }
            nativeItem = null;
            nativeReady = 0;
            nativeWake = false;
            pipes.wakeWriters(p.id);
        }

        void cancelNative(RuntimeReason why) {
            if (nativeItem == null) return;
            reason(why);
            clearNative();
        }

        int nativePadding(Datagrams dg, Datagrams.ChannelUse channel, Datagrams.NativeWork item,
                          Datagrams.SendFlow flow, boolean replica, long deadline) {
            if (!active() || !datagram) return 0;
            long now = port.now();
            int base = io.zmux.internal.wire.ChannelDatagram.length(channel.id(), item.dataLengthAt(dg));
            int room = Math.max(base, channel.paddedLimit());
            boolean padding = channel.padding() && !noPadding;
            if (nativeItem == item) {
                if (now < nativeReady()) return -1;
                clearNative();
            } else {
                if (flow != null && flow.hold() != null) flow.hold().close();
                nativePlan.clear();
                effects.clear();
                try {
                    sh.planDatagram(new DatagramView(pid, now, base, channel.limit(), channel.room(), padding, replica,
                            budget ? meters() : ShapingMeters.ZERO), nativePlan, effects);
                } catch (RuntimeException | AssertionError failed) {
                    fault();
                    return 0;
                }
                apply(effects, null);
                if (!active()) return 0;
                long requested = nativePlan.delayNanos();
                long beforeExpiry = Math.min(requested, Time.sub(deadline, now));
                if (beforeExpiry < requested) clip(RuntimeReason.POLICY_DEADLINE);
                long wait = pause(beforeExpiry, item.delay.spent);
                if (wait > 0) {
                    nativeItem = item;
                    direction = dg;
                    nativeChannel = channel;
                    if (padding) {
                        int target = UnitPlan.target(base, nativePlan.padTo(), nativePlan.padMultiple(), nativePlan.pad());
                        long extra = Math.min(profileRoom(), Math.min(room, target) - base);
                        if (extra > 0) nativeReservation = ledger.reserve(extra);
                    }
                    nativeReady = Time.add(now, wait);
                    nativeHold = new DatagramHold(item, flow, candidate ->
                            (!(candidate instanceof Datagrams.Out out) || out.ready())
                                    && nativeChannel.takes(candidate.dataLengthAt(direction)), now, nativeReady);
                    nativeWake = true;
                    return -1;
                }
            }
            int target = UnitPlan.target(base, nativePlan.padTo(), nativePlan.padMultiple(), nativePlan.pad());
            int extra = padding ? (int) Math.min(room - base, profileRoom()) : 0;
            if (target - base > extra) {
                var reason = !channel.padding() ? RuntimeReason.UNSUPPORTED_CAPABILITY
                        : noPadding ? RuntimeReason.DISABLED
                        : extra < room - base ? RuntimeReason.OVERHEAD_LIMIT : RuntimeReason.CLIPPED_ROOM;
                clip(reason);
            }
            return Math.min(target - base, extra);
        }

        void recordDatagram(RecordWriter writer, Datagrams.NativeWork item, Datagrams.SendFlow flow,
                            java.util.function.Predicate<Datagrams.NativeWork> eligible) {
            if (!active()) return;
            if (flow != null && flow.hold() != null && flow.hold() != recordDatagrams) flow.hold().close();
            if (!prep(writer) || recordDelayLane == null || recordDatagrams != null) return;
            recordDatagrams = new DatagramHold(item, flow, eligible, recordStart, recordReady);
        }

        void recordDatagramSent(Datagrams.NativeWork item) {
            if (recordDatagrams != null && recordDatagrams.head == item) recordDatagrams.close(recordDeadline());
        }

        private void advanceRecordDelay(long now) {
            if (recordDatagrams != null) recordDatagrams.advance(now, recordDeadline());
            else if (!recordDelayLane.tx.notWritable()) {
                accountWait(recordDelayLane.shapeDelay, recordStart, Math.min(recordDeadline(), now));
            }
            recordStart = now;
        }

        void writableChanged(Pipe.OutLane lane, long now) {
            if (gateLane == lane) settleHeld();
            if (recordDelayLane == lane) advanceRecordDelay(now);
        }

        void finishRecordDelay() {
            if (recordDatagrams != null) {
                recordDatagrams.close(recordDeadline());
                recordDatagrams = null;
            } else if (recordDelayLane != null) advanceRecordDelay(port.now());
            recordDelayLane = null;
            recordStart = recordReady = 0;
            recordWake = false;
        }

        /** The record was closed with the plan applied. */
        void ended(RecordWriter w) {
            planned = false;
            finishRecordDelay();
            if (heldSince == Time.NEVER && !port.dataWaiting()) writerLane(w).shapeDelay.reset();
            seq++;
            if (rp.isStop()) w.stopAfterCommit(this);
        }
    }
}
