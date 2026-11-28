package io.zmux.internal.loop;

import io.zmux.carrier.CarrierChannel;

import io.zmux.internal.core.TimerHeap;
import io.zmux.internal.core.Time;
import io.zmux.internal.spi.AttachmentRelease;
import io.zmux.internal.spi.CarrierProvider;
import io.zmux.internal.core.Action;

import io.zmux.internal.core.Batch;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.ReceiveSpans;
import io.zmux.internal.wire.VarInt;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.Map;

/**
 * The transports of one session and what each is to it:
 * the pipe and lane it carries, the batch of its writes, the nesting edge and the memory
 * charge of its connection. The only place a transport is attached to or detached from a
 * session; detaching closes it and returns its charge and edge exactly once. Loop only.
 */
final class CarrierSet {
    private final SessionRuntime rt;
    private final ArrayList<Attached> list = new ArrayList<>(1);
    private final java.util.concurrent.ConcurrentMap<Long, PipeLedger> accounting = new java.util.concurrent.ConcurrentHashMap<>();

    io.zmux.PipeAccounting.Ticket ticket(long pipe) { return accounting.get(pipe); }

    boolean accountingAdmits(Transport carrier, long pipe, int directions) {
        var policy = rt.hooks().accounting();
        if (!policy.enabled() || !policy.strict()) return true;
        PipeLedger ledger = accounting.get(pipe);
        return (ledger == null ? new PipeLedger(pipe, policy) : ledger).admits(carrier, directions);
    }

    CarrierSet(SessionRuntime rt) {
        this.rt = rt;
    }

    /** A transport attached to the session: the inbound it reports to. */
    final class Attached implements Inbound {
        final Transport t;
        final java.util.function.LongSupplier unsent;
        private final CarrierSetup.Bound closeRight;
        final long pipe;
        final long lane;
        /** The batch of the write in flight, reused across writes. */
        final Batch batch;
        private final Runnable nesting;
        private boolean removed;
        private boolean writeQueued;
        /** A request remains pending across an in-flight write or a writable hold. */
        private boolean writeRequested;
        private boolean writeBlocked;
        private boolean nativeRequested;
        private CarrierChannel channel;
        private io.zmux.internal.core.PipeTable.Writer writer;
        private WatchedSender watchedSender;
        final AttachmentRelease attachment;
        private final AttachmentRelease physicalRelease = sessionRelease.fork();
        /** Stops the receiving of the pipe's datagram channel; null without one. */
        private Runnable channelStop, channelWatch;
        private TimerHeap.Timer channelRecheck;
        private PipeLedger.Segment accountIn, accountOut;

        private Attached(CarrierSetup.Bound binding, long pipe, long lane, Runnable nesting, AttachmentRelease attachment) {
            this.attachment = attachment;
            closeRight = binding;
            t = binding.carrier;
            batch = (t.directions() & Action.DIR_OUT) != 0 ? new Batch() : null;
            unsent = t::unsent;
            this.pipe = pipe;
            this.lane = lane;
            this.nesting = nesting;
        }

        @Override public boolean session() { return true; }
        @Override public boolean segmentedInput() { return true; }

        @Override
        public int bytes(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            int n = rt.core.receive(pipe, lane, buf, data, pos, lim, rt.now());
            if (accountIn != null && n > 0) countInput(data, null, pos, pos + n);
            rt.markDirty();
            return n;
        }

        @Override
        public int bytes(Transport t, ReceiveSpans spans) {
            int n = rt.core.receive(pipe, lane, spans, rt.now());
            if (accountIn != null && n > 0) countInput(null, spans, 0, n);
            rt.markDirty();
            return n;
        }

        private void countInput(ByteBuffer data, ReceiveSpans spans, int from, int end) {
            int p = from, records = 0;
            while (p < end) {
                long length = VarInt.get(data, spans, p, end);
                if (length <= 0) break;
                int prefix = VarInt.lengthOf(ReceiveSpans.get(data, spans, p));
                if (length > end - p - prefix) break;
                p += prefix + (int) length;
                records++;
            }
            accountIn.add(p - from, records, end - p, false);
        }

        @Override
        public void message(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            if (accountIn != null) accountIn.add(lim - pos + t.info().carrier().recordOverhead(), 1, 0, false);
            rt.core.receiveMessage(pipe, lane, buf, data, pos, lim, rt.now());
            rt.markDirty();
        }

        @Override
        public void ended(Transport t, boolean graceful, boolean partial) {
            if (partial && accountIn != null) {
                int bytes = t.partialInputBytes();
                accountIn.add(0, 0, bytes, bytes == 0);
            }
            rt.core.pipeEnded(pipe, lane, graceful, partial, graceful ? null : t.inputError(), rt.now());
            rt.markDirty();
        }

        @Override
        public void writable(Transport t, boolean on) {
            if (!ownsWriter()) return;
            publishWritable(on && batch.ready());
            if (on) rt.core.pipes().wakeWriters(pipe);
            rt.markDirty();
        }

        @Override public void progress(Transport t, Batch b, int confirmed) {
            rt.core.writeProgress(b, confirmed, rt.now());
            rt.markDirty();
        }

        @Override
        public void written(Transport t, Batch b, int confirmed, Throwable cause) {
            if (accountOut != null) {
                var tally = t.accountedWrite(b, confirmed);
                accountOut.add(tally.bytes(), tally.records(), tally.partial(), cause != null && tally.bytes() == 0 && tally.partial() == 0);
            }
            rt.written(b, confirmed, cause);
            if (!ownsWriter()) return;
            if (!batch.ready()) publishWritable(false);
            // A partial NIO write can signal readiness before the batch is returned.
            else if (writeBlocked && cause == null) publishWritable(t.writable());
        }

        private void publishWritable(boolean on) {
            writeBlocked = !on;
            rt.core.setWritable(pipe, lane, on, rt.now());
        }

        private boolean ownsWriter() {
            return !removed && writer != null && writer.current();
        }

        void channelChanged() {
            if (!rt.loop.inLoop()) {
                rt.loop.tryExecute(this::channelChanged);
                return;
            }
            if (!ownsWriter()) return;
            rt.core.channelChanged(pipe, rt.now());
            nativeReady();
            rt.markDirty();
        }

        void nativeReady() {
            if (channel == null || !ownsWriter() || !rt.core.hasChannelOutput()) return;
            nativeRequested = true;
            enqueue();
        }

        void recheckNative() {
            if (channelWatch != null) return;
            if (!ownsWriter() || !rt.core.hasChannelOutput()) {
                if (channelRecheck != null) rt.schedule(channelRecheck, Time.NEVER);
                return;
            }
            if (channelRecheck == null) {
                channelRecheck = new TimerHeap.Timer() {
                    @Override public void fire(long now) { nativeReady(); }
                };
            }
            if (!rt.scheduled(channelRecheck)) {
                rt.schedule(channelRecheck, Time.add(rt.now(), Time.MILLISECOND));
            }
        }

        void arenaReleased() {
            if (!rt.loop.inLoop()) {
                rt.loop.tryExecute(this::arenaReleased);
                return;
            }
            if (!ownsWriter()) return;
            if (t.writable()) publishWritable(true);
            rt.core.pipes().wakeWriters(pipe);
        }

        void ready() {
            if (!ownsWriter()) return;
            writeRequested = true;
            enqueue();
        }

        private void enqueue() {
            if (writeQueued || (!nativeRequested && (!t.writable() || !batch.ready()))) return;
            writeQueued = true;
            rt.ready(this);
        }

        boolean claim() {
            writeQueued = false;
            return ownsWriter();
        }

        /** The independent datagram channel can run while a record request stays pending. */
        boolean claimRecord() {
            if (!writeRequested || !t.writable() || !batch.ready()) return false;
            writeRequested = false;
            return true;
        }

        CarrierChannel claimChannel() {
            if (!nativeRequested) return null;
            nativeRequested = false;
            return channel;
        }

        @Override
        public void outputShut(Transport t) {
            if (!ownsWriter()) return;
            rt.core.outputShut(pipe, lane, rt.now());
            rt.markDirty();
        }

        @Override
        public boolean failed(Transport t, Throwable error) {
            if (removed) return false;
            rt.internalError(error);
            return true;
        }
    }

    /**
     * Attaches t for pipe and lane and starts it bound to its attachment; nesting is the
     * nesting edge the attachment holds (returned on detach). An attached transport stays as it is.
     */
    Attached attach(Transport t, long pipe, long lane, Runnable nesting) {
        Attached a = find(t);
        if (a != null) {
            if (nesting != null) nesting.run();
            return a;
        }
        AttachmentRelease parent = null;
        if (lane != 0) {
            for (Attached main : list) {
                if (main.pipe == pipe && main.lane == 0) { parent = main.attachment; break; }
            }
        }
        CarrierSetup setup = t.setup();
        var binding = CarrierSetup.binding(t);
        a = new Attached(binding, pipe, lane, nesting == null ? () -> {} : nesting, setup.attach(t, parent));
        if (lane == 0) {
            var done = a.attachment.completion();
            retiring.put(pipe, done);
            done.whenComplete((v, e) -> retiring.remove(pipe, done));
        }
        var policy = rt.hooks().accounting();
        if (policy.enabled()) {
            var ledger = accounting.computeIfAbsent(pipe, id -> new PipeLedger(id, policy));
            int directions = t.directions();
            if ((directions & Action.DIR_IN) != 0) a.accountIn = ledger.half(t.receiveSource(), false);
            if ((directions & Action.DIR_OUT) != 0) a.accountOut = ledger.half(t.sendSource(), true);
            if (lane == 0) {
                var done = a.attachment.completion();
                ledger.finishAfter(done);
                done.whenComplete((v, e) -> accounting.remove(pipe, ledger));
            }
        }
        list.add(a);
        attachCarrierSample(a);
        if ((t.directions() & Action.DIR_OUT) != 0) {
            var physical = (PhysicalTransport) t;
            var encoder = physical.encoder();
            a.batch.workspace(encoder);
            rt.core.pipes().attachHistory(pipe, lane, physical.history());
            a.writer = rt.core.pipes().writer(pipe, lane, a::ready, a::nativeReady);
            encoder.onWritable(a::arenaReleased);
        }
        CarrierChannel ch = lane == 0 ? t.channel() : null;
        a.channel = ch;
        if (ch != null) {
            a.channelWatch = ch.watch(a::channelChanged);
            Attached attached = a;
            a.channelStop = ch.receive((data, from, end) -> {
                if (attached.removed) return;
                rt.core.receiveChannelDatagram(pipe, data, from, end, rt.now());
                rt.markDirty();
            });
        }
        t.taken();
        t.start(a);
        rt.core.pipes().wakeWriters(pipe);
        return a;
    }

    Attached find(Transport t) {
        for (Attached a : list) if (a.t == t) return a;
        return null;
    }

    /** Only redirectable carriers need additional attachment state. All access is on the session loop. */
    private static final class WatchedSender {
        private final Runnable unwatch;
        private Object identity;
        private boolean hasQuery;

        private WatchedSender(Runnable unwatch) { this.unwatch = unwatch; }
    }

    private void attachCarrierSample(Attached a) {
        Transport t = a.t;
        if ((t.directions() & Action.DIR_OUT) == 0) return;
        Runnable unwatch = t.watchCarrier(() -> rt.loop.tryExecute(() -> refreshCarrierSample(a)));
        if (unwatch == null) {
            Object identity = t.carrierIdentity();
            if (identity != null) register(identity, t.carrierQuery(identity));
        } else {
            a.watchedSender = new WatchedSender(unwatch);
            refreshCarrierSample(a);
        }
    }

    private static void refreshCarrierSample(Attached a) {
        if (a.removed) return;
        var watched = a.watchedSender;
        if (watched == null) return;
        Object identity = a.t.carrierIdentity();
        if (identity == watched.identity && (identity == null || watched.hasQuery)) return;
        var query = identity == null ? null : a.t.carrierQuery(identity);
        // A redirect can race the query lookup and settle on the same connection. Keep the
        // registration alive while promoting its unknown provider when the query becomes available.
        if (identity == watched.identity && query == null) return;
        if (identity != null) register(identity, query);
        if (watched.identity != null) CarrierSampler.detach(watched.identity);
        watched.identity = identity;
        watched.hasQuery = query != null;
    }

    private static void register(Object identity, CarrierProvider.Query query) {
        CarrierSampler.attach(identity, query);
    }

    private static void detachCarrierSample(Attached a) {
        if ((a.t.directions() & Action.DIR_OUT) == 0) return;
        var watched = a.watchedSender;
        a.watchedSender = null;
        Object identity = watched == null ? a.t.carrierIdentity() : watched.identity;
        if (identity != null) CarrierSampler.detach(identity);
        if (watched != null) watched.unwatch.run();
    }

    /** Captures the outbound connection cache handle while attachment membership is owned by the loop. */
    CarrierSampler.Connection carrierConnection(long pipe) {
        for (Attached a : list) {
            if (a.pipe == pipe && a.lane == 0 && (a.t.directions() & Action.DIR_OUT) != 0) {
                Object identity = a.t.carrierIdentity();
                if (identity != null) return CarrierSampler.connection(identity);
            }
        }
        return null;
    }

    /** Captures the outbound connection cache handles for a session snapshot. */
    Map<Long, CarrierSampler.Connection> carrierConnections() {
        Map<Long, CarrierSampler.Connection> connections = new HashMap<>();
        for (Attached a : list) {
            if (a.lane != 0 || (a.t.directions() & Action.DIR_OUT) == 0) continue;
            Object identity = a.t.carrierIdentity();
            if (identity == null) continue;
            var connection = CarrierSampler.connection(identity);
            if (connection != null) connections.put(a.pipe, connection);
        }
        return connections;
    }

    int size() { return list.size(); }

    Attached get(int i) { return list.get(i); }

    /** Detaches a and closes its transport (abortive with a cause); idempotent. */
    void remove(Attached a, Throwable cause) {
        if (a.removed) return;
        a.removed = true;
        if (a.accountIn != null) a.accountIn.retire();
        if (a.accountOut != null) a.accountOut.retire();
        list.remove(a);
        if (a.writer != null) a.writer.close();
        detachCarrierSample(a);
        try {
            if (a.channelRecheck != null) rt.schedule(a.channelRecheck, Time.NEVER);
            Runnable stop = a.channelStop, watch = a.channelWatch;
            a.channelStop = a.channelWatch = null;
            try {
                if (stop != null) SessionHost.safe(stop);
            } finally {
                if (watch != null) SessionHost.safe(watch);
            }
            if (rt.core.state().compareTo(io.zmux.internal.core.SessionState.CLOSING) >= 0) a.t.sessionEnded(rt.core.error());
        } finally {
            closeRemoved(a, cause);
        }
    }

    private void closeRemoved(Attached a, Throwable cause) {
        try {
            if (cause != null) {
                a.closeRight.close(cause);
            } else {
                var carrier = a.closeRight.carrier;
                var escalate = carrier.closeEscalation();
                a.closeRight.close(null);
                if (escalate != null) boundClose(carrier, escalate);
            }
        } finally {
            release(a);
            a.attachment.untilClosed(a.t);
            a.physicalRelease.until(a.t.whenClosed());
        }
    }

    /**
     * A graceful close of t that leaves work running is aborted once the linger timeout passes (SPEC 6.5 step 4),
     * so that no carrier outlives its session unbounded.
     */
    private void boundClose(PhysicalTransport t, java.util.function.Consumer<Throwable> escalate) {
        var deadline = new CloseDeadline(rt.loop, escalate);
        if (rt.loop.stopping()) { deadline.run(); return; }
        rt.loop.schedule(deadline, io.zmux.internal.core.Time.add(rt.loop.timerNow(), rt.core.lingerTimeout()));
        rt.loop.onShutdown(deadline);
        t.whenClosed().whenComplete((v, e) -> deadline.loop.release(deadline::disarm));
    }

    /** The owner loop consumes the escalation on expiry or shutdown, or discards it on release. */
    private static final class CloseDeadline extends TimerHeap.Timer implements Runnable {
        private final SessionHost loop;
        private java.util.function.Consumer<Throwable> action;

        CloseDeadline(SessionHost loop, java.util.function.Consumer<Throwable> action) {
            this.loop = loop;
            this.action = action;
        }

        void disarm() {
            if (action == null) return; // Shutdown may already have consumed it.
            action = null;
            loop.schedule(this, io.zmux.internal.core.Time.NEVER);
            loop.removeShutdownHook(this);
        }

        @Override public void fire(long now) { run(); }
        @Override public void run() {
            var abort = action;
            disarm();
            if (abort != null) abort.accept(CLOSE_BOUND);
        }
    }

    @SuppressWarnings("StaticAssignmentOfThrowable") // a shared marker cause; the stack of its creation says nothing
    private static final Throwable CLOSE_BOUND = new java.io.IOException("graceful close bounded by the linger timeout");

    /** Detaches and closes every transport (the session ended). */
    void closeAll() {
        for (Attached a : new ArrayList<>(list)) remove(a, null);
    }

    // ---- the release of closed carriers ----

    private final AttachmentRelease sessionRelease = new AttachmentRelease();
    private final java.util.concurrent.ConcurrentMap<Long, java.util.concurrent.CompletableFuture<Void>> retiring =
            new java.util.concurrent.ConcurrentHashMap<>();

    java.util.concurrent.CompletionStage<Void> pipeClosed(long pipe) {
        var done = retiring.get(pipe);
        return done == null ? Transport.RELEASED : done.minimalCompletionStage();
    }

    java.util.concurrent.CompletableFuture<Void> released() {
        sessionRelease.close();
        return sessionRelease.completion();
    }

    private static void release(Attached attached) {
        attached.nesting.run();
    }
}
