package io.zmux.internal.loop;

import io.zmux.internal.spi.AttachmentRelease;
import io.zmux.internal.spi.CarrierProvider;
import io.zmux.internal.core.Action;

import io.zmux.internal.core.Batch;
import io.zmux.internal.core.ReadBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
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

    CarrierSet(SessionRuntime rt) {
        this.rt = rt;
    }

    /** A transport attached to the session: the inbound it reports to. */
    final class Attached implements Inbound {
        final Transport t;
        private final CarrierSetup.Bound closeRight;
        final long pipe;
        final long lane;
        /** The batch of the write in flight, reused across writes. */
        final Batch batch = new Batch();
        private final Runnable nesting;
        private ConnectionCharge charge;
        private boolean removed;
        private WatchedSender watchedSender;
        final AttachmentRelease attachment;
        private final AttachmentRelease physicalRelease = sessionRelease.fork();
        /** Stops the receiving of the pipe's datagram channel; null without one. */
        private Runnable channelStop;

        private Attached(CarrierSetup.Bound binding, long pipe, long lane, Runnable nesting, AttachmentRelease attachment) {
            this.attachment = attachment;
            closeRight = binding;
            t = binding.carrier;
            this.pipe = pipe;
            this.lane = lane;
            this.nesting = nesting;
        }

        @Override public boolean session() { return true; }

        @Override
        public int bytes(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            int n = rt.core.receive(pipe, lane, buf, data, pos, lim, Clock.now());
            rt.markDirty();
            return n;
        }

        @Override
        public void message(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            rt.core.receiveMessage(pipe, lane, buf, data, pos, lim, Clock.now());
            rt.markDirty();
        }

        @Override
        public void ended(Transport t, boolean graceful, boolean partial) {
            rt.core.pipeEnded(pipe, lane, graceful, partial, graceful ? null : t.inputError(), Clock.now());
            rt.markDirty();
        }

        @Override
        public void writable(Transport t, boolean on) {
            rt.core.setWritable(pipe, lane, on, Clock.now());
            rt.markDirty();
        }

        @Override
        public void written(Transport t, Batch b, Throwable cause) {
            rt.written(t, b, cause);
        }

        @Override
        public void outputShut(Transport t) {
            rt.core.outputShut(pipe, lane, Clock.now());
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
        list.add(a);
        charge(a);
        attachCarrierSample(a);
        CarrierChannel ch = lane == 0 ? t.channel() : null;
        if (ch != null) {
            Attached at = a;
            a.channelStop = ch.receive((b, pos, end) -> {
                if (at.removed) return;
                rt.core.receiveChannelDatagram(pipe, b, pos, end, Clock.now());
                rt.markDirty();
            });
        }
        t.taken();
        t.start(a);
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
        list.remove(a);
        detachCarrierSample(a);
        if (a.channelStop != null) a.channelStop.run();
        if (rt.core.state().compareTo(io.zmux.internal.core.SessionState.CLOSING) >= 0) a.t.sessionEnded(rt.core.error());
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
        rt.loop.schedule(deadline, io.zmux.internal.core.Time.add(Clock.now(), rt.core.lingerTimeout()));
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

        @Override void fire(long now) { run(); }
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

    // ---- memory charges, one per connection identity ----

    private static final class ConnectionCharge {
        long bytes;
        Runnable release;
        int refs;
        ConnectionCharge(long bytes, Runnable release) { this.bytes = bytes; this.release = release; }
    }

    private final IdentityHashMap<Object, ConnectionCharge> connections = new IdentityHashMap<>();

    private void charge(Attached a) {
        Object connection = a.t.info().connection();
        long bytes = a.t.info().receiveMemory();
        if (connection == null) return;
        var core = rt.core;
        ConnectionCharge charge = connections.get(connection);
        if (charge == null) {
            Runnable release;
            if (core.sharedBudget() == null) {
                core.memory().adjustCarrier(bytes); release = () -> {};
            } else release = core.sharedBudget().acquireCarrier(connection, bytes);
            charge = new ConnectionCharge(bytes, release); connections.put(connection, charge);
        } else if (bytes > charge.bytes) {
            if (core.sharedBudget() == null) core.memory().adjustCarrier(bytes - charge.bytes);
            else {
                Runnable updated = core.sharedBudget().acquireCarrier(connection, bytes);
                charge.release.run(); charge.release = updated;
            }
            charge.bytes = bytes;
        }
        charge.refs++;
        a.charge = charge;
    }

    private void release(Attached a) {
        a.nesting.run();
        ConnectionCharge charge = a.charge;
        a.charge = null;
        if (charge != null && --charge.refs == 0) {
            connections.remove(a.t.info().connection());
            if (rt.core.sharedBudget() == null) rt.core.memory().adjustCarrier(-charge.bytes);
            else charge.release.run();
        }
    }
}
