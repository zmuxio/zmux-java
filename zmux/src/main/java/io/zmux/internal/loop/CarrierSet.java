package io.zmux.internal.loop;

import io.zmux.internal.core.Batch;
import io.zmux.internal.core.ReadBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;

/**
 * The transports of one session and what each is to it (IMPLEMENTATION.md Section 2.8):
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
        final long pipe;
        final long lane;
        /** The batch of the write in flight, reused across writes. */
        final Batch batch = new Batch();
        private final Runnable nesting;
        private ConnectionCharge charge;
        private boolean removed;
        /** Stops the receiving of the pipe's datagram channel; null without one. */
        private Runnable channelStop;

        private Attached(Transport t, long pipe, long lane, Runnable nesting) {
            this.t = t;
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
        a = new Attached(t, pipe, lane, nesting == null ? () -> {} : nesting);
        list.add(a);
        charge(a);
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

    int size() { return list.size(); }

    Attached get(int i) { return list.get(i); }

    /** Detaches a and closes its transport (abortive with a cause); idempotent. */
    void remove(Attached a, Throwable cause) {
        if (a.removed) return;
        a.removed = true;
        list.remove(a);
        if (a.channelStop != null) a.channelStop.run();
        a.t.close(cause);
        release(a);
    }

    /** Detaches and closes every transport (the session ended). */
    void closeAll() {
        for (Attached a : new ArrayList<>(list)) remove(a, null);
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
