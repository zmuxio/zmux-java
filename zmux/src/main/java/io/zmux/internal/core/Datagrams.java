package io.zmux.internal.core;

import io.zmux.internal.wire.ChannelDatagram;
import io.zmux.internal.wire.Frame;
import io.zmux.carrier.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;

/**
 * Datagram queues of one session (EXTENSIONS.md Section 6): the send queue, the session and
 * per-stream receive queues, the receive byte budget and the receive-limiter tokens. Owned by
 * the session's loop; the session resolves flows to streams and supplies the peer's limits.
 */
public final class Datagrams {
    /** Resolves a received datagram flow to its stream. */
    interface FlowLookup {
        /** The stream of a flow, or null; a flow no stream can have is a protocol violation. */
        Stream flow(long streamId) throws WireException;

        /** Whether streamId names a peer stream within the limit that has not been opened yet. */
        boolean unopened(long streamId);

        /** When a datagram held now for a stream not opened yet expires: one smoothed RTT later. */
        long holdUntil();
    }

    /**
     * How DATAGRAM frames may use the datagram channel of the pipe they go to (EXTENSIONS.md
     * Section 6.8): room 0 when they go in records.
     *
     * @param room the largest channel datagram the channel takes now
     * @param id the Pipe Stream ID, or {@link ChannelDatagram#NO_ID}
     */
    record ChannelUse(int room, long id) {
        static final ChannelUse NONE = new ChannelUse(0, ChannelDatagram.NO_ID);

        /** Whether a DATAGRAM frame of n bytes of data goes on the channel. */
        boolean takes(int n) {
            return room > 0 && ChannelDatagram.length(id, n) <= room;
        }
    }

    /** The session's view of the pipe DATAGRAM frames go to and its channel. */
    interface Channels {
        /** The pipe DATAGRAM frames go to now (MULTIPATH.md Section 11.1 rule S7), or null. */
        Pipe datagramPipe();

        /** How DATAGRAM frames for p may use its carrier's channel. */
        ChannelUse of(Pipe p);
    }

    private static final Channels NO_CHANNELS = new Channels() {
        @Override public Pipe datagramPipe() { return null; }
        @Override public ChannelUse of(Pipe p) { return ChannelUse.NONE; }
    };

    /** A received datagram for a peer stream not opened yet (EXTENSIONS.md Sections 6.4 and 6.8). */
    private record Held(long id, byte[] e, long until, boolean channel) {
        /** What it counts against the receive byte limit: its bytes and a fixed overhead, which bounds how many are held. */
        long cost() { return (long) e.length + HELD_COST; }
    }

    /** The overhead a held datagram counts against the receive byte limit besides its bytes. */
    static final int HELD_COST = 64;

    /** A stream datagram with its context. */
    public record StreamDatagram(long context, byte[] payload) {}

    /**
     * A driver admission token for a received datagram under a receive limiter. No payload
     * leaves the core while application code runs.
     */
    public static final class Limited {
        private final Stream stream;
        private byte[] payload;
        private final int bytes;
        private volatile boolean active = true;

        Limited(Stream stream, byte[] payload) {
            this.stream = stream;
            this.payload = payload;
            bytes = stream == null ? payload.length : payload.length - VarInt.lengthOf(payload[0]);
        }

        public int bytes() { return bytes; }
        public boolean active() { return active; }

        void revoke() {
            active = false;
            payload = null;
        }
    }

    /** A queued datagram: its encoding (flow prefix included), stream and payload length. */
    private record Out(byte[] data, Stream st, int payload) {
        boolean ready() { return st == null || st.tab.opened(); }
        boolean dead() { return st != null && !st.tab.opened() && st.snd.cancelled(); }
    }

    /** One stream's receive queue and its waiters. */
    private static final class PerStream {
        final ArrayDeque<byte[]> q = new ArrayDeque<>(2);
        /** Receivers waiting for a datagram; one is woken per datagram, every one on an end. */
        final WaitList waiters = new WaitList();
    }

    private final CoreConfig cfg;
    private final SessionStats stats;
    private final Outbox outbox;

    private final ArrayDeque<Out> send = new ArrayDeque<>(0);
    private final ArrayDeque<byte[]> recv = new ArrayDeque<>(0);
    private final WaitList recvW = new WaitList();
    private final WaitList sendW = new WaitList();
    private final IdentityHashMap<Stream, PerStream> streams = new IdentityHashMap<>(4);
    private final ArrayDeque<Limited> limited = new ArrayDeque<>(0);
    private final IdentityHashMap<byte[], Limited> pending = new IdentityHashMap<>(4);
    /** Every received datagram still held by the core, oldest first, with its stream (null: session). */
    private final LinkedHashMap<byte[], Stream> received = new LinkedHashMap<>();
    private long receivedBytes;
    /** Received datagrams of peer streams not opened yet, in arrival order (and of expiry), and their cost. */
    private final ArrayDeque<Held> held = new ArrayDeque<>(0);
    private long heldBytes;
    private Channels channels = NO_CHANNELS;

    private boolean peerKnown;
    private long peerMax;
    private long peerMaxRecord;
    private boolean flows;

    Datagrams(CoreConfig cfg, SessionStats stats, Outbox outbox) {
        this.cfg = cfg;
        this.stats = stats;
        this.outbox = outbox;
    }

    /** Sets the session's view of the datagram pipe and its channel. */
    void channels(Channels c) {
        channels = c;
    }

    /** The peer's parameters are known (SPEC.md Section 6.1). */
    void peer(long maxDatagramFrame, long maxRecord, boolean flowsNegotiated) {
        peerKnown = true;
        peerMax = maxDatagramFrame;
        peerMaxRecord = maxRecord;
        flows = flowsNegotiated;
    }

    /** The largest datagram payload the peer accepts now with a flow prefix of flowLen bytes, 0 when none. */
    public int maxPayload(int flowLen) {
        if (!peerKnown || peerMax == 0) return 0;
        long m = Math.min(peerMax, peerMaxRecord);
        if (flows) m -= flowLen;
        return m < 1 ? 0 : (int) (m - 1);
    }

    /** Bytes of received datagrams the core holds (memory accounting). */
    long receivedBytes() {
        return receivedBytes + heldBytes;
    }

    // ---- sending ----

    /**
     * Queues a datagram; a full queue drops its oldest entry. st is null for a session datagram;
     * a stream datagram's stream has its id. prefixed: a session datagram carries flow 0.
     */
    void send(Stream st, long context, byte[] payload, boolean prefixed) throws CoreException {
        ByteBuffer prefix = ByteBuffer.allocate(16);
        if (st != null) {
            VarInt.put(prefix, st.tab.id() + 1);
            VarInt.put(prefix, context);
        } else if (prefixed) {
            prefix.put((byte) 0);
        }
        int pl = prefix.position();
        if (peerKnown) {
            if (peerMax == 0) throw CoreException.of(CoreException.Reason.NOT_NEGOTIATED);
            int m = maxPayload(pl);
            if (payload.length > m) throw CoreException.tooLarge(m);
        }
        byte[] data = new byte[pl + payload.length];
        System.arraycopy(prefix.array(), 0, data, 0, pl);
        System.arraycopy(payload, 0, data, pl, payload.length);
        if (send.size() >= cfg.datagramSendQueue()) {
            send.poll();
            stats.datagramDroppedSend();
        }
        send.add(new Out(data, st, payload.length));
    }

    /** A send limiter refused a datagram before queueing it. */
    public void limitedSend() {
        stats.datagramDroppedLimit();
        stats.datagramDroppedSend();
    }

    public boolean sendQueueFull() {
        return send.size() >= cfg.datagramSendQueue();
    }

    boolean queued() {
        return !send.isEmpty();
    }

    /**
     * Whether a queued datagram goes in a record (or is dropped) now; those for the datagram
     * pipe's channel go by {@link #pollChannel}.
     */
    boolean sendable() {
        if (!peerKnown || send.isEmpty()) return false;
        ChannelUse ch = channels.of(channels.datagramPipe());
        for (Out o : send) if (o.dead() || o.ready() && !ch.takes(o.data().length)) return true;
        return false;
    }

    /**
     * Writes queued datagrams into w, records of p; those behind an unopened stream and those
     * for p's datagram channel keep their place.
     */
    void write(RecordWriter w, Pipe p) {
        ChannelUse ch = p == channels.datagramPipe() ? channels.of(p) : ChannelUse.NONE;
        ArrayDeque<Out> kept = null;
        while (!send.isEmpty()) {
            Out o = send.peek();
            if (o.dead() || o.data().length + 1 > peerMax) {
                send.poll();
                stats.datagramDroppedSend();
                continue;
            }
            if (!o.ready() || ch.takes(o.data().length)) {
                if (kept == null) kept = new ArrayDeque<>(2);
                kept.add(send.remove());
                continue;
            }
            if (!w.datagram(o.data())) break;
            send.poll();
            stats.datagramSent();
            sentBytes(o.st(), o.payload());
            Waiter sw = sendW.poll();
            if (sw != null) outbox.wake(sw);
        }
        if (kept != null) {
            kept.addAll(send);
            send.clear();
            send.addAll(kept);
        }
    }

    /**
     * Moves the queued datagrams that fit channel ch into dst as channel datagrams, in queue
     * order (EXTENSIONS.md Section 6.8); they count as sent. Returns how many.
     */
    int pollChannel(ChannelUse ch, java.util.List<byte[]> dst, java.util.List<ChannelSent> sent) {
        if (!peerKnown || send.isEmpty() || ch.room() == 0) return 0;
        int n = 0;
        for (var it = send.iterator(); it.hasNext(); ) {
            Out o = it.next();
            if (o.dead() || o.data().length + 1 > peerMax) {
                it.remove();
                stats.datagramDroppedSend();
                continue;
            }
            if (!o.ready() || !ch.takes(o.data().length)) continue;
            it.remove();
            dst.add(ChannelDatagram.encode(ch.id(), o.data()));
            sent.add(new ChannelSent(o.st(), o.payload()));
            stats.datagramSentChannel();
            sentBytes(o.st(), o.payload());
            n++;
            Waiter sw = sendW.poll();
            if (sw != null) outbox.wake(sw);
        }
        return n;
    }

    /** A channel datagram {@link #pollChannel} returned: what its refusal by the carrier takes back. */
    public record ChannelSent(Stream st, int payload) {}

    private void sentBytes(Stream st, int payload) {
        stats.datagramBytesSent(payload);
        if (st != null) {
            st.counters.datagramSent(1, payload);
        }
    }

    /** The carrier refused a channel datagram counted as sent: it is dropped instead. */
    void channelRefused(ChannelSent c) {
        stats.channelSendFailed(c.payload());
        if (c.st() != null) {
            c.st().counters.datagramSent(-1, -c.payload());
        }
    }

    public void waitSend(Waiter w) {
        sendW.add(w);
    }

    public void cancelSendWait(Waiter w) {
        sendW.remove(w);
    }

    // ---- receiving ----

    /**
     * Handles a DATAGRAM frame, from a record or (channel) a carrier's datagram channel; lookup
     * resolves a stream flow.
     */
    void onFrame(Frame f, FlowLookup lookup, boolean channel) throws WireException {
        ByteBuffer b = f.src;
        int pos = f.dataPos, end = f.dataPos + f.dataLen;
        if (flows) {
            long flow = VarInt.get(b, pos, end);
            if (flow < 0) throw WireException.protocol("truncated datagram flow");
            pos += VarInt.lengthOf(b.get(pos));
            if (flow > 0) {
                long ctx = VarInt.get(b, pos, end);
                if (ctx < 0) throw WireException.protocol("truncated datagram context");
                int cn = VarInt.lengthOf(b.get(pos));
                long id = flow - 1;
                Stream st = lookup.flow(id);
                if (st == null && lookup.unopened(id)) {
                    // It overtook the stream's opening frame (EXTENSIONS.md 6.4, 6.8): held for at most one
                    // smoothed RTT, counted as received once delivered.
                    hold(id, streamDatagram(ctx, b, pos + cn, end), lookup.holdUntil(), channel);
                    return;
                }
                if (st == null || st.tab.discarded() || st.rcv.stopped()) {
                    dropReceived();
                    return;
                }
                countReceived(channel);
                received(st, streamDatagram(ctx, b, pos + cn, end));
                return;
            }
        }
        byte[] payload = new byte[end - pos];
        b.get(pos, payload);
        countReceived(channel);
        received(null, payload);
    }

    /** A stream datagram as its queue holds it: the context ID, then the payload b[from, end). */
    private static byte[] streamDatagram(long ctx, ByteBuffer b, int from, int end) {
        byte[] e = new byte[VarInt.length(ctx) + end - from];
        int i = VarInt.put(e, 0, ctx);
        b.get(from, e, i, end - from);
        return e;
    }

    // ---- datagrams for peer streams not opened yet ----

    /**
     * Counts a datagram delivered to a receive queue (or the receive limiter); one dropped
     * before, on arrival or while held, counts as dropped only.
     */
    private void countReceived(boolean channel) {
        stats.datagramReceived();
        if (channel) stats.datagramsReceivedChannel(1);
    }

    /**
     * Holds a datagram for the peer stream id, not opened yet, within the byte limit: for one
     * smoothed RTT, but not past one held after it, so that held datagrams expire in arrival order.
     */
    private void hold(long id, byte[] e, long until, boolean channel) {
        Held last = held.peekLast();
        if (last != null && until < last.until) until = last.until;
        Held h = new Held(id, e, until, channel);
        held.add(h);
        heldBytes += h.cost();
        trimBytes();
    }

    private void dropHeldHead() {
        heldBytes -= held.remove().cost();
        dropReceived();
    }

    /** Delivers the datagrams held for a peer stream that opened, in arrival order. */
    void releaseHeld(Stream st) {
        if (held.isEmpty()) return;
        long id = st.tab.id();
        ArrayDeque<Held> mine = null;
        for (var it = held.iterator(); it.hasNext(); ) {
            Held h = it.next();
            if (h.id != id) continue;
            it.remove();
            heldBytes -= h.cost();
            if (mine == null) mine = new ArrayDeque<>(2);
            mine.add(h);
        }
        if (mine != null) for (Held h : mine) { countReceived(h.channel); received(st, h.e); }
    }

    /** When the first held datagram expires (the oldest), {@link Time#NEVER} when none is held. */
    long heldDeadline() {
        Held h = held.peek();
        return h == null ? Time.NEVER : h.until;
    }

    /** Drops the held datagrams whose stream did not open in time, oldest first. */
    void expireHeld(long now) {
        while (!held.isEmpty() && held.peek().until <= now) dropHeldHead();
    }

    /** A received datagram (stream datagrams carry their context prefix). */
    void received(Stream st, byte[] e) {
        if (cfg.datagramReceiveLimited()) {
            Limited token = new Limited(st, e);
            limited.add(token);
            pending.put(e, token);
            track(st, e);
            trimCount(st);
            trimBytes();
            return;
        }
        queue(st, e);
    }

    /** Takes one limiter token, its payload retained within the datagram budget. */
    public Limited pollLimited() {
        return limited.poll();
    }

    /**
     * Applies a limiter decision once; evicted tokens have no effect and never resurrect data.
     * True when the datagram was queued for the application.
     */
    boolean deliver(Limited p, boolean drop, boolean closing) {
        if (!p.active || p.payload == null) return false;
        byte[] bytes = p.payload;
        Stream stream = p.stream;
        untrack(bytes);
        if (drop || closing || stream != null && (stream.tab.discarded() || stream.rcv.stopped())) {
            if (drop) stats.datagramDroppedLimit();
            dropReceived();
            return false;
        }
        queue(stream, bytes);
        return received.containsKey(bytes);
    }

    private void queue(Stream st, byte[] e) {
        if (st != null) {
            if (st.tab.discarded() || st.rcv.stopped()) {
                dropReceived();
                return;
            }
            PerStream ps = streams.computeIfAbsent(st, k -> new PerStream());
            if (ps.q.size() >= cfg.datagramStreamQueue()) {
                untrack(ps.q.poll());
                dropReceived();
                st.counters.datagramDropped();
            }
            ps.q.add(e);
            st.counters.datagramQueued();
            track(st, e);
            trimBytes();
            ps.waiters.wakeFirst(outbox);
            outbox.wake(st.rcv.rsig());
            return;
        }
        if (recv.size() >= cfg.datagramReceiveQueue()) {
            untrack(recv.poll());
            dropReceived();
        }
        recv.add(e);
        track(null, e);
        trimBytes();
        recvW.wakeFirst(outbox);
    }

    void dropReceived() {
        stats.datagramDroppedRecv();
        outbox.event(Event.datagramDropped(stats.datagramsDroppedRecv()));
    }

    private void track(Stream stream, byte[] bytes) {
        received.put(bytes, stream);
        receivedBytes += bytes.length;
    }

    private void untrack(byte[] bytes) {
        if (!received.containsKey(bytes)) return;
        received.remove(bytes);
        receivedBytes -= bytes.length;
        Limited token = pending.remove(bytes);
        if (token != null) {
            token.revoke();
            limited.remove(token);
        }
    }

    private void evict(byte[] bytes, Stream stream) {
        if (!pending.containsKey(bytes)) {
            if (stream == null) {
                recv.remove(bytes);
            } else {
                PerStream ps = streams.get(stream);
                if (ps != null) ps.q.remove(bytes);
            }
        }
        untrack(bytes);
        dropReceived();
    }

    private void trimCount(Stream stream) {
        int count = 0;
        for (Stream target : received.values()) if (target == stream) count++;
        int max = stream == null ? cfg.datagramReceiveQueue() : cfg.datagramStreamQueue();
        while (count > max) {
            for (var entry : received.entrySet()) {
                if (entry.getValue() == stream) {
                    evict(entry.getKey(), stream);
                    break;
                }
            }
            count--;
        }
    }

    /** Keeps received datagrams within the byte limit: the oldest held ones go first, then the oldest received. */
    private void trimBytes() {
        while (receivedBytes + heldBytes > cfg.datagramReceiveBytes() && !held.isEmpty()) dropHeldHead();
        while (receivedBytes > cfg.datagramReceiveBytes() && !received.isEmpty()) {
            var first = received.firstEntry();
            evict(first.getKey(), first.getValue());
        }
    }

    /** Pops a session datagram, or null. */
    public byte[] receive() {
        byte[] e = recv.poll();
        if (e == null) return null;
        untrack(e);
        // One waiter is woken per datagram; several queued at once each pass one on.
        if (!recv.isEmpty()) recvW.wakeFirst(outbox);
        stats.datagramBytesReceived(e.length);
        return e;
    }

    public void waitReceive(Waiter w) {
        recvW.add(w);
    }

    public void cancelReceiveWait(Waiter w) {
        recvW.remove(w);
    }

    /** Pops a stream datagram, or null. */
    public StreamDatagram receive(Stream st) {
        PerStream ps = streams.get(st);
        if (ps == null || ps.q.isEmpty()) return null;
        byte[] e = ps.q.poll();
        untrack(e);
        if (!ps.q.isEmpty()) ps.waiters.wakeFirst(outbox);
        ByteBuffer b = ByteBuffer.wrap(e);
        long ctx = VarInt.get(b, 0, e.length);
        int n = VarInt.lengthOf(e[0]);
        stats.datagramBytesReceived(e.length - n);
        st.counters.datagramReturned(e.length - n);
        return new StreamDatagram(ctx, Arrays.copyOfRange(e, n, e.length));
    }

    public void waitReceive(Stream st, Waiter w) {
        streams.computeIfAbsent(st, k -> new PerStream()).waiters.add(w);
    }

    public void cancelReceiveWait(Stream st, Waiter w) {
        PerStream ps = streams.get(st);
        if (ps != null) ps.waiters.remove(w);
    }

    // ---- ends ----

    /** Queues every waiter of st's datagrams, keeping them registered (the stream's state changed). */
    void wakeStream(Stream st) {
        PerStream ps = streams.get(st);
        if (ps != null) ps.waiters.wakeEvery(outbox);
    }

    /** Wakes and withdraws the session-level waiters (close, GOAWAY). */
    void wakeAll() {
        recvW.wakeAll(outbox);
        sendW.wakeAll(outbox);
    }

    /** A released stream: its queued and limiter-held datagrams go. */
    void dropStream(Stream st) {
        PerStream ps = streams.remove(st);
        if (ps != null) for (byte[] e : ps.q) untrack(e);
        for (var entry : new ArrayList<>(received.entrySet())) {
            if (entry.getValue() == st) evict(entry.getKey(), st);
        }
    }

    /** Drops every buffer (CLOSED); outstanding limiter tokens are revoked. */
    void releaseAll() {
        for (Limited token : pending.values()) token.revoke();
        limited.clear();
        pending.clear();
        received.clear();
        held.clear();
        heldBytes = 0;
        recv.clear();
        send.clear();
        streams.clear();
        receivedBytes = 0;
    }
}
