package io.zmux.internal.core;

import io.zmux.internal.buffer.ReceiveLoans;
import io.zmux.internal.buffer.ReadBuf;
import java.util.function.Predicate;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
import io.zmux.internal.buffer.CarrierCharge;

import io.zmux.DatagramSendOptions;
import io.zmux.DatagramSendOptions.Carriage;
import io.zmux.internal.wire.ChannelDatagram;
import io.zmux.shaping.RuntimeReason;
import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import io.zmux.internal.wire.WireException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;

/**
 * Datagram queues of one session: the send queue, the session and
 * per-stream receive queues, the receive byte budget and the receive-limiter tokens. Owned by
 * the session's loop; the session resolves flows to streams and supplies the peer's limits.
 * Every queue bounds its datagrams' sojourn with its own {@link CoDel} state
 * ({@link CoreConfig#datagramQueueDelay()}), decided as each datagram leaves it at the session clock.
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
     * How DATAGRAM frames may use the datagram channel of the pipe they go to: room 0 when they go in records.
     *
     * @param limit the largest channel datagram the channel supports, or -1 while unknown
     * @param room the largest channel datagram the channel takes now
     * @param id the Pipe Stream ID, or {@link ChannelDatagram#NO_ID}
     */
    record ChannelUse(int limit, int room, long id, boolean padding) {
        static final ChannelUse NONE = new ChannelUse(0, 0, ChannelDatagram.NO_ID, false);

        int paddedLimit() {
            return Math.min(room, ChannelDatagram.MAX_PADDED_FRAME + ChannelDatagram.length(id, 0) - 1);
        }

        /** Whether a DATAGRAM frame of n bytes of data goes on the channel. */
        boolean takes(int n) {
            return room > 0 && ChannelDatagram.length(id, n) <= room;
        }

        boolean supports(int n) { return limit < 0 || (limit > 0 && ChannelDatagram.length(id, n) <= limit); }
    }

    record CopyPath(Pipe pipe, boolean nativePath) {}

    /** The session's view of the pipe DATAGRAM frames go to and its channel. */
    interface Channels {
        /** The pipe DATAGRAM frames go to now, or null. */
        Pipe datagramPipe();

        /** How DATAGRAM frames for p may use its carrier's channel. */
        ChannelUse of(Pipe p);

        /**
         * The cheapest eligible carrier of this datagram, evaluated immediately before dispatch.
         * Test drivers can supply only the queried pipe.
         */
        default Pipe pick(Stream st, Pipe p, int length, long deadline, Carriage carriage) { return p; }

        default int recordRoom() { return 0; }

        default int nativeLimit() {
            var use = of(datagramPipe());
            return use.limit() - ChannelDatagram.length(use.id(), 0);
        }

        default int nativeRoom() {
            var use = of(datagramPipe());
            return use.room() - ChannelDatagram.length(use.id(), 0);
        }

        default boolean fixedRecordPath() { return true; }
        default boolean nativeNegotiated() { return false; }

        default boolean redundant(Stream stream, Pipe original) { return false; }

        default CopyPath copyPath(Stream stream, Pipe original, Set<Pipe> selected, int length,
                                  long deadline, Carriage carriage, boolean distinct, boolean adaptive,
                                  DatagramFeedback.Path impaired) { return null; }

        default boolean copyEligible(Stream stream, Pipe original, Pipe candidate, Set<Pipe> selected,
                                     int length, long deadline, Carriage carriage, boolean distinct) {
            return carries(stream, candidate);
        }

        default void wake(Pipe pipe) {}

        /** Whether a DATAGRAM frame of stream st (null: a session datagram) may go on p now. */
        default boolean carries(Stream st, Pipe p) { return true; }
    }

    private static final Channels NO_CHANNELS = new Channels() {
        @Override public Pipe datagramPipe() { return null; }
        @Override public ChannelUse of(Pipe p) { return ChannelUse.NONE; }
    };

    /** A received datagram for a peer stream not opened yet. */
    private record Held(long id, long sequence, byte[] e, long until, boolean channel) {
        /** What it counts against the receive byte limit: its bytes and a fixed overhead, which bounds how many are held. */
        long cost() { return (long) e.length + HELD_COST; }
    }

    /** The overhead a held datagram counts against the receive byte limit besides its bytes. */
    static final int HELD_COST = 64;

    /** A stream datagram with its context. */
    public record StreamDatagram(long context, byte[] payload, long expiry) {
        public StreamDatagram(long context, byte[] payload) { this(context, payload, Time.NEVER); }
    }

    /**
     * A driver admission token for a received datagram under a receive limiter. No payload
     * leaves the core while application code runs.
     */
    public static final class Limited {
        private final Stream stream;
        private byte[] payload;
        private final int bytes;
        private final long sequence;
        private volatile boolean active = true;

        Limited(Stream stream, byte[] payload, long sequence) {
            this.sequence = sequence;
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

    /** The delay already spent belongs to the queued unit across policy generations. */
    abstract static class NativeWork {
        final Shaping.Delay delay = new Shaping.Delay();
        abstract int dataLengthAt(Datagrams direction);
        abstract long eligibleSince();
        abstract long expiry();
    }

    /** A logical datagram waiting for its first dispatch. */
    static final class Out extends NativeWork {
        final byte[] payload;
        final int offset, length;
        final ReadBuf owner;
        final Stream st;
        final long context, at, deadline;
        final DatagramSendOptions options;
        final CarrierCharge charge;
        private long queuedAt;
        private Shaping.PipeShape reserved;

        Out(byte[] payload, Stream st, long context, long at, long deadline, DatagramSendOptions options, CarrierCharge charge) {
            this(payload, 0, payload.length, null, st, context, at, deadline, options, charge);
        }

        Out(byte[] payload, int offset, int length, ReadBuf owner, Stream st, long context,
            long at, long deadline, DatagramSendOptions options, CarrierCharge charge) {
            this.payload = payload;
            this.offset = offset;
            this.length = length;
            this.owner = owner;
            this.st = st;
            this.context = context;
            this.at = at;
            this.deadline = deadline;
            this.options = options;
            this.charge = charge;
        }

        void retain() {
            if (owner != null) owner.ref();
            if (charge != null) charge.retain();
        }

        void release() {
            if (owner != null) owner.unref();
            if (charge != null) charge.release();
        }

        void attachCharge(RecordWriter writer) { writer.chargeDatagram(charge, length); }

        void unreserve() {
            if (reserved != null) reserved.clearNative();
        }

        void cancelReservation(RuntimeReason reason) {
            if (reserved != null) reserved.cancelNative(reason);
        }

        void releaseReservation(Shaping.PipeShape owner) {
            if (reserved == owner) reserved = null;
        }

        @Override int dataLengthAt(Datagrams direction) { return direction.dataLength(this); }
        @Override long eligibleSince() { return queuedAt; }
        @Override long expiry() { return deadline; }

        int flowLength() {
            return st == null ? 1 : VarInt.length(st.tab.id() + 1) + VarInt.length(context);
        }
        boolean ready() { return st == null || st.tab.opened(); }
        boolean dead() { return st != null && !st.tab.opened() && st.snd.cancelled(); }
    }

    /** A received datagram in a receive queue and when it was queued. */
    private record Queued(byte[] e, long at) {}

    /** One stream's receive queue and its waiters. */
    private static final class PerStream {
        final ArrayDeque<Queued> q = new ArrayDeque<>(2);
        /** The queue's sojourn bound. */
        final CoDel codel = new CoDel();
        /** The payload bytes of this stream's datagrams the core holds (queued and awaiting the limiter). */
        private long bytes;
        /** Under a receive limiter: this stream's datagrams the core holds (awaiting the limiter and queued), oldest first. */
        private final ArrayDeque<byte[]> order = new ArrayDeque<>(0);
        /** Receivers waiting for a datagram; one is woken per datagram, every one on an end. */
        final WaitList waiters = new WaitList();
    }

    private final CoreConfig cfg;
    private final SessionStats stats;
    private final Outbox outbox;

    static final class SendFlow {
        final ArrayDeque<Out> queue = new ArrayDeque<>();
        private Shaping.DatagramHold hold;

        Shaping.DatagramHold hold() { return hold; }
        void hold(Shaping.DatagramHold next) { hold = next; }
        final CoDel codel = new CoDel();
        private long bytes, nativeBytes;
        private int nativeCount;
    }

    private static final class Copy extends NativeWork {
        final Out original;
        final Pipe first;
        final Set<Pipe> prior;
        final long sequence;
        final byte[] data;
        final DatagramFeedback.AdaptivePair adaptive;

        Copy(Out original, Pipe first, Set<Pipe> prior, long sequence, byte[] data, DatagramFeedback.AdaptivePair adaptive) {
            this.original = original;
            this.first = first;
            this.prior = prior;
            this.sequence = sequence;
            this.data = data;
            this.adaptive = adaptive;
        }

        @Override int dataLengthAt(Datagrams direction) { return data.length; }
        @Override long eligibleSince() { return original.queuedAt; }
        @Override long expiry() { return original.deadline; }

    }
    private final LinkedHashMap<Pipe, ArrayDeque<Copy>> copies = new LinkedHashMap<>();
    private int copyCount;
    private long sendDeadline = Time.NEVER;
    final CopyBudget budget;

    private final LinkedHashMap<Stream, SendFlow> sending = new LinkedHashMap<>();
    private final IdentityHashMap<Stream, SendFlow> nativeFlows = new IdentityHashMap<>();
    private int sendCount, nativeInFlight;
    private final ArrayDeque<Queued> recv = new ArrayDeque<>(0);
    private final CoDel recvCodel = new CoDel();
    private final WaitList recvW = new WaitList();
    private final WaitList sendW = new WaitList();
    private final IdentityHashMap<Stream, PerStream> streams = new IdentityHashMap<>(4);
    private final ArrayDeque<Limited> limited = new ArrayDeque<>(0);
    private final IdentityHashMap<byte[], Limited> pending = new IdentityHashMap<>(4);
    /** Every received datagram still held by the core, oldest first, with its stream (null: session). */
    @SuppressWarnings("ArrayAsKeyOfSetOrMap") // keyed by the held array itself: the core removes it by identity
    private final LinkedHashMap<byte[], Stream> received = new LinkedHashMap<>();
    /** The bytes of the session datagrams in {@link #received}; stream datagrams count against their stream's bound only. */
    private long receivedBytes;
    // Only receive allocations with a finite flow lifetime have an entry.
    private record Expiry(byte[] bytes, long deadline) {}

    private final IdentityHashMap<byte[], Expiry> receiveExpiry = new IdentityHashMap<>(0);
    private final TreeMap<Long, Set<Expiry>> expiring = new TreeMap<>();

    private void expiresAt(byte[] bytes, long deadline) {
        if (deadline == Time.NEVER) return;
        var expiry = new Expiry(bytes, deadline);
        receiveExpiry.put(bytes, expiry);
        expiring.computeIfAbsent(deadline, ignored -> new HashSet<>()).add(expiry);
    }

    private long forgetExpiry(byte[] bytes) {
        var expiry = receiveExpiry.remove(bytes);
        if (expiry == null) return Time.NEVER;
        var bucket = expiring.get(expiry.deadline());
        bucket.remove(expiry);
        if (bucket.isEmpty()) expiring.remove(expiry.deadline());
        return expiry.deadline();
    }

    private long expiryOf(byte[] bytes) {
        var expiry = receiveExpiry.get(bytes);
        return expiry == null ? Time.NEVER : expiry.deadline();
    }
    /** The bytes of the stream datagrams in {@link #received}, for the memory account. */
    private long streamBytes;
    /** Received datagrams of peer streams not opened yet, in arrival order (and of expiry), and their cost. */
    private final ArrayDeque<Held> held = new ArrayDeque<>(0);
    private long heldBytes;
    private Channels channels = NO_CHANNELS;

    private boolean peerKnown;
    private long peerMax;
    private long peerMaxRecord;
    private boolean sequenced;
    private long nextSequence;
    private final DatagramWindow receiveWindow;
    final DatagramFeedback feedback;

    private boolean exhausted() { return exhausted(1); }

    boolean exhausted(int count) { return sequenced && count - 1L > VarInt.MAX - nextSequence; }
    int sequenceWidth() { return exhausted() ? -1 : sequenceLength(); }

    private int sequenceLength() { return sequenced ? VarInt.length(Math.min(VarInt.MAX, nextSequence)) : 0; }
    private int dataLength(Out out) {
        return out.length + (sequenced ? sequenceLength() + out.flowLength() : 0);
    }

    private byte[] encode(Out out) {
        if (!sequenced && out.length == out.payload.length) return out.payload;
        byte[] data = new byte[dataLength(out)];
        encodeInto(out, data, 0);
        return data;
    }

    private void encodeInto(Out out, byte[] data, int from) {
        int n = from;
        if (sequenced) n = VarInt.put(data, n, nextSequence);
        if (sequenced) n = VarInt.put(data, n, out.st == null ? 0 : out.st.tab.id() + 1);
        if (out.st != null) n = VarInt.put(data, n, out.context);
        System.arraycopy(out.payload, out.offset, data, n, out.length);
        if (out.charge != null) out.charge.encoded(n - from + out.length);
    }

    private long dispatched(Pipe pipe, boolean channel) {
        if (!sequenced) return -1;
        long sequence = nextSequence++;
        if (pipe != null) feedback.dispatched(pipe.id, channel, sequence, clock.getAsLong());
        return sequence;
    }
    /** The largest record cap of the usable pipes' carriers: a datagram beyond it fits on no pipe now. */
    private java.util.function.IntSupplier largestRecordCap = () -> Integer.MAX_VALUE;

    /** The session clock: when a datagram is queued and when it leaves. */
    private final java.util.function.LongSupplier clock;

    Datagrams(CoreConfig cfg, SessionStats stats, Outbox outbox, java.util.function.LongSupplier clock) {
        this.cfg = cfg;
        this.stats = stats;
        this.outbox = outbox;
        this.clock = clock;
        receiveWindow = new DatagramWindow(cfg.datagramWindow());
        feedback = new DatagramFeedback(cfg.sessionPolicy().limits().attachmentLimit());
        budget = new CopyBudget(cfg.copyPolicy().extraPercent(), clock.getAsLong());
    }

    /** Sets the session's view of the datagram pipe and its channel. */
    void channels(Channels c) {
        channels = c;
    }

    /** The peer's parameters are known. */
    void peer(long maxDatagramFrame, long maxRecord, boolean sameFamily) {
        peerKnown = true;
        peerMax = maxDatagramFrame;
        peerMaxRecord = maxRecord;
        sequenced = sameFamily;
        long configured = cfg.local().maxDatagramFrameSize;
        long frame = configured > 0 ? configured : TransportParams.DEFAULT_DATAGRAM_FRAME_CAP;
        if (maxDatagramFrame > 0) frame = Math.min(frame, maxDatagramFrame);
        int header = sameFamily ? 1 + 2 * VarInt.length(0) : 1;
        budget.negotiateBurst(Math.clamp(frame - header, 0, Integer.MAX_VALUE));
    }

    void recordCap(java.util.function.IntSupplier largest) {
        largestRecordCap = largest;
    }

    /**
     * The largest datagram data (flow prefix included) whose DATAGRAM frame, type and Length
     * included as the record writer always counts them, fits cap bytes; -1 when none does. The
     * one computation behind both the advertised maximum and the drop at write time.
     */
    static int maxData(long cap) {
        if (cap < 2) return -1;
        long d = Math.min(cap, Integer.MAX_VALUE) - 2;
        while (d > 0 && 1 + VarInt.length(d) + d > cap) d--;
        return (int) d;
    }

    /** The largest datagram payload the peer accepts now with a flow prefix of flowLen bytes, 0 when none. */
    public int maxPayload(int flowLen) { return Math.max(0, admissionPayload(flowLen)); }

    int admissionPayload(int flowLen) {
        if (!peerKnown || peerMax == 0 || exhausted()) return -1;
        int recordCap = largestRecordCap.getAsInt();
        if (recordCap == 0) recordCap = Integer.MAX_VALUE;
        return payload(Math.max(maxData(Math.min(Math.min(peerMax, peerMaxRecord), recordCap)),
                nativeData(channels.nativeLimit())), flowLen);
    }

    /** The negotiated upper bound across representations, independent of carrier availability. */
    int structuralPayload(int flowLen) {
        int record = maxData(Math.min(peerMax, peerMaxRecord));
        int nativeCap = channels.nativeNegotiated() ? (int) Math.min(peerMax - 1, Integer.MAX_VALUE) : -1;
        return payload(Math.max(record, nativeCap), flowLen);
    }

    /** Largest eligible payload through either supported carriage. */
    public int pathPayload(int flowLen) {
        if (!peerKnown || peerMax == 0 || exhausted()) return 0;
        int record = channels.recordRoom();
        return Math.max(0, payload(Math.max(maxData(Math.min(peerMax, record)), nativeData(channels.nativeRoom())), flowLen));
    }

    /** Largest eligible native-channel payload, or -1 when unavailable. */
    public int nativePayload(int flowLen) {
        return !peerKnown || peerMax == 0 || exhausted() ? -1 : payload(nativeData(channels.nativeRoom()), flowLen);
    }

    private int nativeData(int channel) {
        return (int) Math.min(peerMax - 1, channel);
    }

    private int payload(int data, int flow) {
        return Math.max(-1, sequenced ? data - flow - sequenceLength() : data);
    }

    /** Observed outgoing datagram delivery and loss feedback. */
    public record FeedbackSnapshot(long acknowledged, int pending, long expired, long unknown,
                                   int rttSamples, long roundTripNanos, long sampleAgeNanos) {}

    public FeedbackSnapshot feedback(long pipe, boolean nativePath) {
        return feedback.snapshot(pipe, nativePath, clock.getAsLong());
    }

    /** Bytes of received datagrams the core holds (memory accounting). */
    long receivedBytes() {
        return receivedBytes + heldBytes + streamBytes;
    }

    // ---- sending ----

    /** Queues a validated original. The sequence and encoding are chosen at dispatch. */
    void enqueueSingle(Stream st, long context, byte[] payload, long submitted, long deadline,
                       DatagramSendOptions options, CarrierCharge charge) {
        Out next = new Out(payload, st, context, submitted, deadline, options, charge);
        long flowLimit = st == null ? -1 : cfg.datagramStreamBytes();
        if (sendQueueFull()) {
            // sendFits allows eviction only when a queued original can make room.
            var oldest = sending.values().iterator().next();
            for (var flow : sending.values()) if (flow.queue.getFirst().at < oldest.queue.getFirst().at) oldest = flow;
            dropHead(oldest);
        }
        var flow = queuedFlow(st);
        if (st != null) {
            while (flow.queue.size() + flow.nativeCount >= cfg.datagramStreamQueue()
                    || (flowLimit >= 0 && payload.length > flowLimit - flow.bytes - flow.nativeBytes)) {
                if (flow.hold != null) flow.hold.close();
                var evicted = flow.queue.removeFirst();
                evicted.cancelReservation(RuntimeReason.CANCELLED);
                evicted.release();
                flow.bytes -= evicted.length;
                sendCount--;
                droppedSend();
            }
        }
        enqueue(flow, next);
    }

    void enqueueGroup(Stream st, long context, StreamRecv.Chunk[] payloads, long submitted, long deadline,
                      DatagramSendOptions options, CarrierCharge[] charges) {
        SendFlow flow = queuedFlow(st);
        for (int i = 0; i < payloads.length; i++) {
            StreamRecv.Chunk chunk = payloads[i];
            ByteBuffer payload = chunk.data();
            enqueue(flow, new Out(payload.array(), payload.arrayOffset() + payload.position(), payload.remaining(),
                    chunk.buf(), st, context, submitted, deadline, options, charges[i]));
        }
    }

    private SendFlow queuedFlow(Stream st) {
        return sending.computeIfAbsent(st, ignored -> {
            SendFlow inFlight = nativeFlows.get(st);
            return inFlight == null ? new SendFlow() : inFlight;
        });
    }

    boolean sendFits(Stream st, int count, long bytes, boolean group) {
        long limit = st == null ? -1 : cfg.datagramStreamBytes();
        SendFlow flow = sending.get(st);
        if (flow == null) flow = nativeFlows.get(st);
        int slots = flow == null ? 0 : flow.nativeCount + (group ? flow.queue.size() : 0);
        long occupied = flow == null ? 0 : flow.nativeBytes + (group ? flow.bytes : 0);
        if (!group) {
            return (!sendQueueFull() || !sending.isEmpty()) && (st == null || slots < cfg.datagramStreamQueue())
                    && (limit < 0 || bytes <= limit - occupied);
        }
        return count <= cfg.datagramSendQueue() - sendOccupancy() && count <= cfg.datagramStreamQueue() - slots
                && (limit < 0 || bytes <= limit - occupied);
    }

    CoreException reject(CoreException.DatagramDrop reason) {
        switch (reason) {
            case DEADLINE -> { stats.datagramDeadline(); stats.datagramDroppedSend(); }
            case LIMITER -> limitedSend();
            case NO_NATIVE_PATH -> stats.datagramNativeUnavailable();
            case QUEUE_OVERFLOW, SEQUENCE_EXHAUSTED -> stats.datagramDroppedSend();
        }
        return CoreException.datagramDropped(reason);
    }

    private void enqueue(SendFlow flow, Out next) {
        if (next.charge != null) next.charge.enqueued(next.length);
        next.retain();
        next.queuedAt = clock.getAsLong();
        flow.queue.add(next);
        flow.bytes += next.length;
        sendCount++;
        sendDeadline = Math.min(sendDeadline, next.deadline);
    }

    /** A send limiter refused a datagram before queueing it. */
    private void limitedSend() {
        stats.datagramDroppedLimit();
        stats.datagramDroppedSend();
    }

    private int sendOccupancy() { return sendCount + copyCount + nativeInFlight; }

    public boolean sendQueueFull() { return sendOccupancy() >= cfg.datagramSendQueue(); }

    /** The send cursor and primary queue occupancy, read on the session owner. */
    public record SendSnapshot(int queued, long nextSequence) {}

    public SendSnapshot sendSnapshot() { return new SendSnapshot(sendCount, nextSequence); }

    boolean queued() { return sendCount != 0 || copyCount != 0; }

    boolean waitsFor(Stream st) { return !st.tab.opened() && sending.containsKey(st); }

    private void dropHead(SendFlow flow) {
        flow.queue.getFirst().cancelReservation(RuntimeReason.CANCELLED);
        removeHead(flow);
        droppedSend();
    }

    private void removeHead(SendFlow flow) {
        if (flow.hold != null) flow.hold.close();
        Out out = flow.queue.removeFirst();
        out.unreserve();
        out.release();
        flow.bytes -= out.length;
        sendCount--;
        sending.remove(out.st);
        if (!flow.queue.isEmpty()) sending.put(out.st, flow);
        else flow.codel.idle();
    }

    long sendDeadline() {
        return sendDeadline;
    }

    void expireSend(long now) {
        if (sendDeadline > now) return;
        sendDeadline = Time.NEVER;
        for (var it = sending.values().iterator(); it.hasNext(); ) {
            SendFlow flow = it.next();
            // Attribute the blocked follower's interval before removing its queue identity.
            if (flow.hold != null) flow.hold.advance(now);
            for (var queued = flow.queue.iterator(); queued.hasNext(); ) {
                Out out = queued.next();
                if (out.deadline > now) {
                    sendDeadline = Math.min(sendDeadline, out.deadline);
                    continue;
                }
                if (flow.hold != null && out == flow.queue.peekFirst()) flow.hold.close();
                queued.remove();
                out.cancelReservation(RuntimeReason.APPLICATION_EXPIRED);
                out.release();
                flow.bytes -= out.length;
                sendCount--;
                stats.datagramDeadline();
                wakeSend();
            }
            if (flow.queue.isEmpty()) it.remove();
        }
        for (var paths = copies.entrySet().iterator(); paths.hasNext(); ) {
            var path = paths.next();
            Pipe copyPipe = path.getKey();
            var queue = path.getValue();
            for (var pending = queue.iterator(); pending.hasNext(); ) {
                Copy copy = pending.next();
                long deadline = copy.original.deadline;
                if (deadline <= now) {
                    pending.remove();
                    var shape = copyPipe.shape();
                    if (shape != null && shape.nativeItem() == copy) shape.cancelNative(RuntimeReason.APPLICATION_EXPIRED);
                    copy.original.release();
                    copyCount--;
                } else sendDeadline = Math.min(sendDeadline, deadline);
            }
            if (queue.isEmpty()) paths.remove();
        }
    }

    void pathRetired(Pipe pipe) {
        feedback.retire(pipe.id);
        cancelPathCopies(pipe);
    }

    void cancelPathCopies(Pipe pipe) {
        if (pipe.shape() != null) pipe.shape().clearNative();
        var queue = copies.remove(pipe);
        if (queue != null) {
            for (Copy copy : queue) copy.original.release();
            copyCount -= queue.size();
        }
    }

    /** Removes datagrams that have no protocol representation, independent of which pipe writes. */
    private void prune() {
        if (!queued()) return;
        expireSend(clock.getAsLong());
        for (var it = sending.values().iterator(); it.hasNext(); ) {
            SendFlow flow = it.next();
            while (!flow.queue.isEmpty()) {
                Out out = flow.queue.getFirst();
                int length = dataLength(out);
                boolean nativeFrame = channels.nativeNegotiated() && fitsNativeFrame(length);
                boolean recordFrame = fitsRecord(channels.fixedRecordPath() ? largestRecordCap.getAsInt() : Integer.MAX_VALUE, length);
                boolean unavailable = peerKnown && out.options.carriage() == Carriage.REQUIRE_NATIVE && !channels.nativeNegotiated();
                if (!unavailable && !out.dead() && !exhausted() && (out.st == null || sequenced) && (recordFrame || nativeFrame)) break;
                if (flow.hold != null) flow.hold.close();
                flow.queue.removeFirst();
                out.cancelReservation(RuntimeReason.CANCELLED);
                out.release();
                flow.bytes -= out.length;
                sendCount--;
                if (unavailable) stats.datagramNativeUnavailable();
                else droppedSend();
            }
            if (flow.queue.isEmpty()) it.remove();
        }
    }

    private SendFlow select(Pipe pipe, int recordLimit, ChannelUse nativeUse, boolean nativeChannel, int maxUrgency) {
        SendFlow selected = null;
        int best = maxUrgency + 1;
        for (var flow : sending.values()) {
            Out out = flow.queue.getFirst();
            int urgency = out.st == null ? 3 : out.st.tab.urgency();
            if (urgency >= best || out.reserved != null || !out.ready() || !channels.carries(out.st, pipe)) continue;
            int n = dataLength(out);
            if (nativeChannel ? !fitsNativeFrame(n) || !nativeUse.takes(n)
                    : (out.options.carriage() == Carriage.REQUIRE_NATIVE || nativeUse.supports(n) || n > maxData(Math.min(peerMax, recordLimit)))) continue;
            if (channels.pick(out.st, pipe, n, out.deadline, out.options.carriage()) != pipe) continue;
            selected = flow;
            best = urgency;
        }
        return selected;
    }

    private static long copyCharge(Out original) { return Math.max(1, original.length); }

    private void planCopies(Out out, Pipe original, boolean nativePath, long sequence, byte[] encoded, int from) {
        long now = clock.getAsLong();
        budget.original(now, out.length);
        if (sequence < 0) return;
        boolean adaptive = out.options.copies() == DatagramSendOptions.Copies.Adaptive.INSTANCE;
        int requested = switch (out.options.copies()) {
            case null -> 1;
            case DatagramSendOptions.Copies.Single ignored -> 1;
            case DatagramSendOptions.Copies.Duplicate duplicate -> duplicate.count();
            case DatagramSendOptions.Copies.Adaptive ignored -> 2;
        };
        int count = Math.min(requested, cfg.copyPolicy().maxCopies());
        if (count <= 1 || !channels.redundant(out.st, original)) return;
        boolean distinct = adaptive || Boolean.TRUE.equals(out.options.distinct());
        boolean urgent = out.options.urgent() != null ? out.options.urgent()
                : out.st != null && out.st.tab.urgency() <= 2;
        var originalPath = adaptive ? new DatagramFeedback.Path(original.id, nativePath) : null;
        var impaired = urgent ? null : originalPath;
        var selected = new HashSet<Pipe>();
        selected.add(original);
        byte[] data = encoded;
        while (selected.size() < count) {
            if (sendOccupancy() - 1 >= cfg.datagramSendQueue() || budget.remaining(now) < copyCharge(out)) {
                stats.datagramCopyBudgetDrop();
                return;
            }
            int length = encoded.length - from;
            var target = channels.copyPath(out.st, original, selected, length, out.deadline,
                    out.options.carriage(), distinct, adaptive, impaired);
            if (target == null) {
                if (impaired != null) return;
                stats.datagramCopyPathDrop();
                if (distinct && channels.copyPath(out.st, original, selected, length, out.deadline,
                        out.options.carriage(), false, adaptive, null) != null) stats.datagramCopyIndependenceDrop();
                return;
            }
            Pipe pipe = target.pipe();
            var pair = adaptive ? feedback.bindPair(originalPath,
                    new DatagramFeedback.Path(pipe.id, target.nativePath()), urgent) : null;
            if (adaptive && pair == null) {
                stats.datagramCopyPathDrop();
                return;
            }
            if (from != 0 && data == encoded) data = java.util.Arrays.copyOfRange(encoded, from, encoded.length);
            out.retain();
            copies.computeIfAbsent(pipe, ignored -> new ArrayDeque<>()).addLast(
                    new Copy(out, original, Set.copyOf(selected), sequence, data, pair));
            selected.add(pipe);
            copyCount++;
            channels.wake(pipe);
        }
    }

    private Copy copyFor(Pipe pipe, int recordLimit, ChannelUse channel, boolean nativeChannel) {
        var queue = copies.get(pipe);
        if (queue == null) return null;
        while (!queue.isEmpty()) {
            Copy copy = queue.getFirst();
            Out original = copy.original;
            boolean nativeFit = channel.supports(copy.data.length);
            long now = clock.getAsLong();
            boolean samePair = copy.adaptive == null
                    || (copy.adaptive.nativePath() == nativeFit && feedback.current(copy.adaptive, now));
            boolean distinct = copy.adaptive != null || Boolean.TRUE.equals(original.options.distinct());
            if (!samePair || !channels.copyEligible(original.st, copy.first, pipe, copy.prior,
                    copy.data.length, original.deadline, original.options.carriage(), distinct)) {
                stats.datagramCopyPathDrop();
            } else {
                if (nativeChannel != nativeFit || (nativeFit && !channel.takes(copy.data.length))) return null;
                if (!nativeFit && (original.options.carriage() == Carriage.REQUIRE_NATIVE
                        || !fitsRecord(recordLimit, copy.data.length))) {
                    stats.datagramCopyPathDrop();
                } else if (budget.remaining(now) < copyCharge(original)) {
                    stats.datagramCopyBudgetDrop();
                } else {
                    return copy;
                }
            }
            removeCopy(pipe, queue);
        }
        return null;
    }

    private void removeCopy(Pipe pipe, ArrayDeque<Copy> queue) {
        Copy copy = queue.removeFirst();
        clearCopyReservation(pipe, copy);
        copy.original.release();
        copyCount--;
        if (queue.isEmpty()) copies.remove(pipe);
    }

    private static void clearCopyReservation(Pipe pipe, Copy copy) {
        var shape = pipe.shape();
        if (shape != null && shape.nativeItem() == copy) shape.clearNative();
    }

    private void copied(Pipe pipe, Copy copy, boolean nativeChannel) {
        removeCopy(pipe, copies.get(pipe));
        long now = clock.getAsLong();
        budget.copy(now, copyCharge(copy.original));
        feedback.dispatched(pipe.id, nativeChannel, copy.sequence, now);
        if (!nativeChannel) {
            stats.datagramSent();
            stats.datagramCopy(copy.original.length);
            sentBytes(copy.original.st, copy.original.length);
        }
    }

    private boolean fitsNativeFrame(int data) { return 1L + data <= peerMax; }

    boolean fitsRecord(int recordLimit, int bytes) {
        return bytes <= maxData(Math.min(peerMax, recordLimit));
    }

    boolean sendable(Pipe pipe, int recordLimit) {
        if (!peerKnown || !queued()) return false;
        prune();
        var channel = channels.of(pipe);
        return copyFor(pipe, recordLimit, channel, false) != null || select(pipe, recordLimit, channel, false, 7) != null;
    }

    /** Writes at most one logical datagram, with priority and rotation shared with the native path. */
    boolean write(RecordWriter writer, Pipe pipe, int urgency) {
        if (!peerKnown || !queued()) return false;
        prune();
        var channel = channels.of(pipe);
        Copy copy = copyFor(pipe, writer.recordLimit(), channel, false);
        if (copy != null && writer.datagram(copy.data, -1, copy, null, null)) {
            copy.original.attachCharge(writer);
            copied(pipe, copy, false);
            return true;
        }
        for (;;) {
            var flow = select(pipe, writer.recordLimit(), channel, false, urgency);
            if (flow == null) return false;
            Out out = flow.queue.getFirst();
            if (sojournDrop(out, flow)) { dropHead(flow); continue; }
            byte[] data = encode(out);
            int recordLimit = writer.recordLimit();
            Pipe.OutLane lane = pipe.out().lane(writer.b().lane());
            java.util.function.Predicate<NativeWork> eligible = writer.shape() == null ? null : item -> {
                Out candidate = (Out) item;
                int bytes = dataLength(candidate);
                assert lane != null : "the writer has an outbound lane";
                return !lane.tx.notWritable() && !lane.tx.writing() && candidate.ready()
                        && candidate.options.carriage() != Carriage.REQUIRE_NATIVE
                        && !channel.supports(bytes) && fitsRecord(recordLimit, bytes);
            };
            if (!writer.datagram(data, out.length, out, flow, eligible)) return false;
            out.attachCharge(writer);
            if (out.charge != null) out.charge.dispatched(false);
            long sequence = dispatched(pipe, false);
            planCopies(out, pipe, false, sequence, data, 0);
            removeHead(flow);
            stats.datagramSent();
            sentBytes(out.st, out.length);
            wakeSend();
            return true;
        }
    }

    void write(RecordWriter writer, Pipe pipe) {
        boolean wrote;
        do {
            wrote = write(writer, pipe, 7);
        } while (wrote);
    }

    private boolean sojournDrop(Out out, SendFlow flow) {
        return flow.codel.drop(out.at, clock.getAsLong(), cfg.datagramQueueDelay(), flow.queue.size() == 1);
    }

    private void droppedSend() {
        stats.datagramDroppedSend();
        wakeSend();
    }

    private void wakeSend() {
        Waiter sw = sendW.poll();
        if (sw != null) outbox.wake(sw);
    }

    int pollChannel(Pipe pipe, ChannelUse channel, java.util.List<byte[]> dst, java.util.List<ChannelSent> sent, Shaping shaping) {
        if (!peerKnown) return 0;
        prune();
        var shape = pipe.shape();
        var reserved = shape == null ? null : shape.nativeItem();
        if (reserved != null) shape.nativeRoom(channel);
        boolean releasing = reserved != null && clock.getAsLong() >= shape.nativeReady();
        if (reserved instanceof Out out && (!out.ready() || !channels.carries(out.st, pipe)
                || !channel.supports(dataLength(out)) || !fitsNativeFrame(dataLength(out))
                || (releasing && (!channel.takes(dataLength(out)) || channels.pick(out.st, pipe, dataLength(out),
                        out.deadline, out.options.carriage()) != pipe)))) {
            shape.cancelNative(RuntimeReason.CLIPPED_ROOM);
            reserved = null;
        }
        if (!queued()) {
            if (shape != null) shape.cancelNative(RuntimeReason.CANCELLED);
            return 0;
        }
        if (reserved instanceof Out out && !channel.takes(dataLength(out))) return 0;
        if (reserved instanceof Copy copy && !channel.takes(copy.data.length)) {
            if (!releasing && channel.supports(copy.data.length)) return 0;
            shape.cancelNative(RuntimeReason.CLIPPED_ROOM);
            reserved = null;
        }
        if (channel.room() == 0) return 0;
        int count = 0;
        for (;;) {
            if (exhausted()) {
                prune();
                if (reserved instanceof Out) reserved = null;
            }
            SendFlow flow = reserved instanceof Out out ? sending.get(out.st)
                    : reserved != null || exhausted() ? null : select(pipe, 0, channel, true, 7);
            Copy copy = flow != null ? null : copyFor(pipe, 0, channel, true);
            if (reserved instanceof Copy && copy != reserved) shape.clearNative();
            if (copy != null) {
                int pad = padding(shape, channel, copy, null, true, copy.original.deadline);
                if (pad < 0) return count;
                byte[] bytes = ChannelDatagram.encode(channel.id(), copy.data, pad);
                addChannel(pipe, copy.original, copy.sequence, null, bytes, pad, dst, sent, shaping);
                copied(pipe, copy, true);
                reserved = null;
                count++;
                if (shape != null && shape.serialNative()) return count;
                continue;
            }
            if (flow == null) return count;
            Out out = flow.queue.getFirst();
            if (sojournDrop(out, flow)) {
                dropHead(flow);
                reserved = null;
                continue;
            }
            int length = dataLength(out);
            int pad = padding(shape, channel, out, flow, false, out.deadline);
            if (pad < 0) {
                out.reserved = shape;
                return count;
            }
            byte[] bytes = new byte[ChannelDatagram.length(channel.id(), length) + pad];
            int from = ChannelDatagram.writeHeader(bytes, channel.id(), pad);
            encodeInto(out, bytes, from);
            if (out.charge != null) out.charge.dispatched(true);
            long sequence = dispatched(pipe, true);
            addChannel(pipe, out, sequence, flow, bytes, pad, dst, sent, shaping);
            planCopies(out, pipe, true, sequence, bytes, from);
            removeHead(flow);
            reserved = null;
            count++;
            wakeSend();
            if (shape != null && shape.serialNative()) return count;
        }
    }

    private int padding(Shaping.PipeShape shape, ChannelUse ch, NativeWork item, SendFlow flow, boolean copy, long expiry) {
        return shape == null ? 0 : shape.nativePadding(this, ch, item, flow, copy, expiry);
    }

    private void addChannel(Pipe pipe, Out out, long sequence, SendFlow flow, byte[] bytes, int padding,
                            java.util.List<byte[]> dst, java.util.List<ChannelSent> sent, Shaping shaping) {
        var shape = pipe.shape();
        if (shape != null && !shape.wantsOutbound()) shape = null;
        if (out.charge != null) out.charge.retain();
        OverheadLedger.Reservation receipt = null;
        boolean tracked = false;
        try {
            if (shaping != null) receipt = shaping.submit(padding);
            sent.add(new ChannelSent(out.st, out.length, pipe.id, sequence, flow, out.deadline, bytes.length,
                    shape, out.charge, receipt, padding));
            nativeInFlight++;
            if (flow != null) {
                flow.nativeCount++;
                flow.nativeBytes += out.length;
            }
            tracked = true;
            if (flow != null) nativeFlows.put(out.st, flow);
            dst.add(bytes);
        } finally {
            if (!tracked) {
                try {
                    if (receipt != null) receipt.settle(0, 0, false);
                } finally {
                    if (out.charge != null) out.charge.release();
                }
            }
        }
    }

    /** A dispatch ticket used exactly once by the carrier completion. */
    public static final class ChannelSent {
        final Stream stream;
        final int payload, length;
        final long pipe, sequence, expiry;
        final boolean copy;
        final Shaping.PipeShape shape;
        private final CarrierCharge charge;
        private final SendFlow flow;
        private final OverheadLedger.Reservation overhead;
        private final int padding;
        private boolean settled;

        ChannelSent(Stream stream, int payload, long pipe, long sequence, SendFlow flow, long expiry,
                    int length, Shaping.PipeShape shape, CarrierCharge charge, OverheadLedger.Reservation overhead, int padding) {
            this.overhead = overhead;
            this.padding = padding;
            this.stream = stream;
            this.payload = payload;
            this.pipe = pipe;
            this.sequence = sequence;
            this.flow = flow;
            this.copy = flow == null;
            this.expiry = expiry;
            this.length = length;
            this.shape = shape;
            this.charge = charge;
        }

        public long expiry() { return expiry; }

        private void completeCharge(boolean accepted) {
            if (charge == null) return;
            try {
                if (accepted) charge.accepted(0, payload);
            } finally {
                charge.release();
            }
        }
    }

    private void sentBytes(Stream st, int payload) {
        stats.datagramBytesSent(payload);
        if (st != null) {
            st.counters.datagramSent(1, payload);
        }
    }

    boolean channelDone(ChannelSent ticket, io.zmux.carrier.CarrierChannel.Outcome outcome) {
        boolean accepted = outcome == io.zmux.carrier.CarrierChannel.Outcome.ACCEPTED;
        if (ticket.settled) return false;
        ticket.settled = true;
        try {
            if (ticket.overhead != null) ticket.overhead.settle(accepted ? ticket.length - ticket.padding : 0,
                    accepted ? ticket.padding : 0, outcome == io.zmux.carrier.CarrierChannel.Outcome.UNKNOWN);
        } finally {
            nativeInFlight--;
            var flow = ticket.flow;
            if (flow != null) {
                flow.nativeBytes -= ticket.payload;
                if (--flow.nativeCount == 0) nativeFlows.remove(ticket.stream);
            }
            try {
                ticket.completeCharge(accepted);
            } finally {
                wakeSend();
            }
        }
        if (ticket.sequence >= 0) feedback.completed(ticket.pipe, ticket.sequence, accepted);
        if (!accepted) {
            stats.datagramDroppedSend();
            return true;
        }
        stats.datagramSentChannel();
        if (ticket.copy) stats.datagramCopy(ticket.payload);
        sentBytes(ticket.stream, ticket.payload);
        return true;
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
    void onFrame(Frame f, FlowLookup lookup, long pipe, boolean channel) throws WireException {
        int pos = f.dataPos, end = f.dataPos + f.dataLen;
        if (!sequenced) {
            byte[] payload = new byte[end - pos];
            f.copy(pos, payload);
            admit(null, -1, payload, channel);
            return;
        }
        long sequence = f.varint(pos, end);
        if (sequence < 0) throw WireException.protocol("truncated datagram sequence");
        pos += VarInt.lengthOf(f.get(pos));
        long now = clock.getAsLong();
        expireHeld(now);
        boolean discard = receiveWindow.stale(sequence);
        if (discard) stats.datagramStale();
        else if (receiveWindow.occupied(sequence)) {
            stats.datagramDuplicate();
            discard = true;
        }
        try {
            sequencedFrame(f, pos, end, sequence, lookup, pipe, channel, now, discard);
        } catch (WireException bad) {
            // An occupied or stale Sequence discards the copy before its association affects the session.
            if (!discard) throw bad;
        }
    }

    private void sequencedFrame(Frame f, int pos, int end, long sequence, FlowLookup lookup,
                                long pipe, boolean channel, long now, boolean discard) throws WireException {
        long flow = f.varint(pos, end);
        if (flow < 0) throw WireException.protocol("truncated datagram flow");
        pos += VarInt.lengthOf(f.get(pos));
        Stream stream = null;
        long context = 0;
        if (flow != 0) {
            context = f.varint(pos, end);
            if (context < 0) throw WireException.protocol("truncated datagram context");
            pos += VarInt.lengthOf(f.get(pos));
            stream = lookup.flow(flow - 1);
        }
        feedback.arrived(pipe, channel, sequence, now);
        if (discard) return;
        receiveWindow.observe(sequence);
        if (flow != 0) {
            if (stream == null && lookup.unopened(flow - 1)) {
                hold(flow - 1, sequence, streamDatagram(context, f, pos, end), lookup.holdUntil(), channel);
            } else if (stream == null || stream.tab.discarded() || stream.rcv.stopped()) {
                dropReceived();
            } else {
                admit(stream, sequence, streamDatagram(context, f, pos, end), channel);
            }
            return;
        }
        byte[] payload = new byte[end - pos];
        f.copy(pos, payload);
        admit(null, sequence, payload, channel);
    }

    private void admit(Stream st, long sequence, byte[] payload, boolean channel) {
        if (received(st, sequence, payload)) {
            receiveWindow.occupy(sequence);
            countReceived(channel);
        } else {
            receiveWindow.release(sequence);
        }
    }

    /** A stream datagram as its queue holds it: the context ID, then the payload b[from, end). */
    private static byte[] streamDatagram(long ctx, Frame f, int from, int end) {
        byte[] e = new byte[VarInt.length(ctx) + end - from];
        int i = VarInt.put(e, 0, ctx);
        f.copy(from, e, i, end - from);
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
    private void hold(long id, long sequence, byte[] e, long until, boolean channel) {
        Held last = held.peekLast();
        if (last != null && until < last.until) until = last.until;
        Held h = new Held(id, sequence, e, until, channel);
        if (held.size() >= cfg.datagramHoldPackets() || h.cost() > cfg.datagramReceiveBytes() - receivedBytes - heldBytes) {
            dropReceived();
            return;
        }
        held.add(h);
        heldBytes += h.cost();
        receiveWindow.occupy(sequence);
    }

    private void dropHeldHead() {
        Held removed = held.remove();
        heldBytes -= removed.cost();
        receiveWindow.release(removed.sequence());
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
        if (mine != null) for (Held h : mine) admit(st, h.sequence, h.e, h.channel);
    }

    /** When the first held datagram expires (the oldest), {@link Time#NEVER} when none is held. */
    long heldDeadline() {
        Held h = held.peek();
        long next = h == null ? Time.NEVER : h.until;
        return expiring.isEmpty() ? next : Math.min(next, expiring.firstKey());
    }

    /** Drops the held datagrams whose stream did not open in time, oldest first. */
    void expireHeld(long now) {
        while (!held.isEmpty() && held.peek().until <= now) dropHeldHead();
        while (!expiring.isEmpty() && expiring.firstKey() <= now) {
            byte[] bytes = expiring.firstEntry().getValue().iterator().next().bytes();
            evict(bytes, received.get(bytes));
        }
    }

    /** A received datagram (stream datagrams carry their context prefix). */
    void received(Stream st, byte[] e) { received(st, -1, e); }

    private boolean received(Stream st, long sequence, byte[] e) {
        if (sequenced) {
            long max = st == null ? cfg.datagramReceiveBytes() : cfg.datagramStreamBytes();
            int bytes = st == null ? e.length : payloadBytes(e);
            if (max >= 0 && bytes > max) {
                dropReceived();
                return false;
            }
        }
        if (st != null && st.datagramReceiveLifetime() != Time.NEVER) {
            long lifetime = st.datagramReceiveLifetime();
            if (lifetime == 0) {
                dropReceived();
                st.counters.datagramDropped();
                return false;
            }
            expiresAt(e, Time.add(clock.getAsLong(), lifetime));
        }
        if (cfg.datagramReceiveLimited()) {
            Limited token = new Limited(st, e, sequence);
            limited.add(token);
            pending.put(e, token);
            track(st, e);
            if (st != null) trimStream(st);
            else {
                trimCount(null);
                trimBytes();
            }
            return received.containsKey(e);
        }
        queue(st, e);
        return received.containsKey(e);
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
        long expiry = expiryOf(bytes);
        untrack(bytes);
        if (drop || closing || expiry <= clock.getAsLong() || (stream != null && (stream.tab.discarded() || stream.rcv.stopped()))) {
            if (drop) stats.datagramDroppedLimit();
            dropReceived();
            return false;
        }
        expiresAt(bytes, expiry);
        queue(stream, bytes);
        if (!received.containsKey(bytes)) return false;
        receiveWindow.occupy(p.sequence);
        return true;
    }

    private void queue(Stream st, byte[] e) {
        if (st != null) {
            if (st.tab.discarded() || st.rcv.stopped()) {
                forgetExpiry(e);
                dropReceived();
                return;
            }
            PerStream ps = streams.computeIfAbsent(st, k -> new PerStream());
            if (ps.q.size() >= cfg.datagramStreamQueue()) dropStreamHead(st, ps);
            ps.q.add(new Queued(e, clock.getAsLong()));
            st.counters.datagramQueued();
            track(st, e);
            // The count first, then the bytes, oldest first; a single datagram above the bound stays alone.
            long max = cfg.datagramStreamBytes();
            while (max >= 0 && ps.bytes > max && ps.q.size() > 1) dropStreamHead(st, ps);
            ps.waiters.wakeFirst(outbox);
            outbox.wake(st.rcv.rsig());
            return;
        }
        if (recv.size() >= cfg.datagramReceiveQueue()) {
            untrack(recv.remove().e());
            dropReceived();
        }
        recv.add(new Queued(e, clock.getAsLong()));
        track(null, e);
        trimBytes();
        recvW.wakeFirst(outbox);
    }

    void dropReceived() {
        stats.datagramDroppedRecv();
    }

    /** Drops the oldest datagram of st's queue, counted for the session and the stream. */
    private void dropStreamHead(Stream st, PerStream ps) {
        untrack(ps.q.remove().e());
        dropReceived();
        st.counters.datagramDropped();
    }

    /** The payload bytes of a stream datagram as its queue holds it (after the context ID). */
    private static int payloadBytes(byte[] e) {
        return e.length - VarInt.lengthOf(e[0]);
    }

    private void track(Stream stream, byte[] bytes) {
        received.put(bytes, stream);
        if (stream == null) receivedBytes += bytes.length;
        else {
            streamBytes += bytes.length;
            PerStream ps = streams.computeIfAbsent(stream, k -> new PerStream());
            ps.bytes += payloadBytes(bytes);
            if (cfg.datagramReceiveLimited()) ps.order.add(bytes);
        }
    }

    private void untrack(byte[] bytes) {
        if (!received.containsKey(bytes)) return;
        forgetExpiry(bytes);
        Stream stream = received.remove(bytes);
        if (stream == null) receivedBytes -= bytes.length;
        else {
            streamBytes -= bytes.length;
            PerStream ps = streams.get(stream);
            if (ps != null) {
                ps.bytes -= payloadBytes(bytes);
                if (cfg.datagramReceiveLimited()) ps.order.remove(bytes);
            }
        }
        Limited token = pending.remove(bytes);
        if (token != null) {
            receiveWindow.release(token.sequence);
            token.revoke();
            limited.remove(token);
        }
    }

    private void evict(byte[] bytes, Stream stream) {
        if (!pending.containsKey(bytes)) {
            if (stream == null) {
                recv.removeIf(q -> q.e() == bytes);
            } else {
                PerStream ps = streams.get(stream);
                if (ps != null) ps.q.removeIf(q -> q.e() == bytes);
            }
        }
        untrack(bytes);
        dropReceived();
        if (stream != null) stream.counters.datagramDropped();
    }

    /**
     * Under a receive limiter: keeps st's datagrams awaiting the limiter and queued within its
     * count, then its byte bound, oldest first, leaving at least one.
     */
    private void trimStream(Stream stream) {
        PerStream ps = streams.get(stream);
        if (ps == null) return;
        while (ps.order.size() > cfg.datagramStreamQueue()) evict(ps.order.peek(), stream);
        long max = cfg.datagramStreamBytes();
        while (max >= 0 && ps.bytes > max && ps.order.size() > 1) evict(ps.order.peek(), stream);
    }

    /** Under a receive limiter: keeps the session datagrams within the session queue's count, oldest first. */
    private void trimCount(Stream stream) {
        int count = 0;
        for (Stream target : received.values()) if (target == stream) count++;
        int max = cfg.datagramReceiveQueue();
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

    /**
     * Keeps the session datagrams and those held for streams not opened yet within the receive
     * byte limit: the oldest held ones go first, then the oldest session datagrams. Stream queues
     * have their own bound.
     */
    private void trimBytes() {
        while (receivedBytes + heldBytes > cfg.datagramReceiveBytes() && !held.isEmpty()) dropHeldHead();
        while (receivedBytes > cfg.datagramReceiveBytes()) {
            byte[] oldest = null;
            // Without the limiter every session datagram is in recv, oldest first.
            if (!cfg.datagramReceiveLimited()) oldest = recv.isEmpty() ? null : recv.peek().e();
            else for (var entry : received.entrySet()) if (entry.getValue() == null) { oldest = entry.getKey(); break; }
            if (oldest == null) return;
            evict(oldest, null);
        }
    }

    /** Pops a session datagram, or null; those queued longer than the queue delay allows are dropped on the way, counted. */
    public byte[] receive() {
        long now = clock.getAsLong(), target = cfg.datagramQueueDelay();
        while (!recv.isEmpty() && recvCodel.drop(recv.peek().at(), now, target, recv.size() == 1)) {
            untrack(recv.remove().e());
            dropReceived();
        }
        if (recv.isEmpty()) {
            recvCodel.idle();
            return null;
        }
        byte[] e = recv.remove().e();
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

    /** Pops a stream datagram, or null; those queued longer than the queue delay allows are dropped on the way, counted. */
    public StreamDatagram receive(Stream st) {
        Queued head = readableHead(st);
        if (head == null) return null;
        byte[] data = head.e();
        int prefix = VarInt.lengthOf(data[0]);
        byte[] payload = Arrays.copyOfRange(data, prefix, data.length);
        long expiry = expiryOf(data);
        commitReceive(st, data, prefix);
        return new StreamDatagram(VarInt.get(ByteBuffer.wrap(data), 0, prefix), payload, expiry);
    }

    /** The sole reference to a receive allocation moves with this view at dequeue. */
    public record OwnedDatagram(long context, StreamRecv.Chunk payload, long expiry) {}

    public OwnedDatagram transfer(Stream st, ReceiveLoans loans,
                                  Predicate<ReadBuf> fits) throws CoreException {
        Queued head = readableHead(st);
        if (head == null) return null;
        byte[] data = head.e();
        ByteBuffer view = ByteBuffer.wrap(data);
        int prefix = VarInt.lengthOf(data[0]);
        var root = new ReadBuf(view, null);
        var lease = loans.transfer(root, true, fits);
        if (lease == null) {
            root.unref();
            throw CoreException.of(CoreException.Reason.CHUNK_BUDGET);
        }
        var result = new OwnedDatagram(VarInt.get(view, 0, prefix),
                new StreamRecv.Chunk(view.slice(prefix, data.length - prefix), lease, false), expiryOf(data));
        commitReceive(st, data, prefix);
        return result;
    }

    private Queued readableHead(Stream st) {
        PerStream ps = streams.get(st);
        if (ps == null) return null;
        long now = clock.getAsLong(), target = cfg.datagramQueueDelay();
        expireHeld(now);
        while (!ps.q.isEmpty() && ps.codel.drop(ps.q.peek().at(), now, target, ps.q.size() == 1)) dropStreamHead(st, ps);
        if (ps.q.isEmpty()) ps.codel.idle();
        return ps.q.peekFirst();
    }

    private void commitReceive(Stream st, byte[] data, int prefix) {
        PerStream ps = streams.get(st);
        ps.q.removeFirst();
        untrack(data);
        if (!ps.q.isEmpty()) ps.waiters.wakeFirst(outbox);
        stats.datagramBytesReceived(data.length - prefix);
        st.counters.datagramReturned(data.length - prefix);
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

    /**
     * A stream discarded after the application finished with both its parts: the datagrams queued for it stay
     * readable until its handle is released ({@link #dropStream}); without any, it has nothing to keep.
     */
    void endStream(Stream st) {
        PerStream ps = streams.get(st);
        if (ps == null || ps.q.isEmpty()) dropStream(st);
    }

    /** A released stream: its queued and limiter-held datagrams go. */
    void dropStream(Stream st) {
        PerStream ps = streams.remove(st);
        if (ps != null) for (Queued q : ps.q) untrack(q.e());
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
        receiveExpiry.clear();
        expiring.clear();
        held.clear();
        feedback.clear();
        heldBytes = 0;
        recv.clear();
        for (SendFlow flow : sending.values()) for (Out out : flow.queue) out.release();
        for (var queue : copies.values()) for (Copy copy : queue) copy.original.release();
        sending.clear();
        copies.clear();
        sendCount = copyCount = 0;
        sendDeadline = Time.NEVER;
        streams.clear();
        receivedBytes = 0;
        streamBytes = 0;
    }
}
