package io.zmux.internal.core;

import io.zmux.internal.wire.VarInt;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * The listener's handling of new carriers:
 * classification of the first record, the per-source and listener-wide slots of carriers
 * that have not yet entered a session, and the pairing of split bootstraps. Not thread-safe: the owner serialises.
 */
public final class CarrierAdmission {
    /** The kind of a new carrier. */
    public enum Kind { MORE, SESSION, JOIN, LANE, REJECT }

    /** A classification: the kind and, once the first record is complete, its length (Size field included), else 0. */
    public record Classified(Kind kind, int firstRecordLength) {}

    private static final byte[] TP = io.zmux.internal.wire.Records.TRANSPORT_PARAMETERS_PREFIX;
    private static final byte[] LANE = {(byte) 0xba, 0x05, (byte) 0xa1, (byte) 0xc9};

    /**
     * Classifies the first bytes [pos, limit) of a carrier incrementally. First byte 02 is a Hello, whose
     * parser takes over, and its first record length stays 0.
     */
    public static Classified classify(ByteBuffer b, int pos, int limit, long maxRecord) {
        if (pos >= limit) return new Classified(Kind.MORE, 0);
        if ((b.get(pos) & 0xff) == io.zmux.internal.join.Hello.PROFILE_JOIN) return new Classified(Kind.JOIN, 0);
        long size = VarInt.get(b, pos, limit);
        if (size < 0) return new Classified(Kind.MORE, 0);
        if (size > maxRecord) return new Classified(Kind.REJECT, 0);
        int n = VarInt.lengthOf(b.get(pos));
        int body = pos + n;
        Kind k;
        int need;
        if (size >= 9 && matches(b, body, limit, TP)) {
            k = Kind.SESSION;
            need = 8;
        } else if (size >= 22 && matches(b, body, limit, LANE)) {
            k = Kind.LANE;
            need = 4;
        } else {
            return new Classified(Kind.REJECT, 0);
        }
        if (limit - body < need) return new Classified(Kind.MORE, 0);
        if (limit - body < size) return new Classified(k, 0);
        return new Classified(k, n + (int) size);
    }

    /** The PIPE_LANE that a complete first record contains, or null. */
    public static io.zmux.internal.wire.Frame parseLane(byte[] body) {
        io.zmux.internal.wire.Frame frame = new io.zmux.internal.wire.Frame();
        try { new io.zmux.internal.wire.FrameDecoder().decode(ByteBuffer.wrap(body), 0, body.length, frame); }
        catch (io.zmux.internal.wire.WireException error) { return null; }
        return frame.kind == io.zmux.internal.wire.FrameKind.PIPE_LANE ? frame : null;
    }

    public static byte[] laneKey(io.zmux.internal.wire.Frame frame) {
        byte[] key = new byte[16]; frame.src.get(frame.laneKeyPos, key); return key;
    }

    private static boolean matches(ByteBuffer b, int at, int limit, byte[] prefix) {
        for (int i = 0; i < prefix.length && at + i < limit; i++) if (b.get(at + i) != prefix[i]) return false;
        return true;
    }

    // ---- the bounds of carriers before a session ----

    /** {@link #take}: a slot of the source and of the listener were taken. */
    public static final int TAKEN = 0;
    /** {@link #take}: the source holds its share; nothing was taken. */
    public static final int SOURCE_FULL = 1;
    /** {@link #take}: every slot of the listener is taken; nothing was taken. */
    public static final int FULL = 2;

    /** The default of {@link #CarrierAdmission}'s maxTotal. */
    public static final int DEFAULT_MAX_TOTAL = 1024;

    private int maxPerSource;
    /** The source prefixes: the leading bits of an IPv4 and of an IPv6 address that name its client. */
    private final int prefixV4, prefixV6;
    /** Negative: unlimited. */
    private int maxTotal;
    private final Map<String, Integer> bySource = new HashMap<>();
    private final Map<String, Integer> stalledBySource = new HashMap<>();
    /** The slots taken in all, and those of stalled carriers. */
    private int total, stalled;

    /** The default IPv4 source prefix: the address on its own. */
    public static final int DEFAULT_PREFIX_V4 = 32;
    /** The default IPv6 source prefix. */
    public static final int DEFAULT_PREFIX_V6 = 64;

    /**
     * maxPerSource 0: 8; maxTotal 0: {@link #DEFAULT_MAX_TOTAL}, negative: unlimited;
     * prefixV4 (1 to 32) and prefixV6 (1 to 128) 0: their defaults,
     * validated by the caller.
     */
    public CarrierAdmission(int maxPerSource, int maxTotal, int prefixV4, int prefixV6) {
        if (prefixV4 < 0 || prefixV4 > 32 || prefixV6 < 0 || prefixV6 > 128) throw new IllegalArgumentException("source prefix out of range");
        this.prefixV4 = prefixV4 == 0 ? DEFAULT_PREFIX_V4 : prefixV4;
        this.prefixV6 = prefixV6 == 0 ? DEFAULT_PREFIX_V6 : prefixV6;
        bounds(maxPerSource, maxTotal);
    }

    /**
     * Replaces the per-source and listener-wide bounds, as the constructor reads them; slots
     * already taken stay: a lowered bound refuses new carriers until the count is below it.
     */
    public void bounds(int maxPerSource, int maxTotal) {
        this.maxPerSource = maxPerSource > 0 ? maxPerSource : 8;
        this.maxTotal = maxTotal == 0 ? DEFAULT_MAX_TOTAL : maxTotal;
    }

    /**
     * Takes a slot of source and one of the listener for a new carrier, together: {@link #TAKEN},
     * or {@link #SOURCE_FULL} or {@link #FULL} with nothing taken. {@link #release} gives both back.
     */
    public int take(java.net.InetSocketAddress source) {
        String k = sourceKey(source);
        int n = bySource.getOrDefault(k, 0);
        if (n >= maxPerSource) return SOURCE_FULL;
        if (maxTotal > 0 && total >= maxTotal) return FULL;
        bySource.put(k, n + 1);
        total++;
        return TAKEN;
    }

    /** Returns the slots of a carrier that entered a session, was closed or went to a fallback. */
    public void release(java.net.InetSocketAddress source) {
        String k = sourceKey(source);
        Integer n = bySource.get(k);
        if (n == null) return;
        if (n > 1) bySource.put(k, n - 1); else bySource.remove(k);
        total--;
    }

    /**
     * Marks a carrier that holds its slots as stalled; false when its source already has
     * maxPerSource - 1 stalled carriers (one slot always stays for a new attempt), or the
     * stalled carriers hold half of the listener's slots (rounded down).
     */
    public boolean stall(java.net.InetSocketAddress source) {
        String k = sourceKey(source);
        int n = stalledBySource.getOrDefault(k, 0);
        if (n >= maxPerSource - 1 || (maxTotal > 0 && stalled >= maxTotal / 2)) return false;
        stalledBySource.put(k, n + 1);
        stalled++;
        return true;
    }

    /** Ends a stall: its slots go too. */
    public void unstall(java.net.InetSocketAddress source) {
        String k = sourceKey(source);
        Integer n = stalledBySource.get(k);
        if (n != null) {
            if (n > 1) stalledBySource.put(k, n - 1); else stalledBySource.remove(k);
            stalled--;
        }
        release(source);
    }

    /** The slots taken in all. */
    public int taken() { return total; }

    /**
     * The client a source stands for in everything the listener keeps per source (the per-source
     * slots, the count of stalled carriers and the stall duration): an IPv4 address aggregated to
     * its first prefixV4 bits, an IPv6 address to its first prefixV6 bits, an IPv4-mapped IPv6
     * address as IPv4, the port ignored. The only such function; an unknown source ("") is one
     * client.
     */
    public String sourceKey(java.net.InetSocketAddress source) {
        if (source == null) return "";
        java.net.InetAddress a = source.getAddress();
        if (a == null) return source.getHostString();
        byte[] b = a.getAddress();
        int from = 0, bits = prefixV6;
        if (b.length == 4 || mapped(b)) { from = b.length - 4; bits = prefixV4; }
        byte[] key = java.util.Arrays.copyOfRange(b, from, from + Math.ceilDiv(bits, 8));
        if (bits % 8 != 0) key[key.length - 1] &= (byte) (0xff << (8 - bits % 8));
        return (b.length - from == 4 ? "4/" : "6/") + bits + "/" + java.util.HexFormat.of().formatHex(key);
    }

    /** Whether b is an IPv4-mapped IPv6 address (::ffff:a.b.c.d). */
    private static boolean mapped(byte[] b) {
        if (b.length != 16 || b[10] != (byte) 0xff || b[11] != (byte) 0xff) return false;
        for (int i = 0; i < 10; i++) if (b[i] != 0) return false;
        return true;
    }

    // ---- split bootstrap pairing ----

    /**
     * Pairs the two unidirectional carriers of a split bootstrap by their token. A waiting half
     * holds its slots of the listener ({@link #take}); the pairing adds no bound of its own.
     */
    public static final class SplitPairing<T> {
        private final Map<String, Token<T>> active = new HashMap<>();

        /** The sole reservation for a generation of this token. Its holder retires it after cleanup. */
        public static final class Token<T> implements AutoCloseable {
            private final SplitPairing<T> table;
            private final String key;
            private final boolean up;
            private T waiting;

            private Token(SplitPairing<T> table, String key, boolean up, T first) {
                this.table = table;
                this.key = key;
                this.up = up;
                waiting = first;
            }

            /** Stop matching. The reservation remains live through physical cleanup. */
            public void seal() { waiting = null; }

            @Override public void close() {
                waiting = null;
                table.active.remove(key, this);
            }
        }

        /** A live token and, on a match, the other arrival. */
        public record Offer<T>(Token<T> reservation, T partner) {}

        /** Null refuses the arrival. A fresh reservation waits, a matched one carries its partner. */
        public Offer<T> offer(byte[] token, boolean up, T carrier) {
            return offer(token, up, carrier, waiting -> true);
        }

        /** Consume the waiting generation only after both owners have admitted the match. */
        public Offer<T> offer(byte[] token, boolean up, T carrier, java.util.function.Predicate<T> match) {
            String key = java.util.HexFormat.of().formatHex(token);
            Token<T> slot = active.get(key);
            if (slot == null) {
                slot = new Token<>(this, key, up, carrier);
                active.put(key, slot);
                return new Offer<>(slot, null);
            }
            if (slot.waiting == null || slot.up == up || !match.test(slot.waiting)) return null;
            T partner = slot.waiting;
            slot.waiting = null;
            return new Offer<>(slot, partner);
        }
    }
}
