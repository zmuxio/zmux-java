package io.zmux.internal.core;

import io.zmux.carrier.VarInt;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The listener's handling of new carriers (MULTIPATH.md 5.5 and 5.6, SPEC.md 6.1.1 and 6.2):
 * classification of the first record, per-source admission of carriers that have not yet
 * delivered it, and the pairing of split bootstraps. Not thread-safe: the owner serialises.
 */
public final class CarrierAdmission {
    /** The kind of a new carrier. */
    public enum Kind { MORE, SESSION, JOIN, LANE, REJECT }

    /** A classification: the kind and, once the first record is complete, its length (Size field included), else 0. */
    public record Classified(Kind kind, int firstRecordLength) {}

    private static final byte[] TP = io.zmux.internal.wire.Records.TRANSPORT_PARAMETERS_PREFIX;
    private static final byte[] JOIN = {(byte) 0x9a, 0x24, (byte) 0xde, 0x36};
    private static final byte[] LANE = {(byte) 0xba, 0x05, (byte) 0xa1, (byte) 0xc9};

    /** Classifies the first bytes [pos, limit) of a carrier incrementally. */
    public static Classified classify(ByteBuffer b, int pos, int limit, long maxRecord) {
        if (pos >= limit) return new Classified(Kind.MORE, 0);
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
        } else if (size >= 26 && matches(b, body, limit, JOIN)) {
            k = Kind.JOIN;
            need = 4;
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

    /** The PIPE_JOIN that body (a record body without its Size field, or an out-of-band join) starts with, or null. */
    public static io.zmux.internal.wire.Frame parseJoin(byte[] body) {
        io.zmux.internal.wire.Frame f = new io.zmux.internal.wire.Frame();
        try {
            new io.zmux.internal.wire.FrameDecoder().decode(ByteBuffer.wrap(body), 0, body.length, f);
        } catch (io.zmux.internal.wire.WireException e) {
            return null;
        }
        return f.kind == io.zmux.internal.wire.FrameKind.PIPE_JOIN ? f : null;
    }

    /** Strict unpadded base64url of exactly one PIPE_JOIN, bounded by its wire maximum. */
    public static byte[] decodeJoinHeader(String value) {
        if (value == null || value.length() > (64 * 8 + 5) / 6 || value.indexOf('=') >= 0) return null;
        try {
            byte[] bytes = java.util.Base64.getUrlDecoder().decode(value);
            if (bytes.length > 64 || !java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes).equals(value)) return null;
            var frame = new io.zmux.internal.wire.Frame();
            int consumed = new io.zmux.internal.wire.FrameDecoder().decode(ByteBuffer.wrap(bytes), 0, bytes.length, frame);
            return consumed == bytes.length && frame.kind == io.zmux.internal.wire.FrameKind.PIPE_JOIN ? bytes : null;
        } catch (IllegalArgumentException | io.zmux.internal.wire.WireException invalid) { return null; }
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

    /** The MAC input of a decoded PIPE_JOIN: its bytes before the token (MULTIPATH.md 10.3). */
    public static byte[] joinMac(io.zmux.internal.wire.Frame f) {
        int start = f.join.tokenPos - f.join.macLength;
        byte[] m = new byte[f.join.macLength];
        f.src.get(start, m);
        return m;
    }

    /** The locator a decoded PIPE_JOIN names. */
    public static byte[] joinLocator(io.zmux.internal.wire.Frame f) {
        byte[] loc = new byte[f.join.locatorLen];
        f.src.get(f.join.locatorPos, loc);
        return loc;
    }

    private static boolean matches(ByteBuffer b, int at, int limit, byte[] prefix) {
        for (int i = 0; i < prefix.length && at + i < limit; i++) if (b.get(at + i) != prefix[i]) return false;
        return true;
    }

    // ---- per-source bounds of pending carriers (MULTIPATH.md 5.6) ----

    private final int maxPerSource;
    private final long firstRecordTimeout;
    private final Map<String, Integer> bySource = new HashMap<>();
    private final Map<String, Integer> stalledBySource = new HashMap<>();

    /** maxPerSource 0: 8; firstRecordTimeout 0: 2 s. */
    public CarrierAdmission(int maxPerSource, long firstRecordTimeout) {
        this.maxPerSource = maxPerSource > 0 ? maxPerSource : 8;
        this.firstRecordTimeout = firstRecordTimeout > 0 ? firstRecordTimeout : 2 * Time.SECOND;
    }

    /** Takes a slot for a new carrier from source; false: close it. */
    public boolean admit(java.net.InetSocketAddress source) {
        String k = sourceKey(source);
        int n = bySource.getOrDefault(k, 0);
        if (n >= maxPerSource) return false;
        bySource.put(k, n + 1);
        return true;
    }

    /** Returns the slot of a carrier whose first record arrived, that was closed, or that a fallback took. */
    public void release(java.net.InetSocketAddress source) {
        bySource.computeIfPresent(sourceKey(source), (k, v) -> v > 1 ? v - 1 : null);
    }

    /**
     * Marks a carrier that holds its slot as stalled; false when its source already has
     * maxPerSource - 1 stalled carriers (one slot always stays for a new attempt).
     */
    public boolean stall(java.net.InetSocketAddress source) {
        String k = sourceKey(source);
        int n = stalledBySource.getOrDefault(k, 0);
        if (n >= maxPerSource - 1) return false;
        stalledBySource.put(k, n + 1);
        return true;
    }

    /** Ends a stall: its slot goes too. */
    public void unstall(java.net.InetSocketAddress source) {
        stalledBySource.computeIfPresent(sourceKey(source), (k, v) -> v > 1 ? v - 1 : null);
        release(source);
    }

    /**
     * The client a source stands for in every per-source limit and keyed duration: an IPv6
     * address aggregated to its /64, an IPv4 address as it is, the port ignored. The only such
     * function; an unknown source ("") is one client.
     */
    public static String sourceKey(java.net.InetSocketAddress source) {
        if (source == null) return "";
        java.net.InetAddress a = source.getAddress();
        if (a == null) return source.getHostString();
        byte[] b = a.getAddress();
        return b.length == 16 ? java.util.HexFormat.of().formatHex(b, 0, 8) + "::/64" : a.getHostAddress();
    }

    /** When a carrier accepted at t must have its first record. */
    public long deadline(long t) {
        return Time.add(t, firstRecordTimeout);
    }

    // ---- split bootstrap pairing (SPEC.md 6.1.1) ----

    /** Pairs the two unidirectional carriers of a split bootstrap by their token. */
    public static final class SplitPairing {
        /**
         * The outcome of offering a carrier: wait for the partner, paired (partner given), close (a reused token or
         * a second half of the same direction), or refuse at the pairing limits.
         */
        public enum Result { WAIT, DONE, REJECT, REFUSE }

        private record Waiting(boolean up, String source, long deadline, Object carrier) {}

        private final int maxTotal;
        private final int maxPerSource;
        private final Map<String, Waiting> wait = new HashMap<>();
        private final java.util.Set<String> used = new java.util.HashSet<>();
        private final Map<String, Integer> bySource = new HashMap<>();
        private Object partner;

        /** 0: the defaults 64 and 8. */
        public SplitPairing(int maxTotal, int maxPerSource) {
            this.maxTotal = maxTotal > 0 ? maxTotal : 64;
            this.maxPerSource = maxPerSource > 0 ? maxPerSource : 8;
        }

        private static String key(byte[] token) {
            return java.util.HexFormat.of().formatHex(token);
        }

        /** Offers a carrier (up: the initiator writes on it); on DONE, {@link #partner()} is the other carrier. */
        public Result offer(byte[] token, boolean up, java.net.InetSocketAddress client, Object carrier, long deadline) {
            String source = sourceKey(client);
            String k = key(token);
            partner = null;
            if (used.contains(k)) return Result.REJECT;
            Waiting w = wait.get(k);
            if (w != null) {
                if (w.up() == up) return Result.REJECT;
                wait.remove(k);
                release(w.source());
                used.add(k);
                partner = w.carrier();
                return Result.DONE;
            }
            if (wait.size() >= maxTotal || bySource.getOrDefault(source, 0) >= maxPerSource) return Result.REFUSE;
            wait.put(k, new Waiting(up, source, deadline, carrier));
            bySource.merge(source, 1, Integer::sum);
            return Result.WAIT;
        }

        public Object partner() {
            return partner;
        }

        private void release(String source) {
            bySource.computeIfPresent(source, (s, v) -> v > 1 ? v - 1 : null);
        }

        /** Drops held carriers whose deadline passed into dst. */
        public void expire(long now, List<Object> dst) {
            for (Iterator<Waiting> it = wait.values().iterator(); it.hasNext(); ) {
                Waiting w = it.next();
                if (now >= w.deadline()) {
                    it.remove();
                    release(w.source());
                    dst.add(w.carrier());
                }
            }
        }

        /** Removes a held carrier that gave up; false when a partner already took it. */
        public boolean withdraw(byte[] token, Object carrier) {
            String k = key(token);
            Waiting w = wait.get(k);
            if (w == null || w.carrier() != carrier) return false;
            wait.remove(k);
            release(w.source());
            return true;
        }

        /** Releases a used token when its session ends. */
        public void forget(byte[] token) {
            used.remove(key(token));
        }
    }
}
