package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A transport parameter set (SPEC.md Section 5, MULTIPATH.md Section 3, RESUME.md
 * Section 3, EXTENSIONS.md Sections 2 and 4.1). Presence is a bit per known
 * parameter in {@link #present}.
 */
public final class TransportParams {
    public static final long ID_MAX_IDLE_TIMEOUT = 0x01;
    public static final long ID_INITIAL_MAX_DATA = 0x04;
    public static final long ID_MAX_STREAM_DATA_BIDI_LOCAL = 0x05;
    public static final long ID_MAX_STREAM_DATA_BIDI_REMOTE = 0x06;
    public static final long ID_MAX_STREAM_DATA_UNI = 0x07;
    public static final long ID_MAX_STREAMS_BIDI = 0x08;
    public static final long ID_MAX_STREAMS_UNI = 0x09;
    public static final long ID_RESET_STREAM_AT = 0x1d;
    public static final long ID_MAX_DATAGRAM_FRAME_SIZE = 0x20;
    public static final long ID_MAX_RECORD_SIZE = 0x0571c59429cd0845L;
    public static final long ID_ROLE = 0x17240207L;
    public static final long ID_GOAWAY = 0x04528a5fL;
    public static final long ID_PREAMBLE = 0x0439e07aL;
    public static final long ID_PRIORITY = 0x31e7139dL;
    public static final long ID_DATAGRAM_FLOWS = 0x0af391edL;
    public static final long ID_MULTIPATH = 0x30dc2978L;
    public static final long ID_ACK_THRESHOLD = 0x2762c7e5L;
    public static final long ID_PIPE_ACK = 0x34b47621L;
    public static final long ID_MAX_ACK_DELAY = 0x17fa1ed6L;
    public static final long ID_MAX_LANES = 0x27856ef0L;
    public static final long ID_RESUME = 0x0d5778ceL;
    /** The qmux crate's application_protocols hook (SPEC.md Section 13.3). */
    public static final long ID_APP_PROTOCOLS = 0x3d4f9c2a8b1e6075L;
    /** zmux_carrier_datagrams (EXTENSIONS.md Section 6.8). */
    public static final long ID_CARRIER_DATAGRAMS = 0x3d274c5fL;

    /** The record size limit when max_record_size is absent. */
    public static final int DEFAULT_MAX_RECORD_SIZE = io.zmux.shaping.RecordPlan.MAX_RECORD_LENGTH;

    /** Secret flags of zmux_multipath (MULTIPATH.md Section 3.1). */
    public static final int SECRET_EXPORTER = 0x01;
    public static final int SECRET_PSK = 0x04;
    public static final int SECRET_INSECURE = 0x08;

    public static final int ROLE_INITIATOR = 0;
    public static final int ROLE_RESPONDER = 1;
    public static final int ROLE_AUTO = 2;

    /** Presence bits, in the order of {@link #IDS}. */
    public static final int IDLE = 1, MAX_DATA = 1 << 1, BIDI_LOCAL = 1 << 2, BIDI_REMOTE = 1 << 3, UNI = 1 << 4,
            STREAMS_BIDI = 1 << 5, STREAMS_UNI = 1 << 6, RESET_AT = 1 << 7, DATAGRAM = 1 << 8, RECORD_SIZE = 1 << 9,
            ROLE = 1 << 10, GOAWAY = 1 << 11, PREAMBLE = 1 << 12, PRIORITY = 1 << 13, FLOWS = 1 << 14,
            MULTIPATH = 1 << 15, ACK_THRESHOLD = 1 << 16, PIPE_ACK = 1 << 17, ACK_DELAY = 1 << 18, LANES = 1 << 19,
            RESUME = 1 << 20, APP_PROTOCOLS = 1 << 21, CARRIER_DATAGRAMS = 1 << 22;

    /** The zmux parameters whose presence identifies a zmux peer (SPEC.md Section 13.1). */
    public static final int ANY_ZMUX = ROLE | GOAWAY | PREAMBLE | PRIORITY | FLOWS | MULTIPATH | ACK_THRESHOLD
            | PIPE_ACK | ACK_DELAY | LANES | RESUME | CARRIER_DATAGRAMS;

    private static final long[] IDS = {ID_MAX_IDLE_TIMEOUT, ID_INITIAL_MAX_DATA, ID_MAX_STREAM_DATA_BIDI_LOCAL,
            ID_MAX_STREAM_DATA_BIDI_REMOTE, ID_MAX_STREAM_DATA_UNI, ID_MAX_STREAMS_BIDI, ID_MAX_STREAMS_UNI,
            ID_RESET_STREAM_AT, ID_MAX_DATAGRAM_FRAME_SIZE, ID_MAX_RECORD_SIZE, ID_ROLE, ID_GOAWAY, ID_PREAMBLE,
            ID_PRIORITY, ID_DATAGRAM_FLOWS, ID_MULTIPATH, ID_ACK_THRESHOLD, ID_PIPE_ACK, ID_MAX_ACK_DELAY,
            ID_MAX_LANES, ID_RESUME, ID_APP_PROTOCOLS, ID_CARRIER_DATAGRAMS};

    private static final long MAX_60 = 1L << 60;

    public int present;
    public int secretFlags;
    public final byte[] sessionNonce = new byte[16];
    public long maxIdleTimeoutMillis;
    public long initialMaxData;
    public long maxStreamDataBidiLocal;
    public long maxStreamDataBidiRemote;
    public long maxStreamDataUni;
    public long maxStreamsBidi;
    public long maxStreamsUni;
    public long maxDatagramFrameSize;
    public long maxRecordSize = DEFAULT_MAX_RECORD_SIZE;
    public int role;
    public long roleNonce;
    public long maxPreambleSize;
    public byte[] locator = new byte[0];
    public long ackThreshold;
    public long maxAckDelayMillis;
    public long maxLanes;
    public final byte[] laneKey = new byte[16];
    public long resumeGraceMillis;
    public List<byte[]> appProtocols = List.of();
    /** GREASE parameter to send (encoding only); 0 = none. */
    public long greaseId;
    public byte[] greaseValue = new byte[0];

    public boolean has(int bit) {
        return (present & bit) != 0;
    }

    /** Whether the parameters identify a zmux endpoint. */
    public boolean isZmux() {
        return (present & ANY_ZMUX) != 0;
    }

    private static int bitOf(long id) {
        for (int k = 0; k < IDS.length; k++) if (IDS[k] == id) return 1 << k;
        return 0;
    }

    private static boolean prohibited(long id) {
        return id == 0x00 || id == 0x02 || id == 0x03 || (id >= 0x0a && id <= 0x10);
    }

    /** Whether {@code id} is a GREASE parameter identifier (31 * N + 27). */
    public static boolean isGrease(long id) {
        return id >= 27 && Long.remainderUnsigned(id - 27, 31) == 0;
    }

    /** Decodes and validates a parameter block; every failure is TRANSPORT_PARAMETER_ERROR. */
    public static TransportParams decode(ByteBuffer b, int pos, int len) throws WireException {
        TransportParams p = new TransportParams();
        int end = pos + len;
        int i = pos;
        long[] unknown = null;
        int unknownCount = 0;
        while (i < end) {
            long id = VarInt.get(b, i, end);
            if (id < 0) throw WireException.param("truncated parameter block");
            i += VarInt.lengthOf(b.get(i));
            long vl = VarInt.get(b, i, end);
            if (vl < 0) throw WireException.param("truncated parameter block");
            i += VarInt.lengthOf(b.get(i));
            if (vl > end - i) throw WireException.param("truncated parameter block");
            int vp = i;
            i += (int) vl;
            if (prohibited(id)) throw WireException.param("prohibited QUIC parameter");
            int bit = bitOf(id);
            if (bit == 0) {
                if (unknown == null) unknown = new long[8];
                for (int k = 0; k < unknownCount; k++) {
                    if (unknown[k] == id) throw WireException.param("duplicate parameter");
                }
                if (unknownCount == unknown.length) unknown = Arrays.copyOf(unknown, unknownCount * 2);
                unknown[unknownCount++] = id;
                continue;
            }
            if ((p.present & bit) != 0) throw WireException.param("duplicate parameter");
            p.present |= bit;
            p.decodeOne(id, b, vp, (int) vl);
        }
        p.validate();
        return p;
    }

    private static long varintValue(ByteBuffer b, int pos, int len) throws WireException {
        long v = len == 0 ? -1 : VarInt.get(b, pos, pos + len);
        if (v < 0 || VarInt.lengthOf(b.get(pos)) != len) throw WireException.param("malformed varint parameter");
        return v;
    }

    private void decodeOne(long id, ByteBuffer b, int vp, int vl) throws WireException {
        if (id == ID_RESET_STREAM_AT || id == ID_GOAWAY || id == ID_PRIORITY || id == ID_DATAGRAM_FLOWS || id == ID_PIPE_ACK
                || id == ID_CARRIER_DATAGRAMS) {
            if (vl != 0) throw WireException.param("non-empty flag parameter");
        } else if (id == ID_MAX_IDLE_TIMEOUT) {
            maxIdleTimeoutMillis = varintValue(b, vp, vl);
        } else if (id == ID_INITIAL_MAX_DATA) {
            initialMaxData = varintValue(b, vp, vl);
        } else if (id == ID_MAX_STREAM_DATA_BIDI_LOCAL) {
            maxStreamDataBidiLocal = varintValue(b, vp, vl);
        } else if (id == ID_MAX_STREAM_DATA_BIDI_REMOTE) {
            maxStreamDataBidiRemote = varintValue(b, vp, vl);
        } else if (id == ID_MAX_STREAM_DATA_UNI) {
            maxStreamDataUni = varintValue(b, vp, vl);
        } else if (id == ID_MAX_STREAMS_BIDI) {
            maxStreamsBidi = varintValue(b, vp, vl);
            if (maxStreamsBidi > MAX_60) throw WireException.param("initial_max_streams_bidi above 2^60");
        } else if (id == ID_MAX_STREAMS_UNI) {
            maxStreamsUni = varintValue(b, vp, vl);
            if (maxStreamsUni > MAX_60) throw WireException.param("initial_max_streams_uni above 2^60");
        } else if (id == ID_MAX_DATAGRAM_FRAME_SIZE) {
            maxDatagramFrameSize = varintValue(b, vp, vl);
        } else if (id == ID_MAX_RECORD_SIZE) {
            maxRecordSize = varintValue(b, vp, vl);
            if (maxRecordSize < DEFAULT_MAX_RECORD_SIZE) throw WireException.param("max_record_size below 16382");
        } else if (id == ID_ROLE) {
            long x = vl == 0 ? -1 : VarInt.get(b, vp, vp + vl);
            if (x < 0 || x > 2) throw WireException.param("zmux_role");
            role = (int) x;
            int n = VarInt.lengthOf(b.get(vp));
            int rest = vl - n;
            if (x == ROLE_AUTO) {
                if (rest != 8) throw WireException.param("zmux_role nonce");
                roleNonce = b.getLong(vp + n);
            } else if (rest != 0) {
                throw WireException.param("zmux_role extra bytes");
            }
        } else if (id == ID_PREAMBLE) {
            maxPreambleSize = varintValue(b, vp, vl);
        } else if (id == ID_MULTIPATH) {
            if (vl < 18) throw WireException.param("zmux_multipath too short");
            secretFlags = b.get(vp) & 0xff;
            b.get(vp + 1, sessionNonce);
            int ll = b.get(vp + 17) & 0xff;
            if (ll > 16 || vl != 18 + ll) throw WireException.param("zmux_multipath locator");
            locator = new byte[ll];
            b.get(vp + 18, locator);
        } else if (id == ID_ACK_THRESHOLD) {
            ackThreshold = varintValue(b, vp, vl);
            if (ackThreshold < 1024) throw WireException.param("zmux_ack_threshold below 1024");
        } else if (id == ID_MAX_ACK_DELAY) {
            maxAckDelayMillis = varintValue(b, vp, vl);
            if (maxAckDelayMillis >= 1 << 14) throw WireException.param("zmux_max_ack_delay out of range");
        } else if (id == ID_MAX_LANES) {
            long x = vl == 0 ? -1 : VarInt.get(b, vp, vp + vl);
            if (x <= 0) throw WireException.param("zmux_max_lanes");
            int n = VarInt.lengthOf(b.get(vp));
            if (vl - n != 16) throw WireException.param("zmux_max_lanes");
            maxLanes = x;
            b.get(vp + n, laneKey);
        } else if (id == ID_RESUME) {
            resumeGraceMillis = varintValue(b, vp, vl);
            if (resumeGraceMillis == 0) throw WireException.param("zmux_resume grace 0");
        } else if (id == ID_APP_PROTOCOLS) {
            List<byte[]> list = new ArrayList<>();
            int i = vp, end = vp + vl;
            while (i < end) {
                int l = b.get(i) & 0xff;
                if (1 + l > end - i) throw WireException.param("application_protocols");
                byte[] a = new byte[l];
                b.get(i + 1, a);
                list.add(a);
                i += 1 + l;
            }
            appProtocols = List.copyOf(list);
        }
    }

    private void validate() throws WireException {
        boolean ackish = (present & (MULTIPATH | PIPE_ACK)) != 0;
        if ((present & (ACK_THRESHOLD | ACK_DELAY | LANES)) != 0 && !ackish) {
            throw WireException.param("companion parameter without zmux_multipath or zmux_pipe_ack");
        }
        if (has(RESUME) && !has(MULTIPATH)) throw WireException.param("zmux_resume without zmux_multipath");
        if (has(PREAMBLE)) {
            if (!has(RESET_AT)) throw WireException.param("zmux_preamble without reset_stream_at");
            if (maxPreambleSize < 1 || maxPreambleSize > maxRecordSize - 32
                    || maxPreambleSize > maxStreamDataBidiRemote || maxPreambleSize > maxStreamDataUni) {
                throw WireException.param("zmux_preamble out of range");
            }
        }
    }

    /** The encoded parameter block. */
    public byte[] toBytes() {
        int bound = 512 + greaseValue.length + locator.length;
        for (byte[] a : appProtocols) bound += 1 + a.length;
        ByteBuffer b = ByteBuffer.allocate(bound);
        encode(b);
        return Arrays.copyOf(b.array(), b.position());
    }

    private static void param(ByteBuffer b, long id, byte[] v) {
        VarInt.put(b, id);
        VarInt.put(b, v.length);
        b.put(v);
    }

    private static void varintParam(ByteBuffer b, long id, long v) {
        VarInt.put(b, id);
        VarInt.put(b, VarInt.length(v));
        VarInt.put(b, v);
    }

    private static final byte[] EMPTY = new byte[0];

    /** Encodes the present parameters, then the GREASE parameter, at the buffer's position. */
    public void encode(ByteBuffer b) {
        if (has(IDLE)) varintParam(b, ID_MAX_IDLE_TIMEOUT, maxIdleTimeoutMillis);
        if (has(MAX_DATA)) varintParam(b, ID_INITIAL_MAX_DATA, initialMaxData);
        if (has(BIDI_LOCAL)) varintParam(b, ID_MAX_STREAM_DATA_BIDI_LOCAL, maxStreamDataBidiLocal);
        if (has(BIDI_REMOTE)) varintParam(b, ID_MAX_STREAM_DATA_BIDI_REMOTE, maxStreamDataBidiRemote);
        if (has(UNI)) varintParam(b, ID_MAX_STREAM_DATA_UNI, maxStreamDataUni);
        if (has(STREAMS_BIDI)) varintParam(b, ID_MAX_STREAMS_BIDI, maxStreamsBidi);
        if (has(STREAMS_UNI)) varintParam(b, ID_MAX_STREAMS_UNI, maxStreamsUni);
        if (has(RESET_AT)) param(b, ID_RESET_STREAM_AT, EMPTY);
        if (has(DATAGRAM)) varintParam(b, ID_MAX_DATAGRAM_FRAME_SIZE, maxDatagramFrameSize);
        if (has(RECORD_SIZE)) varintParam(b, ID_MAX_RECORD_SIZE, maxRecordSize);
        if (has(ROLE)) {
            ByteBuffer v = ByteBuffer.allocate(9);
            VarInt.put(v, role);
            if (role == ROLE_AUTO) v.putLong(roleNonce);
            param(b, ID_ROLE, Arrays.copyOf(v.array(), v.position()));
        }
        if (has(GOAWAY)) param(b, ID_GOAWAY, EMPTY);
        if (has(PREAMBLE)) varintParam(b, ID_PREAMBLE, maxPreambleSize);
        if (has(PRIORITY)) param(b, ID_PRIORITY, EMPTY);
        if (has(FLOWS)) param(b, ID_DATAGRAM_FLOWS, EMPTY);
        if (has(CARRIER_DATAGRAMS)) param(b, ID_CARRIER_DATAGRAMS, EMPTY);
        if (has(MULTIPATH)) {
            byte[] v = new byte[18 + locator.length];
            v[0] = (byte) secretFlags;
            System.arraycopy(sessionNonce, 0, v, 1, 16);
            v[17] = (byte) locator.length;
            System.arraycopy(locator, 0, v, 18, locator.length);
            param(b, ID_MULTIPATH, v);
        }
        if (has(ACK_THRESHOLD)) varintParam(b, ID_ACK_THRESHOLD, ackThreshold);
        if (has(PIPE_ACK)) param(b, ID_PIPE_ACK, EMPTY);
        if (has(ACK_DELAY)) varintParam(b, ID_MAX_ACK_DELAY, maxAckDelayMillis);
        if (has(LANES)) {
            byte[] v = new byte[VarInt.length(maxLanes) + 16];
            int n = VarInt.put(v, 0, maxLanes);
            System.arraycopy(laneKey, 0, v, n, 16);
            param(b, ID_MAX_LANES, v);
        }
        if (has(RESUME)) varintParam(b, ID_RESUME, resumeGraceMillis);
        if (has(APP_PROTOCOLS)) {
            int n = 0;
            for (byte[] a : appProtocols) n += 1 + a.length;
            byte[] v = new byte[n];
            int i = 0;
            for (byte[] a : appProtocols) {
                v[i++] = (byte) a.length;
                System.arraycopy(a, 0, v, i, a.length);
                i += a.length;
            }
            param(b, ID_APP_PROTOCOLS, v);
        }
        if (greaseId != 0) param(b, greaseId, greaseValue);
    }
}
