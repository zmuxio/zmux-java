package io.zmux.internal.wire;

import io.zmux.carrier.VarInt;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.zmux.testutil.SpecFixtures;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The wire corpus of the dev spec (CONFORMANCE.md Section 4.1). */
class CorpusTest {

    static Gates gates(Map<String, Object> e) {
        @SuppressWarnings("unchecked")
        List<String> names = (List<String>) e.getOrDefault("gates", List.of());
        long dg = 0;
        boolean rsa = false, ga = false, pr = false, mp = false, pa = false, lanes = false;
        for (String n : names) {
            switch (n) {
                case "max_datagram_frame_size" -> dg = VarInt.MAX;
                case "reset_stream_at" -> rsa = true;
                case "zmux_goaway" -> ga = true;
                case "zmux_priority" -> pr = true;
                case "zmux_multipath" -> mp = true;
                case "zmux_pipe_ack" -> pa = true;
                case "zmux_max_lanes" -> lanes = true;
                default -> fail("unknown gate " + n);
            }
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> limits = (Map<String, Object>) e.get("limits");
        if (limits != null && limits.get("max_datagram_frame_size") instanceof Long v) dg = v;
        return new Gates(dg, rsa, ga, pr, mp, pa, lanes && (mp || pa));
    }

    /** Decodes the frames of a record that is not the first of a bootstrap pipe. */
    static void processFrames(ByteBuffer b, int pos, int end, Gates g, List<FrameKind> kinds) throws WireException {
        FrameDecoder d = new FrameDecoder();
        Frame f = new Frame();
        while (pos < end) {
            int n = d.decode(b, pos, end, f);
            if (f.kind == FrameKind.TRANSPORT_PARAMETERS) TransportParams.decode(b, f.dataPos, f.dataLen);
            g.check(f);
            if (kinds != null) kinds.add(f.kind);
            pos += n;
        }
    }

    static List<FrameKind> process(Map<String, Object> e) throws WireException {
        Gates g = gates(e);
        long limit = e.get("max_record_size") instanceof Long v ? v : 65535;
        List<FrameKind> kinds = new ArrayList<>();
        switch ((String) e.get("kind")) {
            case "frame" -> {
                ByteBuffer b = ByteBuffer.wrap(SpecFixtures.hex(e.get("hex")));
                Frame f = new Frame();
                int n = new FrameDecoder().decode(b, 0, b.limit(), f);
                if (n != b.limit()) throw WireException.frame("trailing bytes");
                if (f.kind == FrameKind.TRANSPORT_PARAMETERS) TransportParams.decode(b, f.dataPos, f.dataLen);
                g.check(f);
                kinds.add(f.kind);
            }
            case "record" -> {
                ByteBuffer b = ByteBuffer.wrap(SpecFixtures.hex(e.get("hex")));
                int pos = 0;
                while (pos < b.limit()) {
                    long size = Records.size(b, pos, b.limit(), limit);
                    if (size < 0) throw WireException.frame("truncated size");
                    int body = pos + VarInt.lengthOf(b.get(pos));
                    if (b.limit() - body < size) throw WireException.frame("truncated record");
                    processFrames(b, body, body + (int) size, g, kinds);
                    pos = body + (int) size;
                }
            }
            case "messages" -> {
                @SuppressWarnings("unchecked")
                List<String> msgs = (List<String>) e.get("messages");
                for (String m : msgs) {
                    ByteBuffer b = ByteBuffer.wrap(SpecFixtures.hex(m));
                    if (b.limit() > limit) throw WireException.frame("message above max_record_size");
                    processFrames(b, 0, b.limit(), g, kinds);
                }
            }
            default -> fail("unknown kind " + e.get("kind"));
        }
        return kinds;
    }

    @Test
    void valid() throws Exception {
        int n = 0;
        for (var e : SpecFixtures.ndjson("wire/valid.ndjson")) {
            List<FrameKind> kinds;
            try {
                kinds = process(e);
            } catch (WireException x) {
                fail(e.get("id") + ": unexpected " + x.getMessage());
                return;
            }
            if (e.get("frame") instanceof Map<?, ?> fr) {
                assertEquals(fr.get("type"), kinds.getFirst().wireName(), (String) e.get("id"));
            }
            if ("frame".equals(e.get("kind"))) {
                checkRoundTrip((String) e.get("id"), SpecFixtures.hex(e.get("hex")), !Boolean.TRUE.equals(e.get("non_minimal")));
            }
            n++;
        }
        assertTrue(n > 50);
    }

    @Test
    void invalid() throws Exception {
        int n = 0;
        for (var e : SpecFixtures.ndjson("wire/invalid.ndjson")) {
            try {
                process(e);
                fail(e.get("id") + ": accepted, want " + e.get("error"));
            } catch (WireException x) {
                assertEquals(e.get("error"), TransportError.name(x.code()), e.get("id") + ": " + x.getMessage());
            }
            n++;
        }
        assertTrue(n > 80);
    }

    /** CONFORMANCE.md Section 4.1: re-encoding is no longer, decodes to the same frame and is stable. */
    static void checkRoundTrip(String id, byte[] in, boolean exact) {
        ByteBuffer b = ByteBuffer.wrap(in);
        Frame f1 = new Frame();
        FrameDecoder d = new FrameDecoder();
        try {
            if (d.decode(b, 0, b.limit(), f1) != b.limit()) return;
        } catch (WireException x) {
            return;
        }
        byte[] e1 = encode(f1, in.length);
        assertTrue(e1.length <= in.length, id + ": re-encoding longer");
        if (exact) assertArrayEquals(in, e1, id + ": re-encoding differs");
        Frame f2 = new Frame();
        try {
            d.decode(ByteBuffer.wrap(e1), 0, e1.length, f2);
        } catch (WireException x) {
            fail(id + ": re-encoding does not decode: " + x.getMessage());
        }
        assertArrayEquals(e1, encode(f2, e1.length), id + ": unstable re-encoding");
    }

    static byte[] encode(Frame f, int bound) {
        ByteBuffer out = ByteBuffer.allocate(bound + 16);
        FrameEncoder.encode(out, f);
        byte[] a = new byte[out.position()];
        out.flip().get(a);
        return a;
    }
}
