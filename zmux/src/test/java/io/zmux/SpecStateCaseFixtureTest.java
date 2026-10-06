package io.zmux;

import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Protocol;
import io.zmux.protocol.Varint62;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.zmux.SpecFixturePeer.LOCAL_BIDI;
import static io.zmux.SpecFixturePeer.LOCAL_UNI;
import static io.zmux.SpecFixturePeer.PEER_BIDI;
import static io.zmux.SpecFixturePeer.WAIT;
import static io.zmux.SpecFixturePeer.bytes;
import static io.zmux.SpecFixtures.has;
import static io.zmux.SpecFixtures.map;
import static io.zmux.SpecFixtures.mapList;
import static io.zmux.SpecFixtures.string;
import static io.zmux.SpecFixtures.stringList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs the zmux-spec {@code portable_state} case set (state_cases that use only the portable event/result
 * vocabulary of {@code examples/fixture_mapping.md} section 2.1) against a real Java session driven by a raw peer.
 *
 * <p>The other state cases are labels of the Go reference harness and are not portable; they are only inventoried
 * by {@code SpecWireFixtureTest}. Initial states are reached with ordinary frames and API calls, each event is driven
 * exactly as the vocabulary defines it, and {@code expect_state} is checked through what the Java API can observe
 * (a read that times out, ends or fails; a write that succeeds and reaches the wire, or fails).
 */
final class SpecStateCaseFixtureTest {
    private static final int LATE_DATA_BYTES = 100;

    @TestFactory
    List<DynamicTest> portableStateCasesBehaveAsSpecified() {
        Map<String, Map<String, Object>> cases = SpecFixtures.byId(SpecFixtures.loadNdjson("state_cases.ndjson"));
        List<String> portable = stringList(map(SpecFixtures.loadJson("case_sets.json"), "sets"), "portable_state");
        assertFalse(portable.isEmpty(), "portable_state case set should not be empty");
        List<DynamicTest> tests = new ArrayList<>();
        for (String id : portable) {
            Map<String, Object> fixture = cases.get(id);
            tests.add(DynamicTest.dynamicTest(id, () -> {
                assertNotNull(fixture, "portable_state references unknown state case " + id);
                runPortableCase(id, fixture);
            }));
        }
        return tests;
    }

    private static void runPortableCase(String id, Map<String, Object> fixture) throws Exception {
        List<Map<String, Object>> steps = mapList(fixture, "steps");
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build())) {
            Target target = establish(peer, string(fixture, "stream_kind"), string(fixture, "ownership"),
                    fixture.get("initial_state"), string(steps.get(0), "event"));
            if (fixture.get("initial_state") instanceof Map) {
                assertState(id + " initial_state", peer, target, SpecFixtures.asMap(fixture.get("initial_state")));
            }
            peer.discardUnconsumed();
            for (int i = 0; i < steps.size(); i++) {
                Map<String, Object> step = steps.get(i);
                String label = id + " step " + (i + 1) + " " + string(step, "event");
                applyEvent(label, peer, target, string(step, "event"));
                if (has(step, "expect_result")) {
                    assertResult(label, peer, target, string(step, "expect_result"));
                }
                if (has(step, "expect_state")) {
                    assertState(label, peer, target, map(step, "expect_state"));
                }
                if (peer.session().state().terminal()) {
                    assertEquals(steps.size() - 1, i, label + ": the session ended before the last step");
                    break;
                }
                peer.discardUnconsumed();
            }
        }
    }

    /** Reaches {@code initial_state} for the case's stream S with ordinary frames and API calls. */
    private static Target establish(SpecFixturePeer peer, String kind, String ownership, Object initial, String firstEvent) throws Exception {
        Target target = new Target();
        boolean bidi;
        if ("bidi".equals(kind)) {
            bidi = true;
        } else if ("uni_local_send_only".equals(kind)) {
            bidi = false;
            assertEquals("local_owned", ownership, "a local send-only uni stream is locally owned");
        } else {
            throw new AssertionError("unsupported stream_kind " + kind);
        }
        boolean peerOwned;
        if ("peer_owned".equals(ownership)) {
            peerOwned = true;
            assertTrue(bidi, "peer-owned portable streams are bidirectional");
        } else if ("local_owned".equals(ownership)) {
            peerOwned = false;
        } else {
            throw new AssertionError("unsupported ownership " + ownership);
        }

        if ("idle".equals(initial)) {
            if (peerOwned) {
                target.streamId = PEER_BIDI;
            } else if (firstEvent.startsWith("local_")) {
                // A local API object that has not sent anything yet, so its ID is not committed on the wire.
                target.stream = bidi ? peer.session().openStream() : peer.session().openUniStream();
                target.streamId = -1L;
            } else {
                // Never opened locally: frames from the peer name an ID the session has not used.
                target.streamId = bidi ? LOCAL_BIDI : LOCAL_UNI;
            }
            return target;
        }

        Map<String, Object> halves = SpecFixtures.asMap(initial);
        String send = string(halves, "send_half");
        String recv = string(halves, "recv_half");
        if (peerOwned) {
            target.streamId = PEER_BIDI;
            target.stream = peer.openPeerStream(PEER_BIDI);
            if ("send_open".equals(send) && "recv_open".equals(recv)) {
                return target;
            }
            if ("send_open".equals(send) && "recv_reset".equals(recv)) {
                peer.send(FrameType.RESET, 0, PEER_BIDI, Varint62.encode(ErrorCode.CANCELLED.code()));
                peer.sync();
                return target;
            }
            if ("send_aborted".equals(send) && "recv_aborted".equals(recv)) {
                peer.send(FrameType.ABORT, 0, PEER_BIDI, Varint62.encode(ErrorCode.CANCELLED.code()));
                peer.sync();
                return target;
            }
            throw new AssertionError("unsupported peer-owned initial_state " + halves);
        }
        if ("send_open".equals(send) && (bidi ? "recv_open".equals(recv) : "absent".equals(recv))) {
            ZmuxSendStream stream = bidi ? peer.session().openStream() : peer.session().openUniStream();
            stream.write(bytes("o"));
            FrameCodec.Frame opener = peer.await(frame -> frame.type() == FrameType.DATA && frame.streamId() != 0L, "local opener");
            target.stream = stream;
            target.streamId = opener.streamId();
            assertEquals(bidi ? LOCAL_BIDI : LOCAL_UNI, target.streamId, "first local stream ID of its class");
            return target;
        }
        throw new AssertionError("unsupported local-owned initial_state " + halves);
    }

    private static void applyEvent(String label, SpecFixturePeer peer, Target target, String event) throws Exception {
        long s = target.streamId;
        switch (event) {
            case "peer_first_DATA":
                assertNull(target.stream, label + ": S should be unopened");
                target.stream = peer.openPeerStream(s);
                break;
            case "peer_first_ABORT":
                peer.send(FrameType.ABORT, 0, s, Varint62.encode(ErrorCode.CANCELLED.code()));
                break;
            case "peer_first_RESET":
            case "peer_late_RESET":
                peer.send(FrameType.RESET, 0, s, Varint62.encode(ErrorCode.CANCELLED.code()));
                break;
            case "peer_first_MAX_DATA":
                peer.send(FrameType.MAX_DATA, 0, s, Varint62.encode(64L));
                break;
            case "peer_first_BLOCKED":
            case "peer_BLOCKED":
                peer.send(FrameType.BLOCKED, 0, s, Varint62.encode(64L));
                break;
            case "peer_first_opening_frame_stream_id_gap":
                peer.send(FrameType.DATA, 0, s + 4L, bytes("x"));
                break;
            case "peer_DATA":
                peer.send(FrameType.DATA, 0, s, bytes("y"));
                break;
            case "peer_DATA_FIN":
                peer.send(FrameType.DATA, Protocol.FRAME_FLAG_FIN, s, bytes("y"));
                break;
            case "peer_STOP_SENDING":
            case "peer_late_STOP_SENDING":
                peer.send(FrameType.STOP_SENDING, 0, s, Varint62.encode(ErrorCode.CANCELLED.code()));
                break;
            case "peer_late_DATA": {
                peer.sync();
                SessionStats.PressureStats before = peer.session().stats().pressure();
                target.sessionReceivedBefore = before.recvSessionReceivedBytes();
                target.sessionRestorableBefore = restorableSessionCredit(before);
                peer.send(FrameType.DATA, 0, s, new byte[LATE_DATA_BYTES]);
                break;
            }
            case "peer_same_MAX_DATA":
                peer.send(FrameType.MAX_DATA, 0, s, Varint62.encode(0L));
                break;
            case "peer_same_BLOCKED":
                peer.send(FrameType.BLOCKED, 0, s, Varint62.encode(0L));
                break;
            case "local_DATA_FIN": {
                assertTrue(target.stream instanceof ZmuxSendStream, label + ": S has no local send direction");
                ((ZmuxSendStream) target.stream).closeWrite();
                peer.await(frame -> frame.type() == FrameType.DATA && frame.streamId() == s
                        && (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0, "DATA|FIN on stream " + s);
                break;
            }
            case "local_STOP_SENDING":
                target.localFailure = attemptLocalReceiveOperation(target.stream, true);
                break;
            case "local_MAX_DATA":
                target.localFailure = attemptLocalReceiveOperation(target.stream, false);
                break;
            default:
                throw new AssertionError(label + ": event outside the portable vocabulary");
        }
    }

    /**
     * A receive-side operation on a stream without a local receive direction. When the Java type has no such
     * operation at all the attempt is impossible by construction, which is the strongest form of "local invalid".
     */
    private static Throwable attemptLocalReceiveOperation(Object stream, boolean stopReading) {
        assertNotNull(stream, "S should be a local API object");
        if (!(stream instanceof ZmuxRecvStream)) {
            return new StreamNotReadableException();
        }
        ZmuxRecvStream recv = (ZmuxRecvStream) stream;
        try {
            if (stopReading) {
                recv.closeRead();
            } else {
                recv.setReadDeadline(Instant.now().plusMillis(50));
                recv.read(new byte[1]);
            }
        } catch (IOException e) {
            return e;
        }
        return null;
    }

    private static void assertResult(String label, SpecFixturePeer peer, Target target, String result) throws Exception {
        long s = target.streamId;
        switch (result) {
            case "protocol_violation":
                assertEquals(ErrorCode.PROTOCOL, peer.awaitSessionClose(), label + ": session CLOSE code");
                break;
            case "abort_stream_state":
                assertEquals(ErrorCode.STREAM_STATE, peer.awaitStreamAbort(s), label + ": ABORT code");
                assertEquals(SessionState.READY, peer.session().state(), label + ": session should stay open");
                break;
            case "local_invalid": {
                Throwable failure = target.localFailure;
                assertNotNull(failure, label + ": the local operation should fail");
                assertTrue(failure instanceof StreamNotReadableException || failure instanceof ReadClosedException,
                        label + ": expected a local stream-side error, got " + failure);
                peer.sync();
                for (FrameCodec.Frame frame : peer.drain()) {
                    assertEquals(0L, frame.streamId(), label + ": nothing may be sent for S, saw " + frame.type() + " on " + frame.streamId());
                }
                break;
            }
            case "sender_must_finish_with_reset_or_fin": {
                FrameCodec.Frame finish = peer.await(frame -> frame.streamId() == s
                        && (frame.type() == FrameType.RESET
                        || (frame.type() == FrameType.DATA && (frame.flags() & Protocol.FRAME_FLAG_FIN) != 0)), "RESET or DATA|FIN on stream " + s);
                assertNotNull(finish, label);
                assertEquals(SessionState.READY, peer.session().state(), label + ": session should stay open");
                break;
            }
            case "restore_session_budget_only_after_terminal_data": {
                peer.sync();
                assertEquals(SessionState.READY, peer.session().state(), label + ": session should stay open");
                peer.assertNothingSentOn(s);
                SessionStats.PressureStats after = peer.session().stats().pressure();
                assertEquals(target.sessionReceivedBefore + LATE_DATA_BYTES, after.recvSessionReceivedBytes(),
                        label + ": discarded bytes must count against the session receive window");
                assertTrue(restorableSessionCredit(after) >= target.sessionRestorableBefore,
                        label + ": discarded bytes must be released back to the session window");
                assertEquals(0L, after.bufferedReceiveBytes(), label + ": terminal-stream DATA must not be buffered");
                break;
            }
            case "no_control_flush":
                peer.sync();
                assertEquals(SessionState.READY, peer.session().state(), label + ": session should stay open");
                List<FrameCodec.Frame> sent = peer.drain();
                assertTrue(sent.isEmpty(), label + ": nothing may be sent in response, saw " + sent.size() + " frame(s)");
                break;
            default:
                throw new AssertionError(label + ": result outside the portable vocabulary");
        }
    }

    private static long restorableSessionCredit(SessionStats.PressureStats pressure) {
        return pressure.recvSessionAdvertisedBytes() - pressure.recvSessionReceivedBytes() + pressure.recvSessionPendingBytes();
    }

    /** Checks the conceptual half states through what the API can observe. */
    private static void assertState(String label, SpecFixturePeer peer, Target target, Map<String, Object> expect) throws Exception {
        String send = string(expect, "send_half");
        String recv = string(expect, "recv_half");
        if (target.stream == null) {
            // ABORT-first: the ID is used and terminal but no stream object ever surfaces.
            assertEquals("send_aborted", send, label + ": only an ABORT-first stream has no API object");
            assertEquals("recv_aborted", recv, label + ": only an ABORT-first stream has no API object");
            assertThrows(AcceptTimeoutException.class, () -> peer.session().acceptStream(Duration.ofMillis(50)),
                    label + ": a control-opened stream must not surface to accept");
            assertEquals(SessionState.READY, peer.session().state(), label + ": session should stay open");
            return;
        }

        if ("absent".equals(recv)) {
            assertFalse(target.stream instanceof ZmuxRecvStream && !(target.stream instanceof ZmuxStream),
                    label + ": S should have no receive direction");
        } else {
            assertTrue(target.stream instanceof ZmuxRecvStream, label + ": S should have a receive direction");
            String observed = probeReceive((ZmuxRecvStream) target.stream);
            switch (recv) {
                case "recv_open":
                case "recv_fin":
                    assertEquals(recv, observed, label + ": recv_half");
                    break;
                case "recv_reset":
                case "recv_aborted":
                    assertEquals("recv_error", observed, label + ": recv_half " + recv + " should fail reads");
                    break;
                default:
                    fail(label + ": unsupported recv_half " + recv);
            }
        }

        assertTrue(target.stream instanceof ZmuxSendStream, label + ": S should have a send direction");
        ZmuxSendStream sendStream = (ZmuxSendStream) target.stream;
        switch (send) {
            case "send_open": {
                sendStream.write(bytes("w"));
                long s = target.streamId;
                FrameCodec.Frame data = peer.await(frame -> frame.type() == FrameType.DATA && frame.streamId() == s, "DATA on open send half " + s);
                assertEquals(0, data.flags() & Protocol.FRAME_FLAG_FIN, label + ": an open send half must not FIN");
                break;
            }
            case "send_fin":
            case "send_reset":
            case "send_aborted":
                assertThrows(IOException.class, () -> sendStream.write(bytes("w")), label + ": " + send + " must reject writes");
                break;
            default:
                fail(label + ": unsupported send_half " + send);
        }
    }

    private static String probeReceive(ZmuxRecvStream stream) throws IOException {
        byte[] buffer = new byte[256];
        try {
            stream.setReadDeadline(Instant.now().plusMillis(50));
            while (true) {
                int n = stream.read(buffer);
                if (n < 0) {
                    return "recv_fin";
                }
            }
        } catch (ReadTimeoutException e) {
            return "recv_open";
        } catch (IOException e) {
            return "recv_error";
        } finally {
            try {
                stream.setReadDeadline(null);
            } catch (IOException ignored) {
                // a terminal receive half may refuse deadline changes
            }
        }
    }

    private static final class Target {
        private long streamId;
        private Object stream;
        private Throwable localFailure;
        private long sessionReceivedBefore;
        private long sessionRestorableBefore;
    }
}
