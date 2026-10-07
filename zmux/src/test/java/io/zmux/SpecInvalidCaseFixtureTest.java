package io.zmux;

import io.zmux.protocol.*;
import io.zmux.transport.BasicDuplexConnection;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.zmux.SpecFixturePeer.*;
import static io.zmux.SpecFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs every case of the vendored zmux-spec {@code invalid_cases.ndjson} against the Java implementation.
 *
 * <p>Like the Go harness, each fixture id needs an explicit runner here and an unmapped id fails, so a new or
 * renamed upstream case cannot pass silently. Runners report what they observed (a codec/session error code, an
 * {@code ABORT} code on a stream, or the local-policy action they verified) and the expected result always comes
 * from the fixture itself; there is no local override table. Session-scope cases are checked on the wire: a raw
 * peer (role initiator, so the Java session is the responder) sends the frames and must receive {@code CLOSE} with
 * the fixture's code.
 */
final class SpecInvalidCaseFixtureTest {
    private static final Map<String, Runner> RUNNERS = new LinkedHashMap<>();

    static {
        RUNNERS.put("preface_duplicate_setting_id", fixture -> {
            assertEquals("settings_tlv", string(inputShape(fixture), "type"), "unexpected input_shape");
            ByteArrayOutputStream settings = new ByteArrayOutputStream();
            long value = 100L;
            for (Object id : SpecFixtures.list(inputShape(fixture), "setting_ids")) {
                FrameCodec.appendTlv(settings, (Long) id, Varint62.encode(value++));
            }
            return prefaceCase(prefaceBytes(Role.INITIATOR.code(), 0L, 1L, 1L, settings.toByteArray()), ZmuxConfig.builder().build());
        });
        RUNNERS.put("preface_invalid_role_value", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_auto_equal_nonce_conflict", fixture -> {
            Map<String, Object> shape = inputShape(fixture);
            ZmuxConfig local = ZmuxConfig.builder()
                    .role(role(string(shape, "local_role")))
                    .tieBreakerNonce(longValue(shape, "local_tie_breaker_nonce"))
                    .build();
            return prefaceCase(prefaceBytes(role(string(shape, "peer_role")).code(), longValue(shape, "peer_tie_breaker_nonce"), 1L, 1L, new byte[0]), local);
        });
        RUNNERS.put("preface_auto_zero_nonce", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_frame_payload_limit_too_small", SpecInvalidCaseFixtureTest::prefaceSettingLimitCase);
        RUNNERS.put("preface_control_payload_limit_too_small", SpecInvalidCaseFixtureTest::prefaceSettingLimitCase);
        RUNNERS.put("preface_extension_payload_limit_too_small", SpecInvalidCaseFixtureTest::prefaceSettingLimitCase);
        RUNNERS.put("preface_invalid_magic", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_unsupported_preface_ver", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_no_protocol_version_overlap", fixture -> {
            Map<String, Object> shape = inputShape(fixture);
            ZmuxConfig local = ZmuxConfig.builder()
                    .minProto(longValue(shape, "local_min_proto"))
                    .maxProto(longValue(shape, "local_max_proto"))
                    .build();
            return prefaceCase(prefaceBytes(Role.INITIATOR.code(), 0L, longValue(shape, "peer_min_proto"), longValue(shape, "peer_max_proto"), new byte[0]), local);
        });
        RUNNERS.put("preface_explicit_same_role_conflict", fixture -> {
            Map<String, Object> shape = inputShape(fixture);
            ZmuxConfig local = ZmuxConfig.builder().role(role(string(shape, "local_role"))).build();
            return prefaceCase(prefaceBytes(role(string(shape, "peer_role")).code(), 0L, 1L, 1L, new byte[0]), local);
        });
        RUNNERS.put("preface_settings_len_exceeds_limit", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_duplicate_padding", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_duplicate_unknown_setting_id", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_setting_empty_value", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_setting_truncated_value", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_setting_noncanonical_value", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_setting_trailing_byte", SpecInvalidCaseFixtureTest::hexPrefaceCase);
        RUNNERS.put("preface_settings_tlv_overrun", SpecInvalidCaseFixtureTest::hexPrefaceCase);

        RUNNERS.put("frame_length_too_small", fixture -> {
            long frameLength = longValue(inputShape(fixture), "frame_length");
            byte[] body = new byte[(int) frameLength];
            Arrays.fill(body, (byte) FrameType.DATA.code());
            return invalidFrameCase(concat(Varint62.encode(frameLength), body), 0L, false);
        });
        RUNNERS.put("frame_ext_payload_underflow", fixture -> {
            assertEquals("EXT", string(inputShape(fixture), "frame_type"), "unexpected input_shape");
            assertEquals("too_small_for_ext_type_prefix", string(inputShape(fixture), "frame_length_shape"), "unexpected input_shape");
            return invalidFrameCase(rawFrame(FrameType.EXT.code(), 0L, new byte[0]), 0L, false);
        });
        RUNNERS.put("frame_length_smaller_than_stream_id_prefix", fixture -> {
            Map<String, Object> shape = inputShape(fixture);
            int encodingLength = (int) longValue(shape, "stream_id_encoding_length");
            long streamId = encodingLength == 1 ? 0L : 1L << (8 * (encodingLength / 2) - 2);
            byte[] encodedStreamId = Varint62.encode(streamId);
            assertEquals(encodingLength, encodedStreamId.length, "stream_id encoding length");
            return invalidFrameCase(concat(Varint62.encode(longValue(shape, "frame_length")), new byte[]{(byte) FrameType.DATA.code()}, encodedStreamId), 0L, false);
        });
        RUNNERS.put("frame_ping_with_forbidden_fin_flag", fixture -> invalidFrameCase(hex(string(fixture, "hex")), 0L, false));
        RUNNERS.put("frame_pong_too_short", fixture -> {
            assertEquals("PONG", string(inputShape(fixture), "frame_type"), "unexpected input_shape");
            return invalidFrameCase(rawFrame(FrameType.PONG.code(), 0L, new byte[(int) longValue(inputShape(fixture), "payload_len")]), 0L, false);
        });
        RUNNERS.put("frame_ping_payload_exceeds_local_echoable_limit", SpecInvalidCaseFixtureTest::pingPayloadLimitCase);
        RUNNERS.put("frame_abort_on_stream_zero", fixture -> {
            assertEquals("ABORT", string(inputShape(fixture), "frame_type"), "unexpected input_shape");
            return invalidFrameCase(rawFrame(FrameType.ABORT.code(), longValue(inputShape(fixture), "stream_id"), Varint62.encode(ErrorCode.CANCELLED.code())), 0L, false);
        });
        RUNNERS.put("frame_max_data_trailing_garbage", fixture -> trailingGarbageCase(fixture, FrameType.MAX_DATA));
        RUNNERS.put("frame_blocked_trailing_garbage", fixture -> trailingGarbageCase(fixture, FrameType.BLOCKED));
        RUNNERS.put("frame_unknown_core_type", fixture -> invalidFrameCase(hex(string(fixture, "hex")), 0L, false));
        RUNNERS.put("frame_priority_update_duplicate_singleton", SpecInvalidCaseFixtureTest::priorityUpdateDuplicateSingletonCase);
        RUNNERS.put("frame_priority_update_truncated_tlv_header", fixture -> {
            assertEquals("truncated_stream_hint_tlv_header", string(inputShape(fixture), "payload_shape"), "unexpected input_shape");
            byte[] payload = concat(Varint62.encode(Protocol.EXT_PRIORITY_UPDATE), Varint62.encode(Protocol.METADATA_STREAM_PRIORITY));
            return invalidPriorityUpdateCase(fixture, payload);
        });
        RUNNERS.put("frame_priority_update_tlv_value_overrun", fixture -> {
            assertEquals("stream_hint_tlv_value_overrun", string(inputShape(fixture), "payload_shape"), "unexpected input_shape");
            byte[] payload = concat(
                    Varint62.encode(Protocol.EXT_PRIORITY_UPDATE),
                    Varint62.encode(Protocol.METADATA_STREAM_PRIORITY),
                    Varint62.encode(2L),
                    new byte[]{0x01}
            );
            return invalidPriorityUpdateCase(fixture, payload);
        });
        RUNNERS.put("frame_priority_update_noncanonical_value", fixture -> {
            byte[] raw = hex(string(fixture, "hex"));
            // One-byte frame_length, code and stream_id precede the EXT payload in this fixture.
            assertEquals(FrameType.EXT.code(), raw[1] & 0xff, "unexpected frame code");
            assertEquals(longValue(inputShape(fixture), "stream_id"), raw[2] & 0xffL, "unexpected stream_id");
            return invalidPriorityUpdateCase(fixture, Arrays.copyOfRange(raw, 3, raw.length));
        });
        RUNNERS.put("frame_priority_update_without_capability", SpecInvalidCaseFixtureTest::priorityUpdateWithoutCapabilityCase);
        RUNNERS.put("frame_priority_update_on_unused_stream", SpecInvalidCaseFixtureTest::priorityUpdateOnUnusedStreamCase);
        RUNNERS.put("frame_priority_update_on_terminal_stream", SpecInvalidCaseFixtureTest::priorityUpdateOnTerminalStreamCase);
        RUNNERS.put("frame_unknown_ext_subtype", SpecInvalidCaseFixtureTest::unknownExtSubtypeCase);
        RUNNERS.put("frame_data_open_metadata_without_capability", fixture -> {
            assertTrue(stringList(inputShape(fixture), "capabilities").isEmpty(), "unexpected input_shape");
            try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
                peer.send(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, PEER_BIDI, openMetadataPayload(
                        new long[]{Protocol.METADATA_OPEN_INFO}, new byte[][]{bytes("a")}, bytes("hi")));
                return Outcome.sessionError(peer.awaitSessionClose());
            }
        });
        RUNNERS.put("frame_data_open_metadata_on_open_stream", fixture -> {
            assertTrue(SpecFixtures.bool(inputShape(fixture), "stream_exists"), "unexpected input_shape");
            try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), Protocol.CAPABILITY_OPEN_METADATA, Settings.defaults())) {
                peer.send(FrameType.DATA, 0, PEER_BIDI, bytes("x"));
                peer.sync();
                peer.send(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, PEER_BIDI, openMetadataPayload(
                        new long[]{Protocol.METADATA_OPEN_INFO}, new byte[][]{bytes("a")}, bytes("hi")));
                return Outcome.sessionError(peer.awaitSessionClose());
            }
        });
        RUNNERS.put("frame_data_open_metadata_duplicate_singleton", SpecInvalidCaseFixtureTest::openMetadataDuplicateSingletonCase);
        RUNNERS.put("frame_data_exceeds_stream_max_data", SpecInvalidCaseFixtureTest::dataExceedsStreamMaxDataCase);
        RUNNERS.put("frame_first_max_data_on_unused_stream", SpecInvalidCaseFixtureTest::firstFrameOnUnusedStreamCase);
        RUNNERS.put("frame_first_blocked_on_unused_stream", SpecInvalidCaseFixtureTest::firstFrameOnUnusedStreamCase);
        RUNNERS.put("frame_first_stop_sending_on_unused_stream", SpecInvalidCaseFixtureTest::firstFrameOnUnusedStreamCase);
        RUNNERS.put("frame_first_reset_on_unused_stream", SpecInvalidCaseFixtureTest::firstFrameOnUnusedStreamCase);
        RUNNERS.put("frame_data_exceeds_session_max_data", SpecInvalidCaseFixtureTest::dataExceedsSessionMaxDataCase);
        RUNNERS.put("frame_peer_stream_id_gap", fixture -> {
            Map<String, Object> shape = inputShape(fixture);
            assertEquals(PEER_BIDI, longValue(shape, "expected_next_stream_id"), "fixture assumes the responder's view");
            try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
                peer.send(frameType(string(shape, "incoming_frame")), 0, longValue(shape, "incoming_stream_id"), bytes("x"));
                return Outcome.sessionError(peer.awaitSessionClose());
            }
        });
        RUNNERS.put("frame_blocked_wrong_side_uni", SpecInvalidCaseFixtureTest::wrongSideUniCase);
        RUNNERS.put("frame_data_wrong_side_uni", SpecInvalidCaseFixtureTest::wrongSideUniCase);
        RUNNERS.put("frame_max_data_wrong_side_uni", SpecInvalidCaseFixtureTest::wrongSideUniCase);
        RUNNERS.put("frame_stop_sending_wrong_side_uni", SpecInvalidCaseFixtureTest::wrongSideUniCase);
        RUNNERS.put("frame_reset_wrong_side_uni", SpecInvalidCaseFixtureTest::wrongSideUniCase);
        RUNNERS.put("session_goaway_last_accepted_increase", SpecInvalidCaseFixtureTest::goAwayIncreaseCase);
        RUNNERS.put("local_provisional_open_cancel_must_not_burn_stream_id", SpecInvalidCaseFixtureTest::provisionalOpenCancelCase);
        RUNNERS.put("hidden_control_opened_stream_exceeds_hard_cap_without_shedding", SpecInvalidCaseFixtureTest::hiddenControlHardCapCase);
        RUNNERS.put("late_data_after_close_read_exceeds_session_aggregate_cap", SpecInvalidCaseFixtureTest::lateDataAggregateCapCase);
        RUNNERS.put("rapid_open_abort_churn_without_local_limit", SpecInvalidCaseFixtureTest::rapidOpenAbortChurnCase);
    }

    private static void assertOutcome(String id, Map<String, Object> expected, Outcome outcome) {
        assertNotNull(outcome, id + ": runner returned no outcome");
        String scope = string(expected, "scope");
        if (has(expected, "error")) {
            ErrorCode code = SpecFixtures.errorCode(string(expected, "error"));
            if ("stream".equals(scope)) {
                assertEquals(Outcome.Kind.STREAM_ERROR, outcome.kind, id + ": expected a stream-scoped error, got " + outcome);
            } else {
                assertTrue("session".equals(scope) || "session_establishment".equals(scope), id + ": unsupported error scope " + scope);
                assertEquals(Outcome.Kind.SESSION_ERROR, outcome.kind, id + ": expected a session-scoped error, got " + outcome);
            }
            assertEquals(code, outcome.code, id + ": error code");
            return;
        }
        assertEquals(Outcome.Kind.ACTION, outcome.kind, id + ": expected a verified action, got " + outcome);
        assertEquals(string(expected, "action"), outcome.action, id + ": action");
    }

    private static Outcome hexPrefaceCase(Map<String, Object> fixture) throws Exception {
        byte[] raw = hex(string(fixture, "hex"));
        int role = raw.length > 5 ? raw[5] & 0xff : -1;
        ZmuxConfig local = ZmuxConfig.builder()
                .role(role == Role.INITIATOR.code() ? Role.RESPONDER : Role.INITIATOR)
                .build();
        return prefaceCase(raw, local);
    }

    private static Outcome prefaceSettingLimitCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        long settingId;
        switch (string(shape, "setting")) {
            case "max_frame_payload":
                settingId = Protocol.SETTING_MAX_FRAME_PAYLOAD;
                break;
            case "max_control_payload_bytes":
                settingId = Protocol.SETTING_MAX_CONTROL_PAYLOAD_BYTES;
                break;
            case "max_extension_payload_bytes":
                settingId = Protocol.SETTING_MAX_EXTENSION_PAYLOAD_BYTES;
                break;
            default:
                throw new AssertionError("unsupported setting " + string(shape, "setting"));
        }
        ByteArrayOutputStream settings = new ByteArrayOutputStream();
        FrameCodec.appendTlv(settings, settingId, Varint62.encode(longValue(shape, "value")));
        return prefaceCase(prefaceBytes(Role.INITIATOR.code(), 0L, 1L, 1L, settings.toByteArray()), ZmuxConfig.builder().build());
    }

    /**
     * Judges one peer preface twice: with the codec (parse, then negotiate against the local preface) and with a real
     * session establishment that reads the same bytes. Both must fail with the same code.
     */
    private static Outcome prefaceCase(byte[] peerPreface, ZmuxConfig local) throws Exception {
        ErrorCode codecCode = codecPrefaceFailure(peerPreface, local);
        ErrorCode sessionCode = sessionPrefaceFailure(peerPreface, local);
        assertEquals(codecCode, sessionCode, "codec and session establishment must reject the preface with the same code");
        return Outcome.sessionError(sessionCode);
    }

    private static ErrorCode codecPrefaceFailure(byte[] peerPreface, ZmuxConfig local) {
        try {
            Preface peer = ZmuxCodec.parsePreface(peerPreface);
            assertEquals(peer, FrameCodec.decoder(new ByteArrayInputStream(peerPreface)).readPreface(), "preface readers disagree");
            ZmuxCodec.negotiatePrefaces(local.localPreface(), peer);
        } catch (ZmuxException e) {
            ErrorCode code = ErrorCode.fromCode(e.code());
            ZmuxException decoderError = assertThrows(
                    ZmuxException.class,
                    () -> {
                        Preface decoded = FrameCodec.decoder(new ByteArrayInputStream(peerPreface)).readPreface();
                        ZmuxCodec.negotiatePrefaces(local.localPreface(), decoded);
                    },
                    "Decoder preface path must reject the preface too"
            );
            assertEquals(code, ErrorCode.fromCode(decoderError.code()), "preface readers disagree on the error code");
            return code;
        } catch (IOException e) {
            throw new AssertionError("preface codec failed without a zmux error code", e);
        }
        throw new AssertionError("preface codec accepted an invalid preface " + hex(peerPreface));
    }

    private static ErrorCode sessionPrefaceFailure(byte[] peerPreface, ZmuxConfig local) throws Exception {
        try (ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             Socket peerSocket = new Socket("127.0.0.1", listener.getLocalPort());
             Socket sessionSocket = listener.accept()) {
            BasicDuplexConnection connection = new BasicDuplexConnection(
                    sessionSocket.getInputStream(),
                    sessionSocket.getOutputStream(),
                    sessionSocket,
                    sessionSocket.getLocalSocketAddress(),
                    sessionSocket.getRemoteSocketAddress()
            );
            AtomicReference<Throwable> openError = new AtomicReference<>();
            AtomicReference<ZmuxSession> opened = new AtomicReference<>();
            CountDownLatch finished = new CountDownLatch(1);
            Thread thread = new Thread(() -> {
                try {
                    opened.set(Zmux.open(connection, local));
                } catch (Throwable t) {
                    openError.set(t);
                } finally {
                    finished.countDown();
                }
            }, "spec-invalid-preface-open");
            thread.setDaemon(true);
            thread.start();

            BufferedOutputStream output = new BufferedOutputStream(peerSocket.getOutputStream());
            output.write(peerPreface);
            output.flush();
            assertTrue(finished.await(WAIT.toMillis(), TimeUnit.MILLISECONDS), "session establishment did not finish");
            if (opened.get() != null) {
                opened.get().close();
                throw new AssertionError("session establishment accepted an invalid preface " + hex(peerPreface));
            }
            ErrorCode code = ZmuxErrors.code(openError.get());
            assertNotNull(code, "establishment failure should carry a zmux error code: " + openError.get());
            return code;
        }
    }

    // ---- preface runners ----

    /**
     * Judges one encoded frame with every codec reader and with a live session. {@code deferredToSession} marks the
     * extension-subtype cases that the session-frame reader leaves to the session (it checks negotiation first).
     */
    private static Outcome invalidFrameCase(byte[] raw, long peerCapabilities, boolean deferredToSession, FrameCodec.Frame... setup) throws Exception {
        Limits limits = Settings.defaults().limits();
        ErrorCode codecCode = codecFailure("ZmuxCodec.parseFrame", () -> ZmuxCodec.parseFrame(raw, limits));
        assertEquals(codecCode, codecFailure("FrameCodec.readFrame", () -> FrameCodec.readFrame(new ByteArrayInputStream(raw), limits)));
        assertEquals(codecCode, codecFailure("Decoder.readFrame", () -> FrameCodec.decoder(new ByteArrayInputStream(raw)).readFrame(limits)));
        assertEquals(codecCode, codecFailure("FrameEnvelopeCodec.readInboundFrame",
                () -> FrameEnvelopeCodec.readInboundFrame(FrameCodec.decoder(new ByteArrayInputStream(raw)), limits, null)));
        if (deferredToSession) {
            FrameEnvelopeCodec.readInboundSessionFrame(FrameCodec.decoder(new ByteArrayInputStream(raw)), limits, null);
        } else {
            assertEquals(codecCode, codecFailure("FrameEnvelopeCodec.readInboundSessionFrame",
                    () -> FrameEnvelopeCodec.readInboundSessionFrame(FrameCodec.decoder(new ByteArrayInputStream(raw)), limits, null)));
        }

        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), peerCapabilities, Settings.defaults())) {
            for (FrameCodec.Frame frame : setup) {
                peer.send(frame);
            }
            if (setup.length > 0) {
                peer.sync();
            }
            peer.sendRaw(raw);
            Outcome outcome = Outcome.sessionError(peer.awaitSessionClose());
            assertEquals(codecCode, outcome.code, "codec and session must agree on the error code for " + hex(raw));
            return outcome;
        }
    }

    private static ErrorCode codecFailure(String label, Executable read) {
        ZmuxException error = assertThrows(ZmuxException.class, read, label + " must reject the frame");
        return ErrorCode.fromCode(error.code());
    }

    private static Outcome trailingGarbageCase(Map<String, Object> fixture, FrameType type) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        assertEquals(type.name(), string(shape, "frame_type"), "unexpected input_shape");
        assertEquals("canonical_varint_plus_trailing_bytes", string(shape, "payload_shape"), "unexpected input_shape");
        byte[] payload = concat(Varint62.encode(1024L), new byte[]{0x01});
        return invalidFrameCase(rawFrame(type.code(), longValue(shape, "stream_id"), payload), 0L, false);
    }

    private static Outcome invalidPriorityUpdateCase(Map<String, Object> fixture, byte[] payload) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        assertEquals("PRIORITY_UPDATE", string(shape, "ext_type"), "unexpected input_shape");
        long streamId = longValue(shape, "stream_id");
        // Open the target stream first so the receiver unquestionably parses the update payload.
        return invalidFrameCase(
                rawFrame(FrameType.EXT.code(), streamId, payload),
                capabilities(shape),
                true,
                new FrameCodec.Frame(FrameType.DATA, 0, streamId, bytes("x"))
        );
    }

    private static Outcome pingPayloadLimitCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        long localLimit = longValue(shape, "local_max_control_payload_bytes");
        long peerLimit = longValue(shape, "peer_max_control_payload_bytes");
        int attempted = (int) longValue(shape, "attempted_ping_payload_len");
        ZmuxConfig config = ZmuxConfig.builder()
                .settings(Settings.defaults().toBuilder().maxControlPayloadBytes(localLimit).build())
                .build();
        try (SpecFixturePeer peer = SpecFixturePeer.open(config, 0L, Settings.defaults().toBuilder().maxControlPayloadBytes(peerLimit).build())) {
            // PING payload = 8-byte token + echo, so this echo makes the attempted payload length.
            byte[] echo = new byte[attempted - 8];
            IOException error = assertThrows(IOException.class, () -> peer.session().ping(echo, Duration.ofSeconds(1)),
                    "a PING above min(local, peer) control payload limit must not be sent");
            assertEquals(ErrorCode.FRAME_SIZE, ZmuxErrors.code(error), "oversized PING error code");
            assertTrue(peer.drain().stream().noneMatch(frame -> frame.type() == FrameType.PING), "oversized PING must not reach the wire");

            byte[] fitting = new byte[(int) Math.min(localLimit, peerLimit) - 8 - 64];
            Thread pinger = new Thread(() -> {
                try {
                    peer.session().ping(fitting, Duration.ofSeconds(2));
                } catch (Exception ignored) {
                    // the raw peer answers below; failures surface through the frame assertions
                }
            }, "spec-ping-limit");
            pinger.setDaemon(true);
            pinger.start();
            FrameCodec.Frame ping = peer.await(frame -> frame.type() == FrameType.PING, "PING within the limit");
            assertTrue(ping.payload().length <= Math.min(localLimit, peerLimit), "PING payload exceeds the echoable limit");
            peer.send(FrameType.PONG, 0, 0L, ping.payload());
            pinger.join(WAIT.toMillis());
            assertEquals(SessionState.READY, peer.session().state(), "the refused PING must not affect the session");
        }
        return Outcome.action("forbid_send");
    }

    // ---- frame-shape runners ----

    private static Outcome priorityUpdateDuplicateSingletonCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        long streamId = longValue(shape, "stream_id");
        List<Map<String, Object>> tlvs = SpecFixtures.mapList(shape, "stream_metadata_tlvs");
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        Varint62.write(payload, Protocol.EXT_PRIORITY_UPDATE);
        for (Map<String, Object> tlv : tlvs) {
            assertEquals("stream_priority", string(tlv, "type"), "unexpected input_shape TLV");
            FrameCodec.appendTlv(payload, Protocol.METADATA_STREAM_PRIORITY, Varint62.encode(longValue(tlv, "value")));
        }
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), capabilities(shape), Settings.defaults())) {
            ZmuxStream stream = peer.openPeerStream(streamId);
            peer.send(FrameType.EXT, 0, streamId, payload.toByteArray());
            peer.sync();
            assertEquals(SessionState.READY, peer.session().state(), "duplicate singleton must not fail the session");
            assertEquals(0L, stream.metadata().priority(), "duplicate singleton update must be ignored entirely");
            assertNoErrorSignal(peer, streamId);

            // Control: a well-formed update on the same stream is applied, so the ignore above is not vacuous.
            peer.send(FrameType.EXT, 0, streamId, FrameCodec.buildPriorityUpdatePayload(
                    capabilities(shape), 9L, null, Settings.defaults().maxExtensionPayloadBytes()));
            peer.sync();
            assertEquals(9L, stream.metadata().priority(), "a valid PRIORITY_UPDATE should still apply");
        }
        return Outcome.action("ignore_entire_update_payload");
    }

    private static Outcome priorityUpdateWithoutCapabilityCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        assertTrue(stringList(shape, "capabilities").isEmpty(), "unexpected input_shape");
        long streamId = longValue(shape, "stream_id");
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            ZmuxStream stream = peer.openPeerStream(streamId);
            peer.send(FrameType.EXT, 0, streamId, priorityUpdatePayload(9L));
            peer.sync();
            assertEquals(SessionState.READY, peer.session().state(), "unnegotiated PRIORITY_UPDATE must not fail the session");
            assertEquals(0L, stream.metadata().priority(), "unnegotiated PRIORITY_UPDATE must be ignored");
            assertNoErrorSignal(peer, streamId);
        }
        return Outcome.action("ignore");
    }

    private static Outcome priorityUpdateOnUnusedStreamCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        assertFalse(SpecFixtures.bool(shape, "stream_exists"), "unexpected input_shape");
        long streamId = longValue(shape, "stream_id");
        long caps = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), caps, Settings.defaults())) {
            peer.send(FrameType.EXT, 0, streamId, priorityUpdatePayload(9L));
            peer.sync();
            assertEquals(SessionState.READY, peer.session().state(), "PRIORITY_UPDATE on an unused stream must not fail the session");
            assertThrows(AcceptTimeoutException.class, () -> peer.session().acceptStream(Duration.ofMillis(50)),
                    "PRIORITY_UPDATE must not open the stream");
            assertNoErrorSignal(peer, streamId);

            // The ID was not consumed: DATA can still open it, without the ignored priority.
            ZmuxStream stream = peer.openPeerStream(streamId);
            assertEquals(streamId, stream.streamId(), "DATA should still open the stream ID");
            assertEquals(0L, stream.metadata().priority(), "the ignored update must not be applied later");
        }
        return Outcome.action("ignore");
    }

    private static Outcome priorityUpdateOnTerminalStreamCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        assertEquals("terminal", string(shape, "stream_state"), "unexpected input_shape");
        long streamId = longValue(shape, "stream_id");
        long caps = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), caps, Settings.defaults())) {
            ZmuxStream stream = peer.openPeerStream(streamId);
            peer.send(FrameType.ABORT, 0, streamId, Varint62.encode(ErrorCode.CANCELLED.code()));
            peer.sync();
            peer.send(FrameType.EXT, 0, streamId, priorityUpdatePayload(9L));
            peer.sync();
            assertEquals(SessionState.READY, peer.session().state(), "PRIORITY_UPDATE on a terminal stream must not fail the session");
            assertEquals(0L, stream.metadata().priority(), "PRIORITY_UPDATE must not revive a terminal stream");
            assertNoErrorSignal(peer, streamId);
        }
        return Outcome.action("ignore");
    }

    // ---- session-behaviour runners ----

    private static Outcome unknownExtSubtypeCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        long streamId = longValue(shape, "stream_id");
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            peer.send(FrameType.EXT, 0, streamId, concat(Varint62.encode(longValue(shape, "ext_type")), new byte[]{(byte) 0xaa, (byte) 0xbb}));
            peer.sync();
            assertEquals(SessionState.READY, peer.session().state(), "unknown EXT subtype must be ignored");
            assertNoErrorSignal(peer, streamId);
        }
        return Outcome.action("ignore");
    }

    private static Outcome openMetadataDuplicateSingletonCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        long streamId = longValue(shape, "stream_id");
        List<Map<String, Object>> tlvs = SpecFixtures.mapList(shape, "stream_metadata_tlvs");
        long[] types = new long[tlvs.size()];
        byte[][] values = new byte[tlvs.size()][];
        for (int i = 0; i < tlvs.size(); i++) {
            assertEquals("open_info", string(tlvs.get(i), "type"), "unexpected input_shape TLV");
            types[i] = Protocol.METADATA_OPEN_INFO;
            values[i] = hex(string(tlvs.get(i), "value_hex"));
        }
        byte[] appData = hex(string(shape, "application_payload_hex"));
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), Protocol.CAPABILITY_OPEN_METADATA, Settings.defaults())) {
            peer.send(FrameType.DATA, Protocol.FRAME_FLAG_OPEN_METADATA, streamId, openMetadataPayload(types, values, appData));
            ZmuxStream stream = peer.session().acceptStream(WAIT);
            assertEquals(streamId, stream.streamId(), "the DATA should still open the stream");
            assertEquals(0, stream.openInfo().length, "duplicate singleton must drop the whole OPEN_METADATA block");
            byte[] read = new byte[appData.length];
            int n = stream.read(read);
            assertEquals(hex(appData), hex(Arrays.copyOf(read, Math.max(n, 0))), "application payload must still be delivered");
            assertNoErrorSignal(peer, streamId);
        }
        return Outcome.action("ignore_entire_open_metadata_block");
    }

    private static Outcome dataExceedsStreamMaxDataCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        long streamId = longValue(shape, "stream_id");
        long received = longValue(shape, "stream_bytes_received");
        long limit = longValue(shape, "peer_stream_max_data");
        long incoming = longValue(shape, "incoming_data_length");
        assertTrue(received + incoming > limit && received <= limit, "fixture should overrun the stream window");
        Settings local = Settings.defaults().toBuilder()
                .initialMaxStreamDataBidiPeerOpened(limit)
                .initialMaxStreamDataBidiLocallyOpened(limit)
                .build();
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().settings(local).build(), 0L, Settings.defaults())) {
            peer.send(FrameType.DATA, 0, streamId, new byte[(int) received]);
            peer.sync();
            assertTrue(peer.drain().stream().noneMatch(frame -> frame.type() == FrameType.MAX_DATA && frame.streamId() == streamId),
                    "harness precondition: no stream credit was granted before the overrun");
            peer.send(FrameType.DATA, 0, streamId, new byte[(int) incoming]);
            Outcome outcome = Outcome.streamError(streamId, peer.awaitStreamAbort(streamId));
            assertEquals(SessionState.READY, peer.session().state(), "a stream window overrun must not fail the session");
            return outcome;
        }
    }

    private static Outcome dataExceedsSessionMaxDataCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        long streamId = longValue(shape, "stream_id");
        long received = longValue(shape, "session_bytes_received");
        long limit = longValue(shape, "peer_session_max_data");
        long incoming = longValue(shape, "incoming_data_length");
        assertTrue(received + incoming > limit && received <= limit, "fixture should overrun the session window");
        Settings local = Settings.defaults().toBuilder()
                .initialMaxData(limit)
                .initialMaxStreamDataBidiPeerOpened(4L * limit)
                .build();
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().settings(local).build(), 0L, Settings.defaults())) {
            peer.send(FrameType.DATA, 0, streamId, new byte[(int) received]);
            peer.sync();
            assertTrue(peer.drain().stream().noneMatch(frame -> frame.type() == FrameType.MAX_DATA && frame.streamId() == 0L),
                    "harness precondition: no session credit was granted before the overrun");
            peer.send(FrameType.DATA, 0, streamId, new byte[(int) incoming]);
            return Outcome.sessionError(peer.awaitSessionClose());
        }
    }

    private static Outcome firstFrameOnUnusedStreamCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        assertEquals("idle", string(shape, "initial_stream_state"), "unexpected input_shape");
        FrameType type = frameType(string(shape, "incoming_frame"));
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            peer.send(type, 0, longValue(shape, "stream_id"), firstFramePayload(type));
            return Outcome.sessionError(peer.awaitSessionClose());
        }
    }

    /**
     * Wrong-side frames on an opened uni stream: on a local send-only stream the peer may not send DATA, BLOCKED or
     * RESET; on a local receive-only stream it may not send MAX_DATA or STOP_SENDING.
     */
    private static Outcome wrongSideUniCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        FrameType type = frameType(string(shape, "incoming_frame"));
        String kind = string(shape, "stream_kind");
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            long streamId;
            if ("uni_local_send_only".equals(kind)) {
                ZmuxSendStream stream = peer.session().openUniStream();
                stream.write(bytes("x"));
                FrameCodec.Frame opener = peer.await(frame -> frame.type() == FrameType.DATA, "local uni opener");
                streamId = opener.streamId();
                assertEquals(LOCAL_UNI, streamId, "first local uni stream ID");
            } else if ("uni_local_receive_only".equals(kind)) {
                streamId = PEER_UNI;
                peer.send(FrameType.DATA, 0, streamId, bytes("x"));
                ZmuxRecvStream accepted = peer.session().acceptUniStream(WAIT);
                assertEquals(streamId, accepted.streamId(), "accepted uni stream ID");
            } else {
                throw new AssertionError("unsupported stream_kind " + kind);
            }
            peer.sync();
            peer.send(type, 0, streamId, type == FrameType.DATA ? bytes("y") : firstFramePayload(type));
            Outcome outcome = Outcome.streamError(streamId, peer.awaitStreamAbort(streamId));
            assertEquals(SessionState.READY, peer.session().state(), type + " from the wrong side must not fail the session");
            return outcome;
        }
    }

    private static Outcome goAwayIncreaseCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            peer.send(FrameType.GOAWAY, 0, 0L, FrameCodec.buildGoAwayPayload(
                    longValue(shape, "prior_last_accepted_bidi_stream_id"),
                    longValue(shape, "prior_last_accepted_uni_stream_id"),
                    ErrorCode.NO_ERROR.code(),
                    ""
            ));
            peer.sync();
            assertFalse(peer.session().state().terminal(), "the first GOAWAY must be accepted");
            peer.send(FrameType.GOAWAY, 0, 0L, FrameCodec.buildGoAwayPayload(
                    longValue(shape, "incoming_last_accepted_bidi_stream_id"),
                    longValue(shape, "incoming_last_accepted_uni_stream_id"),
                    ErrorCode.NO_ERROR.code(),
                    ""
            ));
            return Outcome.sessionError(peer.awaitSessionClose());
        }
    }

    private static Outcome provisionalOpenCancelCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        assertTrue(SpecFixtures.bool(shape, "cancel_before_first_frame_commit"), "unexpected input_shape");
        try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
            ZmuxStream first = peer.session().openStream();
            first.closeWithError(ErrorCode.CANCELLED.code(), "");
            ZmuxStream second = peer.session().openStream();
            second.write(bytes("x"));
            FrameCodec.Frame data = peer.await(frame -> frame.type() == FrameType.DATA, "DATA from the second stream");

            // Whatever reached the wire for this class must use contiguous IDs from the first one: either the
            // cancelled stream never used an ID, or its ID was consumed on the wire before the second opener.
            peer.drain();
            List<Long> order = new ArrayList<>();
            for (FrameCodec.Frame frame : peer.received()) {
                if (frame.streamId() != 0L && (frame.streamId() & 3L) == LOCAL_BIDI && !order.contains(frame.streamId())) {
                    order.add(frame.streamId());
                }
            }
            assertEquals(data.streamId(), (long) order.get(order.size() - 1), "the second stream should be the latest local bidi ID seen");
            for (int i = 0; i < order.size(); i++) {
                assertEquals(LOCAL_BIDI + 4L * i, (long) order.get(i), "local bidi IDs on the wire must have no gap: " + order);
            }
            assertEquals(data.streamId(), second.streamId(), "second stream ID");
        }
        return Outcome.action("forbid_gap");
    }

    private static Outcome hiddenControlHardCapCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        int count = (int) longValue(shape, "hidden_control_opened_count");
        int hardCap = (int) longValue(shape, "hidden_control_opened_hard_cap");
        assertEquals("keep_all_hidden_streams", string(shape, "attempted_action"), "unexpected input_shape");
        ZmuxConfig config = ZmuxConfig.builder()
                .acceptBacklogLimit(2 * hardCap)
                .hiddenAbortChurnThreshold(4 * count)
                .build();
        try (SpecFixturePeer peer = SpecFixturePeer.open(config, 0L, Settings.defaults())) {
            assertEquals(hardCap, peer.session().stats().hiddenState().hardCap(), "harness precondition: hidden-state hard cap");
            for (int i = 0; i < count; i++) {
                peer.send(FrameType.ABORT, 0, PEER_BIDI + 4L * i, Varint62.encode(ErrorCode.CANCELLED.code()));
            }
            peer.sync();
            assertEquals(SessionState.READY, peer.session().state(), "hidden control-opened streams under the churn limit must not fail the session");
            SessionStats.HiddenStateStats hidden = peer.session().stats().hiddenState();
            assertTrue(hidden.retained() <= hardCap, "retained hidden state " + hidden.retained() + " exceeds the hard cap " + hardCap);
            assertThrows(AcceptTimeoutException.class, () -> peer.session().acceptStream(Duration.ofMillis(50)),
                    "ABORT-first streams must not surface to accept");
            // The shed IDs stay used: DATA on the newest one is not a new stream.
            peer.send(FrameType.DATA, 0, PEER_BIDI + 4L * (count - 1), bytes("late"));
            peer.sync();
            assertEquals(SessionState.READY, peer.session().state(), "late DATA on a shed hidden stream must not fail the session");
            assertThrows(AcceptTimeoutException.class, () -> peer.session().acceptStream(Duration.ofMillis(50)),
                    "late DATA must not reopen a shed hidden stream");
        }
        return Outcome.action("forbid_unbounded_hidden_state");
    }

    private static Outcome lateDataAggregateCapCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        int directions = (int) longValue(shape, "stopped_directions");
        assertEquals("above_cap", string(shape, "late_data_session_aggregate"), "unexpected input_shape");
        int tail = 512;
        long aggregateCap = 16L * 1024L;
        assertTrue((long) directions * tail > aggregateCap, "harness precondition: the tails must exceed the aggregate cap");
        ZmuxConfig config = ZmuxConfig.builder()
                .aggregateLateDataCap(aggregateCap)
                .acceptBacklogLimit(2 * directions)
                .build();
        try (SpecFixturePeer peer = SpecFixturePeer.open(config, 0L, Settings.defaults())) {
            long initialSessionWindow = peer.session().stats().pressure().recvSessionAdvertisedBytes();
            List<Long> ids = new ArrayList<>();
            for (int i = 0; i < directions; i++) {
                long streamId = PEER_BIDI + 4L * i;
                ids.add(streamId);
                peer.send(FrameType.DATA, 0, streamId, bytes("x"));
                ZmuxStream stream = peer.session().acceptStream(WAIT);
                assertEquals(streamId, stream.streamId(), "accepted stream ID");
                stream.closeRead();
            }
            for (long streamId : ids) {
                peer.send(FrameType.DATA, 0, streamId, new byte[tail]);
            }
            peer.sync();
            assertEquals(SessionState.READY, peer.session().state(), "exceeding the aggregate late-data cap must not fail the session");
            SessionStats.PressureStats pressure = peer.session().stats().pressure();
            assertEquals(0L, pressure.bufferedReceiveBytes(), "late tails after CloseRead must not be buffered");
            long restorable = pressure.recvSessionAdvertisedBytes() - pressure.recvSessionReceivedBytes() + pressure.recvSessionPendingBytes();
            assertTrue(restorable >= initialSessionWindow,
                    "discarded late bytes must be released to the session window (restorable " + restorable + " < " + initialSessionWindow + ")");
        }
        return Outcome.action("forbid_unbounded_late_tail_buffering");
    }

    private static Outcome rapidOpenAbortChurnCase(Map<String, Object> fixture) throws Exception {
        Map<String, Object> shape = inputShape(fixture);
        assertEquals("open_then_abort_loop", string(shape, "pattern"), "unexpected input_shape");
        int threshold = 8;
        ZmuxConfig config = ZmuxConfig.builder()
                .hiddenAbortChurnThreshold(threshold)
                .hiddenAbortChurnWindow(Duration.ofHours(1))
                .build();
        try (SpecFixturePeer peer = SpecFixturePeer.open(config, 0L, Settings.defaults())) {
            for (int i = 0; i < threshold; i++) {
                peer.send(FrameType.ABORT, 0, PEER_BIDI + 4L * i, Varint62.encode(ErrorCode.CANCELLED.code()));
            }
            peer.sync();
            assertEquals(SessionState.READY, peer.session().state(), "churn at the configured threshold is still allowed");
            peer.send(FrameType.ABORT, 0, PEER_BIDI + 4L * threshold, Varint62.encode(ErrorCode.CANCELLED.code()));
            Outcome outcome = Outcome.sessionError(peer.awaitSessionClose());
            assertEquals(ErrorCode.PROTOCOL, outcome.code, "the local churn limit should close the session with PROTOCOL");
        }
        return Outcome.action("forbid_unbounded_stream_churn");
    }

    private static Map<String, Object> inputShape(Map<String, Object> fixture) {
        return map(fixture, "input_shape");
    }

    private static Role role(String name) {
        return Role.valueOf(name.toUpperCase(java.util.Locale.ROOT));
    }

    private static FrameType frameType(String name) {
        return FrameType.valueOf(name);
    }

    private static long capabilities(Map<String, Object> shape) {
        long caps = 0L;
        if (!has(shape, "capabilities")) {
            return caps;
        }
        for (String name : stringList(shape, "capabilities")) {
            switch (name) {
                case "open_metadata":
                    caps |= Protocol.CAPABILITY_OPEN_METADATA;
                    break;
                case "priority_hints":
                    caps |= Protocol.CAPABILITY_PRIORITY_HINTS;
                    break;
                case "stream_groups":
                    caps |= Protocol.CAPABILITY_STREAM_GROUPS;
                    break;
                case "priority_update":
                    // Priority values in an update are only meaningful with priority_hints as well.
                    caps |= Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
                    break;
                default:
                    throw new AssertionError("unsupported capability " + name);
            }
        }
        return caps;
    }

    private static byte[] firstFramePayload(FrameType type) throws IOException {
        switch (type) {
            case MAX_DATA:
            case BLOCKED:
                return Varint62.encode(64L);
            case STOP_SENDING:
            case RESET:
                return Varint62.encode(ErrorCode.CANCELLED.code());
            default:
                throw new AssertionError("no first-frame payload for " + type);
        }
    }

    // ---- helpers ----

    private static byte[] priorityUpdatePayload(long priority) throws IOException {
        return concat(
                Varint62.encode(Protocol.EXT_PRIORITY_UPDATE),
                Varint62.encode(Protocol.METADATA_STREAM_PRIORITY),
                Varint62.encode(Varint62.length(priority)),
                Varint62.encode(priority)
        );
    }

    private static byte[] openMetadataPayload(long[] types, byte[][] values, byte[] appData) throws IOException {
        ByteArrayOutputStream metadata = new ByteArrayOutputStream();
        for (int i = 0; i < types.length; i++) {
            FrameCodec.appendTlv(metadata, types[i], values[i]);
        }
        return concat(Varint62.encode(metadata.size()), metadata.toByteArray(), appData);
    }

    private static byte[] prefaceBytes(int role, long nonce, long minProto, long maxProto, byte[] settingsTlv) throws IOException {
        return concat(
                Protocol.MAGIC.getBytes(StandardCharsets.US_ASCII),
                new byte[]{Protocol.PREFACE_VERSION, (byte) role},
                Varint62.encode(nonce),
                Varint62.encode(minProto),
                Varint62.encode(maxProto),
                Varint62.encode(0L),
                Varint62.encode(settingsTlv.length),
                settingsTlv
        );
    }

    private static byte[] rawFrame(int code, long streamId, byte[] payload) throws IOException {
        byte[] encodedStreamId = Varint62.encode(streamId);
        return concat(
                Varint62.encode(1L + encodedStreamId.length + payload.length),
                new byte[]{(byte) code},
                encodedStreamId,
                payload
        );
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }

    /**
     * The case's frame must have been ignored: no CLOSE, and no ABORT, RESET or STOP_SENDING on the stream.
     */
    private static void assertNoErrorSignal(SpecFixturePeer peer, long streamId) throws IOException {
        for (FrameCodec.Frame frame : peer.drain()) {
            assertFalse(frame.type() == FrameType.CLOSE, "unexpected CLOSE");
            boolean streamError = frame.type() == FrameType.ABORT || frame.type() == FrameType.RESET || frame.type() == FrameType.STOP_SENDING;
            assertFalse(streamError && frame.streamId() == streamId, "unexpected " + frame.type() + " on stream " + streamId);
        }
    }

    @TestFactory
    List<DynamicTest> invalidCaseFixturesBehaveAsSpecified() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Map<String, Object> fixture : SpecFixtures.loadNdjson("invalid_cases.ndjson")) {
            String id = string(fixture, "id");
            tests.add(DynamicTest.dynamicTest(id, () -> {
                Runner runner = RUNNERS.get(id);
                assertNotNull(runner, "invalid fixture " + id + " has no Java runner; add one to " + SpecInvalidCaseFixtureTest.class.getSimpleName());
                Outcome outcome = runner.run(fixture);
                assertOutcome(id, map(fixture, "expected_result"), outcome);
            }));
        }
        return tests;
    }

    @Test
    void everyRunnerMatchesAVendoredFixture() {
        TreeSet<String> stale = new TreeSet<>(RUNNERS.keySet());
        stale.removeAll(SpecFixtures.byId(SpecFixtures.loadNdjson("invalid_cases.ndjson")).keySet());
        assertTrue(stale.isEmpty(), "runners without a vendored invalid_cases fixture: " + stale);
    }

    @TestFactory
    List<DynamicTest> wireInvalidFramesCloseTheSessionWithTheFixtureCode() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Map<String, Object> fixture : SpecFixtures.loadNdjson("wire_invalid.ndjson")) {
            if (!"frame_invalid".equals(string(fixture, "category"))) {
                continue;
            }
            String id = string(fixture, "id");
            tests.add(DynamicTest.dynamicTest(id, () -> {
                Settings local = Settings.defaults();
                if (has(fixture, "receiver_limits")) {
                    Map<String, Object> limits = map(fixture, "receiver_limits");
                    Settings.Builder builder = local.toBuilder();
                    if (has(limits, "max_frame_payload")) {
                        builder.maxFramePayload(longValue(limits, "max_frame_payload"));
                    }
                    if (has(limits, "max_control_payload_bytes")) {
                        builder.maxControlPayloadBytes(longValue(limits, "max_control_payload_bytes"));
                    }
                    if (has(limits, "max_extension_payload_bytes")) {
                        builder.maxExtensionPayloadBytes(longValue(limits, "max_extension_payload_bytes"));
                    }
                    local = builder.build();
                }
                try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().settings(local).build(), 0L, Settings.defaults())) {
                    peer.sendRaw(hex(string(fixture, "hex")));
                    Outcome outcome = Outcome.sessionError(peer.awaitSessionClose());
                    assertEquals(SpecFixtures.errorCode(string(fixture, "expect_error")), outcome.code,
                            id + ": the session must signal the fixture code with CLOSE");
                }
            }));
        }
        return tests;
    }

    /**
     * SPEC 9.1 first-frame table: a stream-scoped MAX_DATA, BLOCKED, STOP_SENDING or RESET as the first frame on a
     * stream ID that was never opened is a session PROTOCOL error, whichever class or owner the ID has and whether or
     * not it is the next expected peer ID.
     */
    @Test
    void firstNonOpeningStreamControlOnUnopenedStreamsClosesWithProtocol() throws Exception {
        long[] streamIds = {PEER_BIDI, PEER_BIDI + 4L, PEER_UNI, LOCAL_BIDI, LOCAL_UNI};
        FrameType[] types = {FrameType.MAX_DATA, FrameType.BLOCKED, FrameType.STOP_SENDING, FrameType.RESET};
        for (long streamId : streamIds) {
            for (FrameType type : types) {
                try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
                    peer.send(type, 0, streamId, firstFramePayload(type));
                    Outcome outcome = Outcome.sessionError(peer.awaitSessionClose());
                    assertEquals(ErrorCode.PROTOCOL, outcome.code,
                            type + " as the first frame on unopened stream " + streamId + " must close the session with PROTOCOL");
                }
            }
        }
    }

    /**
     * The same first-frame rule after other streams exist: an unopened ID between or after live and terminal
     * streams is still never opened by a non-opening frame.
     */
    @Test
    void firstNonOpeningStreamControlAfterEarlierStreamsStillClosesWithProtocol() throws Exception {
        for (FrameType type : new FrameType[]{FrameType.MAX_DATA, FrameType.BLOCKED, FrameType.STOP_SENDING, FrameType.RESET}) {
            try (SpecFixturePeer peer = SpecFixturePeer.open(ZmuxConfig.builder().build(), 0L, Settings.defaults())) {
                peer.send(FrameType.DATA, 0, PEER_BIDI, bytes("live"));
                peer.send(FrameType.ABORT, 0, PEER_BIDI + 4L, Varint62.encode(ErrorCode.CANCELLED.code()));
                peer.sync();
                assertEquals(SessionState.READY, peer.session().state(), "opening DATA and ABORT must not fail the session");

                peer.send(type, 0, PEER_BIDI + 8L, firstFramePayload(type));
                Outcome outcome = Outcome.sessionError(peer.awaitSessionClose());
                assertEquals(ErrorCode.PROTOCOL, outcome.code,
                        type + " as the first frame on the next unused peer stream must close the session with PROTOCOL");
            }
        }
    }

    @FunctionalInterface
    private interface Runner {
        Outcome run(Map<String, Object> fixture) throws Exception;
    }

    private static final class Outcome {
        private final Kind kind;
        private final ErrorCode code;
        private final long streamId;
        private final String action;
        private Outcome(Kind kind, ErrorCode code, long streamId, String action) {
            this.kind = kind;
            this.code = code;
            this.streamId = streamId;
            this.action = action;
        }

        static Outcome sessionError(ErrorCode code) {
            return new Outcome(Kind.SESSION_ERROR, code, 0L, null);
        }

        static Outcome streamError(long streamId, ErrorCode code) {
            return new Outcome(Kind.STREAM_ERROR, code, streamId, null);
        }

        static Outcome action(String action) {
            return new Outcome(Kind.ACTION, null, 0L, action);
        }

        @Override
        public String toString() {
            switch (kind) {
                case SESSION_ERROR:
                    return "session error " + code;
                case STREAM_ERROR:
                    return "ABORT(" + code + ") on stream " + streamId;
                default:
                    return "action " + action;
            }
        }

        enum Kind {
            SESSION_ERROR,
            STREAM_ERROR,
            ACTION
        }
    }
}
