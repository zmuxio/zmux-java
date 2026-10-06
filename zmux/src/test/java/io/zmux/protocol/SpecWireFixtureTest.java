package io.zmux.protocol;

import io.zmux.ErrorCode;
import io.zmux.Settings;
import io.zmux.SpecFixtures;
import io.zmux.ZmuxException;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static io.zmux.SpecFixtures.has;
import static io.zmux.SpecFixtures.hex;
import static io.zmux.SpecFixtures.longValue;
import static io.zmux.SpecFixtures.map;
import static io.zmux.SpecFixtures.mapList;
import static io.zmux.SpecFixtures.string;
import static io.zmux.SpecFixtures.stringList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the vendored zmux-spec wire fixtures ({@code wire_valid.ndjson}, {@code wire_invalid.ndjson}) through every
 * Java frame/preface reader and checks the bundle's own inventory files.
 *
 * <p>Session-level checks of the same invalid frames (the CLOSE code a raw peer receives) live in
 * {@code io.zmux.SpecInvalidCaseFixtureTest}.
 */
final class SpecWireFixtureTest {
    private static final String[] BUNDLE_FILES = {
            "index.json",
            "case_sets.json",
            "wire_valid.ndjson",
            "wire_invalid.ndjson",
            "state_cases.ndjson",
            "invalid_cases.ndjson",
    };

    @TestFactory
    List<DynamicTest> wireValidFixturesDecodeAndRoundTrip() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Map<String, Object> fixture : SpecFixtures.loadNdjson("wire_valid.ndjson")) {
            String id = string(fixture, "id");
            tests.add(DynamicTest.dynamicTest(id, () -> {
                byte[] raw = hex(string(fixture, "hex"));
                Map<String, Object> expect = map(fixture, "expect");
                String category = string(fixture, "category");
                if ("preface_valid".equals(category)) {
                    assertPrefaceFixture(id, raw, expect);
                } else if ("frame_valid".equals(category)) {
                    assertFrameFixture(id, raw, expect);
                } else {
                    fail(id + ": unsupported wire_valid category " + category);
                }
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> wireInvalidFixturesFailWithTheFixtureCode() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Map<String, Object> fixture : SpecFixtures.loadNdjson("wire_invalid.ndjson")) {
            String id = string(fixture, "id");
            tests.add(DynamicTest.dynamicTest(id, () -> {
                byte[] raw = hex(string(fixture, "hex"));
                ErrorCode expected = SpecFixtures.errorCode(string(fixture, "expect_error"));
                String category = string(fixture, "category");
                if ("bytes_invalid".equals(category)) {
                    assertCode(id + " Varint62.decode", expected, () -> Varint62.decode(raw, 0));
                    assertCode(id + " ZmuxCodec.parseVarint", expected, () -> ZmuxCodec.parseVarint(raw));
                    assertCode(id + " Varint62.read", expected, () -> Varint62.read(new ByteArrayInputStream(raw)));
                    assertCode(id + " Varint62.read(Decoder)", expected,
                            () -> Varint62.read(FrameCodec.decoder(new ByteArrayInputStream(raw))));
                } else if ("frame_invalid".equals(category)) {
                    Limits limits = receiverLimits(fixture);
                    assertEveryFrameReaderFails(id, raw, limits, expected);
                    assertCode(id + " FrameEnvelopeCodec.readInboundSessionFrame", expected,
                            () -> FrameEnvelopeCodec.readInboundSessionFrame(FrameCodec.decoder(new ByteArrayInputStream(raw)), limits, null));
                } else {
                    fail(id + ": unsupported wire_invalid category " + category);
                }
            }));
        }
        return tests;
    }

    @Test
    void fixtureIndexCountsMatchVendoredBundle() {
        Map<String, Object> index = SpecFixtures.loadJson("index.json");
        assertEquals("zmux-fixture-bundle-v1", string(index, "schema"), "unexpected fixture bundle schema");
        Set<String> indexed = new HashSet<>();
        for (Map<String, Object> entry : mapList(index, "files")) {
            String path = string(entry, "path");
            assertTrue(path.startsWith("fixtures/"), "index path should name the bundle directory: " + path);
            String name = path.substring("fixtures/".length());
            indexed.add(name);
            String kind = string(entry, "kind");
            assertEquals(kind + ".ndjson", name, "index kind should match its file name");
            assertEquals(
                    longValue(entry, "count"),
                    SpecFixtures.loadNdjson(name).size(),
                    "vendored " + name + " record count must match index.json"
            );
        }
        assertEquals(
                new HashSet<>(Arrays.asList("wire_valid.ndjson", "wire_invalid.ndjson", "state_cases.ndjson", "invalid_cases.ndjson")),
                indexed,
                "index.json should list exactly the four NDJSON bundles"
        );
    }

    @Test
    void fixtureIdsAreUniqueAndCaseSetsResolve() {
        Set<String> wireValid = ids("wire_valid.ndjson");
        Set<String> wireInvalid = ids("wire_invalid.ndjson");
        Set<String> all = new HashSet<>();
        for (String name : new String[]{"wire_valid.ndjson", "wire_invalid.ndjson", "state_cases.ndjson", "invalid_cases.ndjson"}) {
            for (String id : ids(name)) {
                assertTrue(all.add(id), "fixture id " + id + " is not globally unique");
            }
        }

        Map<String, Object> sets = map(SpecFixtures.loadJson("case_sets.json"), "sets");
        for (Map.Entry<String, Object> set : sets.entrySet()) {
            for (String id : stringList(sets, set.getKey())) {
                assertTrue(all.contains(id), "case set " + set.getKey() + " references unknown fixture id " + id);
            }
        }
        assertEquals(wireValid, new TreeSet<>(stringList(sets, "codec_valid")), "codec_valid should equal wire_valid");
        assertEquals(wireInvalid, new TreeSet<>(stringList(sets, "codec_invalid")), "codec_invalid should equal wire_invalid");
    }

    /**
     * Opt-in staleness check for workspaces that also check out zmux-spec: with {@code ZMUX_SPEC_ROOT} set, the
     * vendored bundle must be a byte-for-byte copy of {@code $ZMUX_SPEC_ROOT/fixtures}.
     */
    @Test
    void vendoredBundleMatchesSpecCheckoutWhenConfigured() throws IOException {
        String specRoot = System.getenv("ZMUX_SPEC_ROOT");
        assumeTrue(specRoot != null && !specRoot.trim().isEmpty(), "set ZMUX_SPEC_ROOT to compare with a zmux-spec checkout");
        Path fixtures = Paths.get(specRoot.trim()).resolve("fixtures");
        assumeTrue(Files.isDirectory(fixtures), "zmux-spec fixtures directory not found: " + fixtures);
        for (String name : BUNDLE_FILES) {
            String upstream = new String(Files.readAllBytes(fixtures.resolve(name)), StandardCharsets.UTF_8);
            assertEquals(upstream, SpecFixtures.resourceText(name), "vendored " + name + " is stale; re-copy it from " + fixtures);
        }
    }

    private static void assertPrefaceFixture(String id, byte[] raw, Map<String, Object> expect) throws Exception {
        Preface preface = ZmuxCodec.parsePreface(raw);
        assertEquals(preface, FrameCodec.readPreface(new ByteArrayInputStream(raw)), id + ": InputStream preface reader disagrees");
        assertEquals(preface, FrameCodec.decoder(new ByteArrayInputStream(raw)).readPreface(), id + ": Decoder preface reader disagrees");

        assertEquals(longValue(expect, "preface_ver"), preface.prefaceVersion(), id + ": preface_ver");
        assertEquals(string(expect, "role"), preface.role().name().toLowerCase(Locale.ROOT), id + ": role");
        assertEquals(longValue(expect, "tie_breaker_nonce"), preface.tieBreakerNonce(), id + ": tie_breaker_nonce");
        assertEquals(longValue(expect, "min_proto"), preface.minProto(), id + ": min_proto");
        assertEquals(longValue(expect, "max_proto"), preface.maxProto(), id + ": max_proto");
        assertEquals(longValue(expect, "capabilities"), preface.capabilities(), id + ": capabilities");

        // settings_len is a wire field, so it is read from the raw bytes; a padded preface counts its padding TLV.
        int offset = 6;
        for (int i = 0; i < 4; i++) {
            offset += Varint62.decode(raw, offset).length();
        }
        Varint62.Decoded settingsLen = Varint62.decode(raw, offset);
        offset += settingsLen.length();
        assertEquals(longValue(expect, "settings_len"), settingsLen.value(), id + ": settings_len");
        assertEquals(raw.length - offset, settingsLen.value(), id + ": settings_len should cover the rest of the preface");
        byte[] settingsTlv = Arrays.copyOfRange(raw, offset, raw.length);

        Settings expectedSettings = Settings.defaults();
        if (has(expect, "settings")) {
            expectedSettings = expectedSettings(id, map(expect, "settings"));
        } else {
            assertEquals(0L, settingsLen.value(), id + ": a fixture without settings expectations should carry none");
        }
        assertEquals(expectedSettings, preface.settings(), id + ": settings");

        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        FrameCodec.writePreface(encoded, preface);
        if (carriesPrefacePadding(settingsTlv)) {
            // Padding is ignored on receipt, so re-encoding the parsed preface reproduces it without the padding TLV.
            assertEquals(preface, ZmuxCodec.parsePreface(encoded.toByteArray()), id + ": padded preface should re-encode to an equal preface");
            assertTrue(encoded.size() < raw.length, id + ": re-encoded preface should drop the padding TLV");
        } else {
            assertEquals(hex(raw), hex(encoded.toByteArray()), id + ": preface round trip");
        }
    }

    private static boolean carriesPrefacePadding(byte[] settingsTlv) throws IOException {
        for (Tlv tlv : ZmuxCodec.parseTlvs(settingsTlv)) {
            if (tlv.type() == Protocol.SETTING_PREFACE_PADDING) {
                return true;
            }
        }
        return false;
    }

    private static Settings expectedSettings(String id, Map<String, Object> settings) {
        Settings.Builder builder = Settings.defaults().toBuilder();
        for (String name : settings.keySet()) {
            long value = longValue(settings, name);
            switch (name) {
                case "initial_max_stream_data_bidi_locally_opened":
                    builder.initialMaxStreamDataBidiLocallyOpened(value);
                    break;
                case "initial_max_stream_data_bidi_peer_opened":
                    builder.initialMaxStreamDataBidiPeerOpened(value);
                    break;
                case "initial_max_stream_data_uni":
                    builder.initialMaxStreamDataUni(value);
                    break;
                case "initial_max_data":
                    builder.initialMaxData(value);
                    break;
                case "max_incoming_streams_bidi":
                    builder.maxIncomingStreamsBidi(value);
                    break;
                case "max_incoming_streams_uni":
                    builder.maxIncomingStreamsUni(value);
                    break;
                case "max_frame_payload":
                    builder.maxFramePayload(value);
                    break;
                case "max_control_payload_bytes":
                    builder.maxControlPayloadBytes(value);
                    break;
                case "max_extension_payload_bytes":
                    builder.maxExtensionPayloadBytes(value);
                    break;
                case "ping_padding_key":
                    builder.pingPaddingKey(value);
                    break;
                default:
                    fail(id + ": unsupported settings expectation " + name);
            }
        }
        return builder.build();
    }

    private static void assertFrameFixture(String id, byte[] raw, Map<String, Object> expect) throws Exception {
        Limits limits = Settings.defaults().limits();
        ParsedFrame parsed = ZmuxCodec.parseFrame(raw, limits);
        assertEquals(raw.length, parsed.bytesRead(), id + ": parseFrame should consume the whole fixture");
        FrameCodec.Frame frame = FrameCodec.readFrame(new ByteArrayInputStream(raw), limits);
        assertSameFrame(id + " parseFrame", frame, parsed.frame().type(), parsed.frame().flags(), parsed.frame().streamId(), parsed.frame().payload());
        FrameEnvelopeCodec.InboundFrame inbound = FrameEnvelopeCodec.readInboundFrame(
                FrameCodec.decoder(new ByteArrayInputStream(raw)), limits, null);
        assertSameFrame(id + " readInboundFrame", frame, inbound.type(), inbound.flags(), inbound.streamId(), inbound.payload());
        FrameEnvelopeCodec.InboundFrame session = FrameEnvelopeCodec.readInboundSessionFrame(
                FrameCodec.decoder(new ByteArrayInputStream(raw)), limits, null);
        assertSameFrame(id + " readInboundSessionFrame", frame, session.type(), session.flags(), session.streamId(), session.payload());

        if (has(expect, "frame_length")) {
            assertEquals(longValue(expect, "frame_length"), Varint62.decode(raw, 0).value(), id + ": frame_length");
        }
        assertEquals(string(expect, "frame_type"), frame.type().name(), id + ": frame_type");
        assertEquals(longValue(expect, "stream_id"), frame.streamId(), id + ": stream_id");
        if (has(expect, "flags")) {
            assertEquals(stringList(expect, "flags"), flagNames(frame.flags()), id + ": flags");
        } else {
            assertEquals(0, frame.flags(), id + ": a fixture without flags expectations should carry none");
        }
        if (has(expect, "payload_hex")) {
            assertEquals(string(expect, "payload_hex").toLowerCase(Locale.ROOT), hex(frame.payload()), id + ": payload_hex");
        }
        if (has(expect, "decoded")) {
            assertDecodedFrame(id, frame, map(expect, "decoded"));
        }

        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        FrameCodec.writeFrame(encoded, frame, limits);
        assertEquals(hex(raw), hex(encoded.toByteArray()), id + ": frame round trip");
        ByteArrayOutputStream publicEncoded = new ByteArrayOutputStream();
        ZmuxCodec.writeFrame(publicEncoded, parsed.frame(), limits);
        assertEquals(hex(raw), hex(publicEncoded.toByteArray()), id + ": public codec round trip");
    }

    private static void assertSameFrame(String label, FrameCodec.Frame expected, FrameType type, int flags, long streamId, byte[] payload) {
        assertEquals(expected.type(), type, label + ": frame type");
        assertEquals(expected.flags(), flags, label + ": flags");
        assertEquals(expected.streamId(), streamId, label + ": stream_id");
        assertArrayEquals(expected.payload(), payload, label + ": payload");
    }

    private static List<String> flagNames(int flags) {
        List<String> names = new ArrayList<>();
        if ((flags & Protocol.FRAME_FLAG_OPEN_METADATA) != 0) {
            names.add("OPEN_METADATA");
        }
        if ((flags & Protocol.FRAME_FLAG_FIN) != 0) {
            names.add("FIN");
        }
        return names;
    }

    private static void assertDecodedFrame(String id, FrameCodec.Frame frame, Map<String, Object> decoded) throws Exception {
        Set<String> handled = new LinkedHashSet<>();
        byte[] payload = frame.payload();
        if (has(decoded, "max_offset")) {
            handled.add("max_offset");
            assertSingleVarintPayload(id, "max_offset", longValue(decoded, "max_offset"), payload);
        }
        if (has(decoded, "blocked_at")) {
            handled.add("blocked_at");
            assertSingleVarintPayload(id, "blocked_at", longValue(decoded, "blocked_at"), payload);
        }
        if (has(decoded, "error_code")) {
            handled.add("error_code");
            handled.add("debug_text");
            handled.add("diag_block_dropped");
            FrameCodec.ErrorPayload error = FrameCodec.parseErrorPayload(payload);
            assertEquals(longValue(decoded, "error_code"), error.code(), id + ": error_code");
            if (has(decoded, "debug_text")) {
                assertEquals(string(decoded, "debug_text"), error.reason(), id + ": debug_text");
            } else {
                assertEquals("", error.reason(), id + ": no debug_text expected");
            }
            if (has(decoded, "diag_block_dropped") && SpecFixtures.bool(decoded, "diag_block_dropped")) {
                assertTrue(payload.length > Varint62.decode(payload, 0).length(), id + ": dropped DIAG block should still be present on the wire");
            }
        }
        if (has(decoded, "goaway")) {
            handled.add("goaway");
            Map<String, Object> goaway = map(decoded, "goaway");
            FrameCodec.GoAwayPayload parsed = FrameCodec.parseGoAwayPayload(payload);
            assertEquals(longValue(goaway, "last_accepted_bidi_stream_id"), parsed.lastAcceptedBidi(), id + ": last_accepted_bidi_stream_id");
            assertEquals(longValue(goaway, "last_accepted_uni_stream_id"), parsed.lastAcceptedUni(), id + ": last_accepted_uni_stream_id");
            assertEquals(longValue(goaway, "error_code"), parsed.code(), id + ": goaway error_code");
        }
        if (frame.type() == FrameType.DATA) {
            handled.add("stream_metadata_tlvs");
            handled.add("application_payload_hex");
            handled.add("open_metadata_block_dropped");
            assertDataPayload(id, frame, decoded);
        }
        if (frame.type() == FrameType.EXT) {
            handled.add("ext_type");
            handled.add("ext_type_value");
            handled.add("stream_metadata_tlvs");
            assertExtPayload(id, payload, decoded);
        }
        if (has(decoded, "ping_padding_tag")) {
            handled.add("ping_padding_tag");
            assertPingPaddingTag(id, payload, map(decoded, "ping_padding_tag"));
        }
        for (String key : decoded.keySet()) {
            assertTrue(handled.contains(key), id + ": unsupported decoded expectation " + key);
        }
    }

    private static void assertSingleVarintPayload(String id, String field, long expected, byte[] payload) throws IOException {
        Varint62.Decoded value = Varint62.decode(payload, 0);
        assertEquals(expected, value.value(), id + ": " + field);
        assertEquals(payload.length, value.length(), id + ": " + field + " must be the only payload field");
    }

    private static void assertDataPayload(String id, FrameCodec.Frame frame, Map<String, Object> decoded) throws Exception {
        FrameCodec.DataPayload data = FrameCodec.parseDataPayload(frame.payload(), frame.flags());
        if (has(decoded, "application_payload_hex")) {
            assertEquals(string(decoded, "application_payload_hex").toLowerCase(Locale.ROOT), hex(data.appData()), id + ": application_payload_hex");
        }
        boolean dropped = has(decoded, "open_metadata_block_dropped") && SpecFixtures.bool(decoded, "open_metadata_block_dropped");
        if ((frame.flags() & Protocol.FRAME_FLAG_OPEN_METADATA) != 0) {
            assertTrue(data.hasMetadata(), id + ": OPEN_METADATA frame should report a metadata block");
            assertEquals(!dropped, data.metadataValid(), id + ": open_metadata_block_dropped");
        }
        if (!has(decoded, "stream_metadata_tlvs")) {
            return;
        }
        ExpectedMetadata expected = expectedMetadata(id, mapList(decoded, "stream_metadata_tlvs"));
        long priority = data.metadataValid() ? data.priority() : 0L;
        Long group = data.metadataValid() ? data.group() : null;
        byte[] openInfo = data.metadataValid() ? data.openInfo() : new byte[0];
        assertEquals(expected.priority == null ? 0L : expected.priority, priority, id + ": stream_priority");
        assertEquals(expected.group, group, id + ": stream_group");
        assertEquals(expected.openInfoHex == null ? "" : expected.openInfoHex, hex(openInfo), id + ": open_info");
        if (dropped) {
            // The dropped block must not leak any of its values into the parsed view either.
            assertEquals(0L, data.priority(), id + ": dropped block priority");
            assertNull(data.group(), id + ": dropped block group");
            assertEquals(0, data.openInfo().length, id + ": dropped block open_info");
        }
    }

    private static void assertExtPayload(String id, byte[] payload, Map<String, Object> decoded) throws Exception {
        Varint62.Decoded extType = Varint62.decode(payload, 0);
        if (has(decoded, "ext_type_value")) {
            assertEquals(longValue(decoded, "ext_type_value"), extType.value(), id + ": ext_type_value");
        }
        if (has(decoded, "ext_type")) {
            assertEquals("PRIORITY_UPDATE", string(decoded, "ext_type"), id + ": unsupported ext_type expectation");
            assertEquals(Protocol.EXT_PRIORITY_UPDATE, extType.value(), id + ": ext_type");
        }
        if (!has(decoded, "stream_metadata_tlvs")) {
            return;
        }
        ExpectedMetadata expected = expectedMetadata(id, mapList(decoded, "stream_metadata_tlvs"));
        FrameCodec.ParsedPriorityUpdate update = FrameCodec.parsePriorityUpdatePayload(payload);
        assertTrue(update.valid(), id + ": PRIORITY_UPDATE should be valid");
        assertNull(expected.openInfoHex, id + ": PRIORITY_UPDATE cannot carry open_info");
        assertEquals(expected.priority != null, update.hasPriority(), id + ": stream_priority presence");
        if (expected.priority != null) {
            assertEquals((long) expected.priority, update.priority(), id + ": stream_priority");
        }
        assertEquals(expected.group != null, update.hasGroup(), id + ": stream_group presence");
        assertEquals(expected.group, update.group(), id + ": stream_group");
    }

    private static void assertPingPaddingTag(String id, byte[] payload, Map<String, Object> tag) throws Exception {
        long key = longValue(tag, "ping_padding_key");
        byte[] token = hex(string(tag, "token_hex"));
        byte[] expectedTag = hex(string(tag, "tag_hex"));
        assertEquals(string(tag, "token_hex"), hex(Arrays.copyOfRange(payload, 0, token.length)), id + ": PING token");
        assertEquals(string(tag, "tag_hex"), hex(Arrays.copyOfRange(payload, token.length, token.length + expectedTag.length)), id + ": PING tag bytes");

        // The tag function is private to the runtime; call the production code rather than a test copy of it.
        Class<?> runtime = Class.forName("io.zmux.runtime.SessionRuntime");
        Method tagFunction = runtime.getDeclaredMethod("pingPaddingTag", long.class, long.class);
        tagFunction.setAccessible(true);
        long computed = (Long) tagFunction.invoke(null, key, bigEndianLong(token));
        assertEquals(bigEndianLong(expectedTag), computed, id + ": ping_padding_tag(key, token)");
        Method recognizer = runtime.getDeclaredMethod("hasPingPaddingTag", byte[].class, long.class);
        recognizer.setAccessible(true);
        assertTrue((Boolean) recognizer.invoke(null, payload, key), id + ": receiver should recognize the padded PING");
        assertFalse((Boolean) recognizer.invoke(null, payload, key ^ 1L), id + ": a different key must not match the tag");
    }

    private static long bigEndianLong(byte[] bytes) {
        assertEquals(Long.BYTES, bytes.length, "ping padding fields are 8 bytes");
        long value = 0L;
        for (byte b : bytes) {
            value = value << 8 | (b & 0xffL);
        }
        return value;
    }

    private static ExpectedMetadata expectedMetadata(String id, List<Map<String, Object>> tlvs) {
        ExpectedMetadata expected = new ExpectedMetadata();
        for (Map<String, Object> tlv : tlvs) {
            String type = string(tlv, "type");
            switch (type) {
                case "stream_priority":
                    expected.priority = longValue(tlv, "value");
                    break;
                case "stream_group":
                    expected.group = longValue(tlv, "value");
                    break;
                case "open_info":
                    expected.openInfoHex = string(tlv, "value_hex").toLowerCase(Locale.ROOT);
                    break;
                default:
                    fail(id + ": unsupported stream metadata TLV " + type);
            }
        }
        return expected;
    }

    private static Limits receiverLimits(Map<String, Object> fixture) {
        Settings defaults = Settings.defaults();
        if (!has(fixture, "receiver_limits")) {
            return defaults.limits();
        }
        Map<String, Object> limits = map(fixture, "receiver_limits");
        return new Limits(
                has(limits, "max_frame_payload") ? longValue(limits, "max_frame_payload") : defaults.maxFramePayload(),
                has(limits, "max_control_payload_bytes") ? longValue(limits, "max_control_payload_bytes") : defaults.maxControlPayloadBytes(),
                has(limits, "max_extension_payload_bytes") ? longValue(limits, "max_extension_payload_bytes") : defaults.maxExtensionPayloadBytes()
        );
    }

    static void assertEveryFrameReaderFails(String id, byte[] raw, Limits limits, ErrorCode expected) {
        assertCode(id + " ZmuxCodec.parseFrame", expected, () -> ZmuxCodec.parseFrame(raw, limits));
        assertCode(id + " FrameCodec.readFrame", expected, () -> FrameCodec.readFrame(new ByteArrayInputStream(raw), limits));
        assertCode(id + " Decoder.readFrame", expected, () -> FrameCodec.decoder(new ByteArrayInputStream(raw)).readFrame(limits));
        assertCode(id + " FrameEnvelopeCodec.readInboundFrame", expected, () -> FrameEnvelopeCodec.readInboundFrame(
                FrameCodec.decoder(new ByteArrayInputStream(raw)), limits, null));
    }

    static void assertCode(String label, ErrorCode expected, org.junit.jupiter.api.function.Executable executable) {
        ZmuxException error = assertThrows(ZmuxException.class, executable, label + " should fail with " + expected);
        assertEquals(expected, ErrorCode.fromCode(error.code()), label + ": " + error.getMessage());
    }

    private static Set<String> ids(String name) {
        Set<String> ids = new TreeSet<>();
        for (Map<String, Object> record : SpecFixtures.loadNdjson(name)) {
            assertTrue(ids.add(string(record, "id")), name + " repeats id " + string(record, "id"));
        }
        return ids;
    }

    private static final class ExpectedMetadata {
        private Long priority;
        private Long group;
        private String openInfoHex;
    }
}
