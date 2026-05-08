package io.zmux.protocol;

import io.zmux.ErrorCode;
import io.zmux.Role;
import io.zmux.SchedulerHint;
import io.zmux.Settings;
import io.zmux.ZmuxConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Set;

final class PrefaceCodec {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[] MAGIC_BYTES = Protocol.MAGIC.getBytes(StandardCharsets.US_ASCII);
    private static final SecureRandom PADDING_RANDOM = new SecureRandom();

    private PrefaceCodec() {
    }

    static Preface readPreface(InputStream input) throws IOException {
        byte[] fixed = FrameCodec.readInputBytes(input, 6);
        if (fixed.length != 6) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "parse preface", "truncated preface");
        }
        return readPreface(fixed, new PrefaceReader() {
            @Override
            public long readVarint() throws IOException {
                return Varint62.read(input).value();
            }

            @Override
            public byte[] readSettingsBytes(int length) throws IOException {
                byte[] settingsBytes = FrameCodec.readInputBytes(input, length);
                if (settingsBytes.length != length) {
                    throw FrameCodec.error(ErrorCode.PROTOCOL, "parse preface", "truncated settings_tlv");
                }
                return settingsBytes;
            }
        });
    }

    static Preface readPreface(FrameCodec.Decoder input) throws IOException {
        byte[] fixed = input.readBytesExact(6);
        return readPreface(fixed, new PrefaceReader() {
            @Override
            public long readVarint() throws IOException {
                return Varint62.read(input).value();
            }

            @Override
            public byte[] readSettingsBytes(int length) throws IOException {
                return input.readBytesExact(length);
            }
        });
    }

    static void writePreface(OutputStream output, Preface preface) throws IOException {
        writePreface(output, preface, null);
    }

    static void writePreface(OutputStream output, Preface preface, ZmuxConfig config) throws IOException {
        validatePrefaceForMarshal(preface);
        output.write(MAGIC_BYTES);
        output.write(preface.prefaceVersion());
        output.write(preface.role().code());
        Varint62.write(output, preface.tieBreakerNonce());
        Varint62.write(output, preface.minProto());
        Varint62.write(output, preface.maxProto());
        Varint62.write(output, preface.capabilities());
        byte[] settingsBytes = config != null && config.prefacePadding()
                ? marshalSettingsWithPadding(preface.settings(), config)
                : marshalSettings(preface.settings());
        if (settingsBytes.length > Protocol.MAX_PREFACE_SETTINGS_BYTES) {
            throw FrameCodec.error(
                    ErrorCode.FRAME_SIZE,
                    "marshal preface",
                    "settings_tlv exceeds " + Protocol.MAX_PREFACE_SETTINGS_BYTES + " bytes"
            );
        }
        Varint62.write(output, settingsBytes.length);
        output.write(settingsBytes);
    }

    static Negotiated negotiate(Preface local, Preface peer) throws IOException {
        validatePrefaceForNegotiate(local, "local");
        validatePrefaceForNegotiate(peer, "peer");
        if (local.role() == Role.AUTO && local.tieBreakerNonce() == 0L) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "negotiate prefaces", "local auto role requires non-zero nonce");
        }
        if (peer.role() == Role.AUTO && peer.tieBreakerNonce() == 0L) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "negotiate prefaces", "peer auto role requires non-zero nonce");
        }
        long negotiatedProto = Math.min(local.maxProto(), peer.maxProto());
        if (negotiatedProto < Math.max(local.minProto(), peer.minProto())) {
            throw FrameCodec.error(ErrorCode.UNSUPPORTED_VERSION, "negotiate prefaces", "no compatible protocol version");
        }
        if (local.settings().maxFramePayload() < 16_384L
                || peer.settings().maxFramePayload() < 16_384L
                || local.settings().maxControlPayloadBytes() < 4_096L
                || peer.settings().maxControlPayloadBytes() < 4_096L
                || local.settings().maxExtensionPayloadBytes() < 4_096L
                || peer.settings().maxExtensionPayloadBytes() < 4_096L) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "negotiate prefaces", "receive limits below compatibility floor");
        }
        Role[] resolved = resolveRoles(local.role(), local.tieBreakerNonce(), peer.role(), peer.tieBreakerNonce());
        return new Negotiated(
                negotiatedProto,
                local.capabilities() & peer.capabilities(),
                resolved[0],
                resolved[1],
                peer.settings()
        );
    }

    static Role[] resolveRoles(Role localRole, long localNonce, Role peerRole, long peerNonce) throws IOException {
        if (localRole == Role.INITIATOR && peerRole == Role.RESPONDER) {
            return new Role[]{Role.INITIATOR, Role.RESPONDER};
        }
        if (localRole == Role.RESPONDER && peerRole == Role.INITIATOR) {
            return new Role[]{Role.RESPONDER, Role.INITIATOR};
        }
        if (localRole == Role.INITIATOR && peerRole == Role.AUTO) {
            return new Role[]{Role.INITIATOR, Role.RESPONDER};
        }
        if (localRole == Role.RESPONDER && peerRole == Role.AUTO) {
            return new Role[]{Role.RESPONDER, Role.INITIATOR};
        }
        if (localRole == Role.AUTO && peerRole == Role.INITIATOR) {
            return new Role[]{Role.RESPONDER, Role.INITIATOR};
        }
        if (localRole == Role.AUTO && peerRole == Role.RESPONDER) {
            return new Role[]{Role.INITIATOR, Role.RESPONDER};
        }
        if (localRole == Role.INITIATOR && peerRole == Role.INITIATOR) {
            throw FrameCodec.error(ErrorCode.ROLE_CONFLICT, "resolve roles", "both peers explicitly requested initiator");
        }
        if (localRole == Role.RESPONDER && peerRole == Role.RESPONDER) {
            throw FrameCodec.error(ErrorCode.ROLE_CONFLICT, "resolve roles", "both peers explicitly requested responder");
        }
        if (localRole == Role.AUTO && peerRole == Role.AUTO) {
            if (localNonce == peerNonce) {
                throw FrameCodec.error(ErrorCode.ROLE_CONFLICT, "resolve roles", "equal auto-role nonces");
            }
            return localNonce > peerNonce
                    ? new Role[]{Role.INITIATOR, Role.RESPONDER}
                    : new Role[]{Role.RESPONDER, Role.INITIATOR};
        }
        throw FrameCodec.error(ErrorCode.PROTOCOL, "resolve roles", "invalid role");
    }

    static byte[] marshalSettings(Settings settings) throws IOException {
        Settings defaults = Settings.defaults();
        ByteArrayOutputStream output = new ByteArrayOutputStream(settingsEncodedSize(settings, defaults));
        appendSetting(output, Protocol.SETTING_INITIAL_MAX_STREAM_DATA_BIDI_LOCALLY_OPENED, settings.initialMaxStreamDataBidiLocallyOpened(), defaults.initialMaxStreamDataBidiLocallyOpened());
        appendSetting(output, Protocol.SETTING_INITIAL_MAX_STREAM_DATA_BIDI_PEER_OPENED, settings.initialMaxStreamDataBidiPeerOpened(), defaults.initialMaxStreamDataBidiPeerOpened());
        appendSetting(output, Protocol.SETTING_INITIAL_MAX_STREAM_DATA_UNI, settings.initialMaxStreamDataUni(), defaults.initialMaxStreamDataUni());
        appendSetting(output, Protocol.SETTING_INITIAL_MAX_DATA, settings.initialMaxData(), defaults.initialMaxData());
        appendSetting(output, Protocol.SETTING_MAX_INCOMING_STREAMS_BIDI, settings.maxIncomingStreamsBidi(), defaults.maxIncomingStreamsBidi());
        appendSetting(output, Protocol.SETTING_MAX_INCOMING_STREAMS_UNI, settings.maxIncomingStreamsUni(), defaults.maxIncomingStreamsUni());
        appendSetting(output, Protocol.SETTING_MAX_FRAME_PAYLOAD, settings.maxFramePayload(), defaults.maxFramePayload());
        appendSetting(output, Protocol.SETTING_IDLE_TIMEOUT_MILLIS, settings.idleTimeoutMillis(), defaults.idleTimeoutMillis());
        appendSetting(output, Protocol.SETTING_KEEPALIVE_HINT_MILLIS, settings.keepaliveHintMillis(), defaults.keepaliveHintMillis());
        appendSetting(output, Protocol.SETTING_MAX_CONTROL_PAYLOAD_BYTES, settings.maxControlPayloadBytes(), defaults.maxControlPayloadBytes());
        appendSetting(output, Protocol.SETTING_MAX_EXTENSION_PAYLOAD_BYTES, settings.maxExtensionPayloadBytes(), defaults.maxExtensionPayloadBytes());
        appendSetting(output, Protocol.SETTING_SCHEDULER_HINTS, settings.schedulerHints().code(), defaults.schedulerHints().code());
        appendSetting(output, Protocol.SETTING_PING_PADDING_KEY, settings.pingPaddingKey(), defaults.pingPaddingKey());
        return output.toByteArray();
    }

    private static byte[] marshalSettingsWithPadding(Settings settings, ZmuxConfig config) throws IOException {
        byte[] settingsBytes = marshalSettings(settings);
        byte[] padding = randomPrefacePadding(settings, config);
        if (padding.length == 0) {
            return settingsBytes;
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream(
                settingsBytes.length + Varint62.length(Protocol.SETTING_PREFACE_PADDING)
                        + Varint62.length(padding.length) + padding.length
        );
        output.write(settingsBytes);
        FrameCodec.appendTlv(output, Protocol.SETTING_PREFACE_PADDING, padding);
        return output.toByteArray();
    }

    static Settings parseSettings(byte[] source) throws IOException {
        Settings.Builder builder = Settings.defaults().toBuilder();
        int seenKnown = 0;
        Set<Long> seenUnknown = null;
        int offset = 0;
        while (offset < source.length) {
            Varint62.Decoded typeDecoded = decodeSettingsVarint(source, offset, source.length);
            offset += typeDecoded.length();
            Varint62.Decoded lengthDecoded = decodeSettingsVarint(source, offset, source.length);
            offset += lengthDecoded.length();
            long type = typeDecoded.value();
            long length = lengthDecoded.value();
            if (length > source.length - offset) {
                throw FrameCodec.error(ErrorCode.PROTOCOL, "parse settings", "tlv value overruns containing payload");
            }
            int valueLength = FrameCodec.checkedLength(
                    length,
                    ErrorCode.PROTOCOL,
                    "parse settings",
                    "setting value exceeds Java implementation limit"
            );
            int seenBit = knownSettingSeenBit(type);
            if (seenBit != 0) {
                if ((seenKnown & seenBit) != 0) {
                    throw FrameCodec.error(ErrorCode.PROTOCOL, "parse settings", "duplicate setting id " + type);
                }
                seenKnown |= seenBit;
            } else {
                if (seenUnknown == null) {
                    seenUnknown = new HashSet<>(1);
                }
                if (!seenUnknown.add(type)) {
                    throw FrameCodec.error(ErrorCode.PROTOCOL, "parse settings", "duplicate setting id " + type);
                }
                offset += valueLength;
                continue;
            }
            if (type == Protocol.SETTING_PREFACE_PADDING) {
                offset += valueLength;
                continue;
            }
            Varint62.Decoded decoded = decodeSettingsVarint(source, offset, offset + valueLength);
            if (decoded.length() != valueLength) {
                throw FrameCodec.error(ErrorCode.PROTOCOL, "parse settings", "setting " + type + " has trailing bytes");
            }
            long value = decoded.value();
            offset += valueLength;
            if (type == Protocol.SETTING_INITIAL_MAX_STREAM_DATA_BIDI_LOCALLY_OPENED) {
                builder.initialMaxStreamDataBidiLocallyOpened(value);
            } else if (type == Protocol.SETTING_INITIAL_MAX_STREAM_DATA_BIDI_PEER_OPENED) {
                builder.initialMaxStreamDataBidiPeerOpened(value);
            } else if (type == Protocol.SETTING_INITIAL_MAX_STREAM_DATA_UNI) {
                builder.initialMaxStreamDataUni(value);
            } else if (type == Protocol.SETTING_INITIAL_MAX_DATA) {
                builder.initialMaxData(value);
            } else if (type == Protocol.SETTING_MAX_INCOMING_STREAMS_BIDI) {
                builder.maxIncomingStreamsBidi(value);
            } else if (type == Protocol.SETTING_MAX_INCOMING_STREAMS_UNI) {
                builder.maxIncomingStreamsUni(value);
            } else if (type == Protocol.SETTING_MAX_FRAME_PAYLOAD) {
                builder.maxFramePayload(value);
            } else if (type == Protocol.SETTING_IDLE_TIMEOUT_MILLIS) {
                builder.idleTimeoutMillis(value);
            } else if (type == Protocol.SETTING_KEEPALIVE_HINT_MILLIS) {
                builder.keepaliveHintMillis(value);
            } else if (type == Protocol.SETTING_MAX_CONTROL_PAYLOAD_BYTES) {
                builder.maxControlPayloadBytes(value);
            } else if (type == Protocol.SETTING_MAX_EXTENSION_PAYLOAD_BYTES) {
                builder.maxExtensionPayloadBytes(value);
            } else if (type == Protocol.SETTING_SCHEDULER_HINTS) {
                builder.schedulerHints(SchedulerHint.fromCode(value));
            } else if (type == Protocol.SETTING_PING_PADDING_KEY) {
                builder.pingPaddingKey(value);
            }
        }
        return builder.build();
    }

    private static Varint62.Decoded decodeSettingsVarint(byte[] source, int offset, int limit) throws IOException {
        try {
            return Varint62.decode(source, offset, limit);
        } catch (IOException error) {
            throw FrameCodec.error(
                    ErrorCode.PROTOCOL,
                    "parse settings",
                    error.getMessage() == null ? "invalid settings varint" : error.getMessage(),
                    error
            );
        }
    }

    private static int knownSettingSeenBit(long type) {
        if (type == Protocol.SETTING_INITIAL_MAX_STREAM_DATA_BIDI_LOCALLY_OPENED) {
            return 1;
        }
        if (type == Protocol.SETTING_INITIAL_MAX_STREAM_DATA_BIDI_PEER_OPENED) {
            return 1 << 1;
        }
        if (type == Protocol.SETTING_INITIAL_MAX_STREAM_DATA_UNI) {
            return 1 << 2;
        }
        if (type == Protocol.SETTING_INITIAL_MAX_DATA) {
            return 1 << 3;
        }
        if (type == Protocol.SETTING_MAX_INCOMING_STREAMS_BIDI) {
            return 1 << 4;
        }
        if (type == Protocol.SETTING_MAX_INCOMING_STREAMS_UNI) {
            return 1 << 5;
        }
        if (type == Protocol.SETTING_MAX_FRAME_PAYLOAD) {
            return 1 << 6;
        }
        if (type == Protocol.SETTING_IDLE_TIMEOUT_MILLIS) {
            return 1 << 7;
        }
        if (type == Protocol.SETTING_KEEPALIVE_HINT_MILLIS) {
            return 1 << 8;
        }
        if (type == Protocol.SETTING_MAX_CONTROL_PAYLOAD_BYTES) {
            return 1 << 9;
        }
        if (type == Protocol.SETTING_MAX_EXTENSION_PAYLOAD_BYTES) {
            return 1 << 10;
        }
        if (type == Protocol.SETTING_SCHEDULER_HINTS) {
            return 1 << 11;
        }
        if (type == Protocol.SETTING_PING_PADDING_KEY) {
            return 1 << 12;
        }
        if (type == Protocol.SETTING_PREFACE_PADDING) {
            return 1 << 13;
        }
        return 0;
    }

    private static Role parseRole(int code) throws IOException {
        try {
            return Role.fromCode(code);
        } catch (IllegalArgumentException error) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "parse preface", "invalid role: " + code, error);
        }
    }

    private static boolean hasMagic(byte[] fixed) {
        if (fixed.length < MAGIC_BYTES.length) {
            return false;
        }
        for (int i = 0; i < MAGIC_BYTES.length; ++i) {
            if (fixed[i] != MAGIC_BYTES[i]) {
                return false;
            }
        }
        return true;
    }

    private static Preface readPreface(byte[] fixed, PrefaceReader reader) throws IOException {
        ParsedPrefaceHeader header = parseFixedHeader(fixed);
        long tieBreakerNonce = reader.readVarint();
        long minProto = reader.readVarint();
        long maxProto = reader.readVarint();
        long capabilities = reader.readVarint();
        int settingsLength = checkedSettingsLength(reader.readVarint());
        byte[] settingsBytes = reader.readSettingsBytes(settingsLength);
        return new Preface(
                header.prefaceVersion,
                header.role,
                tieBreakerNonce,
                minProto,
                maxProto,
                capabilities,
                parseSettings(settingsBytes)
        );
    }

    private static ParsedPrefaceHeader parseFixedHeader(byte[] fixed) throws IOException {
        if (!hasMagic(fixed)) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "parse preface", "invalid magic");
        }
        if (fixed[4] != Protocol.PREFACE_VERSION) {
            throw FrameCodec.error(ErrorCode.UNSUPPORTED_VERSION, "parse preface", "unsupported preface version");
        }
        return new ParsedPrefaceHeader(fixed[4], parseRole(fixed[5] & 0xff));
    }

    private static int checkedSettingsLength(long settingsLength) throws IOException {
        if (settingsLength > Protocol.MAX_PREFACE_SETTINGS_BYTES) {
            throw FrameCodec.error(
                    ErrorCode.FRAME_SIZE,
                    "parse preface",
                    "settings_tlv exceeds " + Protocol.MAX_PREFACE_SETTINGS_BYTES + " bytes"
            );
        }
        return FrameCodec.checkedLength(
                settingsLength,
                ErrorCode.FRAME_SIZE,
                "parse preface",
                "settings_tlv exceeds Java implementation limit"
        );
    }

    private static void validatePrefaceForMarshal(Preface preface) throws IOException {
        if (preface == null) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "marshal preface", "preface is required");
        }
        if (preface.prefaceVersion() != Protocol.PREFACE_VERSION) {
            throw FrameCodec.error(ErrorCode.UNSUPPORTED_VERSION, "marshal preface", "unsupported preface version");
        }
        if (preface.role() == null) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "marshal preface", "invalid role");
        }
        if (preface.minProto() == 0L || preface.maxProto() == 0L) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "marshal preface", "protocol version bounds must be non-zero");
        }
        if (preface.role() == Role.AUTO && preface.tieBreakerNonce() == 0L) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "marshal preface", "role=auto requires non-zero tie-breaker nonce");
        }
        if (preface.settings() == null) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "marshal preface", "settings are required");
        }
    }

    private static void validatePrefaceForNegotiate(Preface preface, String name) throws IOException {
        if (preface == null) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "negotiate prefaces", name + " preface is required");
        }
        if (preface.role() == null) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "negotiate prefaces", "invalid role");
        }
        if (preface.settings() == null) {
            throw FrameCodec.error(ErrorCode.PROTOCOL, "negotiate prefaces", "settings are required");
        }
    }

    private static int settingsEncodedSize(Settings settings, Settings defaults) throws IOException {
        long total = 0L;
        total += settingEncodedSize(Protocol.SETTING_INITIAL_MAX_STREAM_DATA_BIDI_LOCALLY_OPENED, settings.initialMaxStreamDataBidiLocallyOpened(), defaults.initialMaxStreamDataBidiLocallyOpened());
        total += settingEncodedSize(Protocol.SETTING_INITIAL_MAX_STREAM_DATA_BIDI_PEER_OPENED, settings.initialMaxStreamDataBidiPeerOpened(), defaults.initialMaxStreamDataBidiPeerOpened());
        total += settingEncodedSize(Protocol.SETTING_INITIAL_MAX_STREAM_DATA_UNI, settings.initialMaxStreamDataUni(), defaults.initialMaxStreamDataUni());
        total += settingEncodedSize(Protocol.SETTING_INITIAL_MAX_DATA, settings.initialMaxData(), defaults.initialMaxData());
        total += settingEncodedSize(Protocol.SETTING_MAX_INCOMING_STREAMS_BIDI, settings.maxIncomingStreamsBidi(), defaults.maxIncomingStreamsBidi());
        total += settingEncodedSize(Protocol.SETTING_MAX_INCOMING_STREAMS_UNI, settings.maxIncomingStreamsUni(), defaults.maxIncomingStreamsUni());
        total += settingEncodedSize(Protocol.SETTING_MAX_FRAME_PAYLOAD, settings.maxFramePayload(), defaults.maxFramePayload());
        total += settingEncodedSize(Protocol.SETTING_IDLE_TIMEOUT_MILLIS, settings.idleTimeoutMillis(), defaults.idleTimeoutMillis());
        total += settingEncodedSize(Protocol.SETTING_KEEPALIVE_HINT_MILLIS, settings.keepaliveHintMillis(), defaults.keepaliveHintMillis());
        total += settingEncodedSize(Protocol.SETTING_MAX_CONTROL_PAYLOAD_BYTES, settings.maxControlPayloadBytes(), defaults.maxControlPayloadBytes());
        total += settingEncodedSize(Protocol.SETTING_MAX_EXTENSION_PAYLOAD_BYTES, settings.maxExtensionPayloadBytes(), defaults.maxExtensionPayloadBytes());
        total += settingEncodedSize(Protocol.SETTING_SCHEDULER_HINTS, settings.schedulerHints().code(), defaults.schedulerHints().code());
        total += settingEncodedSize(Protocol.SETTING_PING_PADDING_KEY, settings.pingPaddingKey(), defaults.pingPaddingKey());
        if (total > Integer.MAX_VALUE) {
            throw FrameCodec.error(ErrorCode.FRAME_SIZE, "marshal settings", "settings_tlv exceeds Java implementation limit");
        }
        return (int) total;
    }

    private static long settingEncodedSize(long type, long value, long defaultValue) throws IOException {
        if (value == defaultValue) {
            return 0L;
        }
        int valueLength = Varint62.length(value);
        return Varint62.length(type) + (long) Varint62.length(valueLength) + valueLength;
    }

    private static void appendSetting(ByteArrayOutputStream output, long type, long value, long defaultValue) throws IOException {
        if (value == defaultValue) {
            return;
        }
        Varint62.write(output, type);
        Varint62.write(output, Varint62.length(value));
        Varint62.write(output, value);
    }

    private static byte[] randomPrefacePadding(Settings settings, ZmuxConfig config) throws IOException {
        long maxPayload = maxPrefacePaddingPayloadBytes(settings, config.prefacePaddingMaxBytes());
        if (maxPayload <= 0L) {
            return EMPTY_BYTES;
        }
        long minPayload = config.prefacePaddingMinBytes();
        if (minPayload == 0L) {
            minPayload = ZmuxConfig.DEFAULT_PREFACE_PADDING_MIN_BYTES;
        }
        if (minPayload > maxPayload) {
            minPayload = maxPayload;
        }
        long paddingLength = minPayload;
        long span = maxPayload - minPayload + 1L;
        if (span > 1L) {
            paddingLength += randomLongBounded(span);
        }
        int length = FrameCodec.checkedLength(
                paddingLength,
                ErrorCode.FRAME_SIZE,
                "marshal preface",
                "settings padding exceeds Java implementation limit"
        );
        byte[] padding = new byte[length];
        PADDING_RANDOM.nextBytes(padding);
        return padding;
    }

    private static long maxPrefacePaddingPayloadBytes(Settings settings, long configuredMax) throws IOException {
        byte[] settingsBytes = marshalSettings(settings);
        if (settingsBytes.length >= Protocol.MAX_PREFACE_SETTINGS_BYTES) {
            return 0L;
        }
        long maxPayload = configuredMax == 0L ? ZmuxConfig.DEFAULT_PREFACE_PADDING_MAX_BYTES : configuredMax;
        long remaining = Protocol.MAX_PREFACE_SETTINGS_BYTES - (long) settingsBytes.length;
        if (maxPayload > remaining) {
            maxPayload = remaining;
        }
        int typeLength = Varint62.length(Protocol.SETTING_PREFACE_PADDING);
        long low = 0L;
        long high = maxPayload;
        while (low < high) {
            long candidate = low + (high - low + 1L) / 2L;
            int lengthLength = Varint62.length(candidate);
            long overhead = typeLength + (long) lengthLength;
            if (overhead <= remaining && candidate <= remaining - overhead) {
                low = candidate;
            } else {
                high = candidate - 1L;
            }
        }
        return low;
    }

    private static long randomLongBounded(long bound) {
        if (bound <= 1L) {
            return 0L;
        }
        long limit = Long.MAX_VALUE - Long.MAX_VALUE % bound;
        long value;
        do {
            value = PADDING_RANDOM.nextLong() & Long.MAX_VALUE;
        } while (value >= limit);
        return value % bound;
    }

    private interface PrefaceReader {
        long readVarint() throws IOException;

        byte[] readSettingsBytes(int length) throws IOException;
    }

    private static final class ParsedPrefaceHeader {
        private final byte prefaceVersion;
        private final Role role;

        private ParsedPrefaceHeader(byte prefaceVersion, Role role) {
            this.prefaceVersion = prefaceVersion;
            this.role = role;
        }
    }
}
