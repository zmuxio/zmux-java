package io.zmux;

public final class Protocol {
    public static final String MAGIC = "ZMUX";
    public static final byte PREFACE_VERSION = 1;
    public static final long PROTO_VERSION = 1;
    public static final int MAX_PREFACE_SETTINGS_BYTES = 4096;
    public static final long MAX_VARINT62 = (1L << 62) - 1;

    public static final int FRAME_FLAG_OPEN_METADATA = 0x20;
    public static final int FRAME_FLAG_FIN = 0x40;

    public static final long CAPABILITY_PRIORITY_HINTS = 1L;
    public static final long CAPABILITY_STREAM_GROUPS = 1L << 1;
    public static final long CAPABILITY_MULTILINK_BASIC_RETIRED = 1L << 2;
    public static final long CAPABILITY_MULTILINK_BASIC = CAPABILITY_MULTILINK_BASIC_RETIRED;
    public static final long CAPABILITY_PRIORITY_UPDATE = 1L << 3;
    public static final long CAPABILITY_OPEN_METADATA = 1L << 4;

    public static final long SETTING_INITIAL_MAX_STREAM_DATA_BIDI_LOCALLY_OPENED = 1;
    public static final long SETTING_INITIAL_MAX_STREAM_DATA_BIDI_PEER_OPENED = 2;
    public static final long SETTING_INITIAL_MAX_STREAM_DATA_UNI = 3;
    public static final long SETTING_INITIAL_MAX_DATA = 4;
    public static final long SETTING_MAX_INCOMING_STREAMS_BIDI = 5;
    public static final long SETTING_MAX_INCOMING_STREAMS_UNI = 6;
    public static final long SETTING_MAX_FRAME_PAYLOAD = 7;
    public static final long SETTING_IDLE_TIMEOUT_MILLIS = 8;
    public static final long SETTING_KEEPALIVE_HINT_MILLIS = 9;
    public static final long SETTING_MAX_CONTROL_PAYLOAD_BYTES = 10;
    public static final long SETTING_MAX_EXTENSION_PAYLOAD_BYTES = 11;
    public static final long SETTING_SCHEDULER_HINTS = 12;

    public static final long METADATA_STREAM_PRIORITY = 1;
    public static final long METADATA_STREAM_GROUP = 2;
    public static final long METADATA_OPEN_INFO = 3;

    public static final long DIAG_DEBUG_TEXT = 1;
    public static final long DIAG_RETRY_AFTER_MILLIS = 2;
    public static final long DIAG_OFFENDING_STREAM_ID = 3;
    public static final long DIAG_OFFENDING_FRAME_TYPE = 4;

    public static final long EXT_PRIORITY_UPDATE = 1;
    public static final long EXT_ML_READY_RETIRED = 2;
    public static final long EXT_ML_ATTACH_RETIRED = 3;
    public static final long EXT_ML_ATTACH_ACK_RETIRED = 4;
    public static final long EXT_ML_DRAIN_REQ_RETIRED = 5;
    public static final long EXT_ML_DRAIN_ACK_RETIRED = 6;

    private Protocol() {
    }

    public static boolean hasCapability(long capabilities, long bit) {
        return (capabilities & bit) != 0;
    }

    public static boolean supportsOpenMetadata(long capabilities) {
        return hasCapability(capabilities, CAPABILITY_OPEN_METADATA);
    }

    public static boolean supportsPriorityUpdate(long capabilities) {
        return hasCapability(capabilities, CAPABILITY_PRIORITY_UPDATE);
    }

    public static boolean canCarryOpenInfo(long capabilities) {
        return supportsOpenMetadata(capabilities);
    }

    public static boolean canCarryPriorityOnOpen(long capabilities) {
        return supportsOpenMetadata(capabilities) && hasCapability(capabilities, CAPABILITY_PRIORITY_HINTS);
    }

    public static boolean canCarryGroupOnOpen(long capabilities) {
        return supportsOpenMetadata(capabilities) && hasCapability(capabilities, CAPABILITY_STREAM_GROUPS);
    }

    public static boolean canCarryPriorityInUpdate(long capabilities) {
        return supportsPriorityUpdate(capabilities) && hasCapability(capabilities, CAPABILITY_PRIORITY_HINTS);
    }

    public static boolean canCarryGroupInUpdate(long capabilities) {
        return supportsPriorityUpdate(capabilities) && hasCapability(capabilities, CAPABILITY_STREAM_GROUPS);
    }

    public static boolean hasPeerVisiblePrioritySemantics(long capabilities) {
        return canCarryPriorityOnOpen(capabilities) || canCarryPriorityInUpdate(capabilities);
    }

    public static boolean hasPeerVisibleGroupSemantics(long capabilities) {
        return canCarryGroupOnOpen(capabilities) || canCarryGroupInUpdate(capabilities);
    }
}
