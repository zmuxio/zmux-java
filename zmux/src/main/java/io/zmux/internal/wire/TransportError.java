package io.zmux.internal.wire;

import java.util.Map;

/** Transport error codes (SPEC.md Section 10.2) and ZMUX_REJECTED. */
public final class TransportError {
    public static final long NO_ERROR = 0x00;
    public static final long INTERNAL_ERROR = 0x01;
    public static final long CONNECTION_REFUSED = 0x02;
    public static final long FLOW_CONTROL_ERROR = 0x03;
    public static final long STREAM_LIMIT_ERROR = 0x04;
    public static final long STREAM_STATE_ERROR = 0x05;
    public static final long FINAL_SIZE_ERROR = 0x06;
    public static final long FRAME_ENCODING_ERROR = 0x07;
    public static final long TRANSPORT_PARAMETER_ERROR = 0x08;
    public static final long PROTOCOL_VIOLATION = 0x0a;
    public static final long NO_VIABLE_PATH = 0x10;
    public static final long EXCESSIVE_LOAD = 0x25ff75efL;
    public static final long ROLE_CONFLICT = 0x277db2c3L;
    public static final long RESUME_TIMEOUT = 0x0eb87167L;
    public static final long RESUME_FAILED = 0x0f05f74eL;
    public static final long PIPE_FAILED = 0x296a5bf5L;
    public static final long PIPE_RESOURCE_LIMIT = 0x0b4fa113L;
    public static final long PIPE_AUTH_FAILED = 0x09613938L;
    public static final long IDLE_TIMEOUT = 0x2736d55cL;
    /** ZMUX_REJECTED, an application code (EXTENSIONS.md Section 7). */
    public static final long REJECTED = 0x5a4d0002L;

    private static final Map<Long, String> NAMES = Map.ofEntries(
            Map.entry(NO_ERROR, "NO_ERROR"), Map.entry(INTERNAL_ERROR, "INTERNAL_ERROR"),
            Map.entry(CONNECTION_REFUSED, "CONNECTION_REFUSED"), Map.entry(FLOW_CONTROL_ERROR, "FLOW_CONTROL_ERROR"),
            Map.entry(STREAM_LIMIT_ERROR, "STREAM_LIMIT_ERROR"), Map.entry(STREAM_STATE_ERROR, "STREAM_STATE_ERROR"),
            Map.entry(FINAL_SIZE_ERROR, "FINAL_SIZE_ERROR"), Map.entry(FRAME_ENCODING_ERROR, "FRAME_ENCODING_ERROR"),
            Map.entry(TRANSPORT_PARAMETER_ERROR, "TRANSPORT_PARAMETER_ERROR"),
            Map.entry(PROTOCOL_VIOLATION, "PROTOCOL_VIOLATION"), Map.entry(NO_VIABLE_PATH, "NO_VIABLE_PATH"),
            Map.entry(EXCESSIVE_LOAD, "ZMUX_EXCESSIVE_LOAD"), Map.entry(ROLE_CONFLICT, "ZMUX_ROLE_CONFLICT"),
            Map.entry(RESUME_TIMEOUT, "ZMUX_RESUME_TIMEOUT"), Map.entry(RESUME_FAILED, "ZMUX_RESUME_FAILED"),
            Map.entry(PIPE_FAILED, "ZMUX_PIPE_FAILED"), Map.entry(PIPE_RESOURCE_LIMIT, "ZMUX_PIPE_RESOURCE_LIMIT"),
            Map.entry(PIPE_AUTH_FAILED, "ZMUX_PIPE_AUTH_FAILED"), Map.entry(IDLE_TIMEOUT, "ZMUX_IDLE_TIMEOUT"));

    private TransportError() {}

    /** Registry name of a transport error code, or {@code 0x..} when unknown. */
    public static String name(long code) {
        String n = NAMES.get(code);
        return n != null ? n : "0x" + Long.toHexString(code);
    }

    /** The code of a registry name, or -1. */
    public static long code(String name) {
        for (var e : NAMES.entrySet()) if (e.getValue().equals(name)) return e.getKey();
        return -1;
    }
}
