package io.zmux.internal.wire;

/** Frame type codepoints (REGISTRY.md Section 3). */
public final class FrameType {
    public static final long PADDING = 0x00;
    public static final long RESET_STREAM = 0x04;
    public static final long STOP_SENDING = 0x05;
    /** STREAM occupies 0x08-0x0f; the low bits are OFF, LEN and FIN. */
    public static final long STREAM = 0x08;
    public static final int STREAM_FIN = 0x01;
    public static final int STREAM_LEN = 0x02;
    public static final int STREAM_OFF = 0x04;
    public static final long MAX_DATA = 0x10;
    public static final long MAX_STREAM_DATA = 0x11;
    public static final long MAX_STREAMS_BIDI = 0x12;
    public static final long MAX_STREAMS_UNI = 0x13;
    public static final long DATA_BLOCKED = 0x14;
    public static final long STREAM_DATA_BLOCKED = 0x15;
    public static final long STREAMS_BLOCKED_BIDI = 0x16;
    public static final long STREAMS_BLOCKED_UNI = 0x17;
    public static final long CLOSE_TRANSPORT = 0x1c;
    public static final long CLOSE_APPLICATION = 0x1d;
    public static final long RESET_STREAM_AT = 0x24;
    public static final long DATAGRAM = 0x30;
    public static final long DATAGRAM_LEN = 0x31;
    public static final long TRANSPORT_PARAMETERS = 0x3f5153300d0a0d0aL;
    public static final long PING_REQUEST = 0x348c67529ef8c7bdL;
    public static final long PING_RESPONSE = 0x348c67529ef8c7beL;
    public static final long GOAWAY = 0x1911f43eL;
    public static final long PRIORITY_UPDATE = 0x073d08cfL;
    public static final long PIPE_JOIN = 0x1a24de36L;
    public static final long PIPE_ACK = 0x0b2b3eebL;
    public static final long PIPE_LANE = 0x3a05a1c9L;
    public static final long PIPE_PING = 0x269b43e6L;
    public static final long PIPE_ABANDON = 0x23b35b74L;
    public static final long PIPE_STATUS = 0x227b2811L;

    private FrameType() {}
}
