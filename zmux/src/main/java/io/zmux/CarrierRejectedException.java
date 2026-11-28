package io.zmux;

import java.io.Serial;

/**
 * An {@link InboundTransform}'s refusal of an unauthenticated carrier. The listener
 * observes the local reason through {@link ListenerConfig#onReject()} and applies
 * {@link ListenerConfig#rejection()} to the carrier.
 */
public final class CarrierRejectedException extends ZmuxException {
    @Serial
    private static final long serialVersionUID = 1L;

    private final int profile;
    private final RecordMode recordMode;
    private final String reason;

    /**
     * @param profile the profile of the first message, 0 when it has none
     * @param recordMode its record mode, or null
     * @param reason the reason, as {@link RejectInfo#reason()}
     */
    public CarrierRejectedException(int profile, RecordMode recordMode, String reason) {
        this(profile, recordMode, reason, null);
    }

    /** With the underlying failure, such as a {@code Gate}'s exception, as the cause. */
    public CarrierRejectedException(int profile, RecordMode recordMode, String reason, Throwable cause) {
        super(reason, cause);
        this.profile = profile;
        this.recordMode = recordMode;
        this.reason = reason;
    }

    public int profile() { return profile; }
    public RecordMode recordMode() { return recordMode; }
    public String reason() { return reason; }
}
