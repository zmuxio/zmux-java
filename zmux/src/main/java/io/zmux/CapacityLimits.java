package io.zmux;

/**
 * Local attachment, carrier and peer Join limits.
 *
 * @param steadyAttachments the steady limit for locally initiated attachments
 * @param joinHeadroom temporary attachment capacity, null for the default
 * @param localCarriers the maximum carrier leases, including carriers awaiting I/O release
 * @param pendingPeerJoins the independent limit for each of authorization execution and unconfirmed peer Joins
 */
public record CapacityLimits(int steadyAttachments, Integer joinHeadroom, int localCarriers, int pendingPeerJoins) {
    public static final int DEFAULT_JOIN_HEADROOM = 8;
    public static final int DEFAULT_PENDING_PEER_JOINS = 4;

    private static final CapacityLimits DEFAULT = new CapacityLimits(1, null);

    public CapacityLimits {
        if (steadyAttachments == 0) steadyAttachments = 1;
        if (joinHeadroom == null) joinHeadroom = DEFAULT_JOIN_HEADROOM;
        long envelope = (long) steadyAttachments + joinHeadroom;
        if (steadyAttachments < 1 || joinHeadroom < 0 || envelope > Integer.MAX_VALUE / 2
                || localCarriers < 0 || pendingPeerJoins < 0) {
            throw new IllegalArgumentException("invalid session capacity limits");
        }
        if (localCarriers == 0) localCarriers = (int) (2 * envelope);
        if (pendingPeerJoins == 0) pendingPeerJoins = DEFAULT_PENDING_PEER_JOINS;
    }

    public CapacityLimits(int steadyAttachments, Integer joinHeadroom) {
        this(steadyAttachments, joinHeadroom, 0, 0);
    }

    public CapacityLimits(int steadyAttachments) {
        this(steadyAttachments, null);
    }

    public static CapacityLimits defaults() { return DEFAULT; }

    /** Total local attachment capacity, including temporary Join headroom. */
    public int attachmentLimit() { return steadyAttachments + joinHeadroom; }
}
