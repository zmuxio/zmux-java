package io.zmux.internal.join;

/**
 * A key exchange of Join and of session epochs: the lengths of
 * the initiator's and the responder's shares, and the priority that orders Groups, higher
 * preferred. The initiator is the joiner, or the end that offers an epoch.
 */
public enum Group {
    /** X25519 alone. */
    X25519(1, 1, 32, 32),
    /** X25519 and ML-KEM-768: {@code e ‖ ek} from the initiator, {@code e ‖ ct} from the responder. */
    X25519_MLKEM768(2, 2, 32 + 1184, 32 + 1088);

    private final long id;
    private final int priority, initiatorShare, responderShare;

    Group(long id, int priority, int initiatorShare, int responderShare) {
        this.id = id;
        this.priority = priority;
        this.initiatorShare = initiatorShare;
        this.responderShare = responderShare;
    }

    /** The registry value. */
    public long id() { return id; }

    /** The place in the total order of Groups, higher preferred. */
    public int priority() { return priority; }

    /** The length of the share the initiator, or else the responder, sends. */
    public int shareLength(boolean initiator) { return initiator ? initiatorShare : responderShare; }

    /** The Group with this registry value, or null for an unknown one. */
    public static Group of(long id) {
        for (Group g : values()) if (g.id == id) return g;
        return null;
    }

    /** The registry values of this implementation, ascending. */
    public static long[] supported() {
        return new long[] {X25519.id, X25519_MLKEM768.id};
    }

    /** The usable Group of highest priority among ids; null when none is known. */
    public static Group highest(long[] ids) {
        Group best = null;
        for (long id : ids) {
            Group g = of(id);
            if (g != null && (best == null || g.priority > best.priority)) best = g;
        }
        return best;
    }
}
