package io.zmux.internal.join;

/** Who protects the records of a carrier; fixed by its handshake. */
public enum Mode {
    /** Records are protected with the keys of the handshake. */
    SEALED,
    /** Records are left to the carrier, whose exporter is bound in. */
    BOUND,
    /** Records are left to the carrier by explicit configuration. */
    TRUSTED;

    /** The public name of a handshake's mode; null: UNMANAGED. */
    public static io.zmux.RecordMode record(Mode m) {
        if (m == null) return io.zmux.RecordMode.UNMANAGED;
        return switch (m) {
            case SEALED -> io.zmux.RecordMode.SEALED;
            case BOUND -> io.zmux.RecordMode.BOUND;
            case TRUSTED -> io.zmux.RecordMode.TRUSTED;
        };
    }

    /** The handshake's mode of a public name. */
    public static Mode of(io.zmux.RecordMode m) {
        if (m == io.zmux.RecordMode.UNMANAGED) throw new IllegalArgumentException("a handshake has no Unmanaged mode");
        return switch (m) {
            case SEALED -> Mode.SEALED;
            case BOUND -> Mode.BOUND;
            case TRUSTED -> Mode.TRUSTED;
            case UNMANAGED -> throw new IllegalArgumentException("a handshake has no Unmanaged mode");
        };
    }

    /** The value on the wire. */
    public int wire() {
        return switch (this) {
            case SEALED -> 0;
            case BOUND -> 1;
            case TRUSTED -> 2;
        };
    }

    /** The mode of a wire value, or null when it is reserved. */
    public static Mode ofWire(int v) {
        return switch (v) {
            case 0 -> SEALED;
            case 1 -> BOUND;
            case 2 -> TRUSTED;
            default -> null;
        };
    }
}
