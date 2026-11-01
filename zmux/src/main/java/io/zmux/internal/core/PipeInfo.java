package io.zmux.internal.core;

/** The states of an attachment's directions as the core tracks them; its statistics are {@link io.zmux.PipeStats}. */
public final class PipeInfo {
    private PipeInfo() {}

    /** Outbound states (STATE_MACHINE.md 3.1); the first seven ordinals are Attach's OUT_ constants. */
    public enum Out {
        NONE, JOINING, ACTIVE, SUSPECT, DRAINING, FAILED, CLOSED, RETIRED;

        /** The public state, the one place they meet; null without the direction or once retired. */
        public io.zmux.PipeState.Outbound toPublic() {
            return switch (this) {
                case NONE, RETIRED -> null;
                case JOINING -> io.zmux.PipeState.Outbound.JOINING;
                case ACTIVE -> io.zmux.PipeState.Outbound.ACTIVE;
                case SUSPECT -> io.zmux.PipeState.Outbound.SUSPECT;
                case DRAINING -> io.zmux.PipeState.Outbound.DRAINING;
                case FAILED -> io.zmux.PipeState.Outbound.FAILED;
                case CLOSED -> io.zmux.PipeState.Outbound.CLOSED;
            };
        }
    }
    /** Inbound states (STATE_MACHINE.md 3.2); the first four ordinals are Attach's IN_ constants. */
    public enum In {
        NONE, JOINING, OPEN, ENDED, RETIRED;

        /** The public state; null without the direction or once retired. */
        public io.zmux.PipeState.Inbound toPublic() {
            return switch (this) {
                case NONE, RETIRED -> null;
                case JOINING -> io.zmux.PipeState.Inbound.JOINING;
                case OPEN -> io.zmux.PipeState.Inbound.OPEN;
                case ENDED -> io.zmux.PipeState.Inbound.ENDED;
            };
        }
    }
    /** Both directions of one pipe. */
    public record States(Out out, In in) {
        static final States SINGLE = new States(Out.ACTIVE, In.OPEN);
        static final States RETIRED = new States(Out.RETIRED, In.RETIRED);
    }
}
