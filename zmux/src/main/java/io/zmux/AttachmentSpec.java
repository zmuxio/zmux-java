package io.zmux;

import java.util.Objects;

/** The directions and options of one protocol attachment. */
public record AttachmentSpec(Directions directions, PipeOptions options) {
    public AttachmentSpec {
        Objects.requireNonNull(directions);
        Objects.requireNonNull(options);
        if (directions == Directions.NONE) throw new IllegalArgumentException("attachment has no direction");
    }

    PipeOptions joinOptions() {
        var direction = switch (directions) {
            case SEND -> PipeOptions.Direction.SEND_ONLY;
            case RECEIVE -> PipeOptions.Direction.RECEIVE_ONLY;
            case BOTH -> PipeOptions.Direction.BIDIRECTIONAL;
            case NONE -> throw new IllegalStateException("attachment has no direction");
        };
        return options.toBuilder().direction(direction).returnOnReply(false).build();
    }
}
