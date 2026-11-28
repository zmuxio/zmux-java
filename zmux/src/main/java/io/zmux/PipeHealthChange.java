package io.zmux;

import java.time.Duration;

/** A committed transition in a session's direction-health sequence. */
public record PipeHealthChange(long seq, PipeId pipe, Direction direction, DirectionHealth before, DirectionHealth after,
                               DirectionHealth.Reason reason, long at, Duration sinceJoin, Throwable error) {}
