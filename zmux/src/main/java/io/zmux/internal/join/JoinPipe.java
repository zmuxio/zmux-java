package io.zmux.internal.join;

/** The required content of a Hello. {@code direction} is 0 up, 1 down, 2 bidi. */
public record JoinPipe(long id, int direction, long fateGroup, long priority, boolean standby) {}
