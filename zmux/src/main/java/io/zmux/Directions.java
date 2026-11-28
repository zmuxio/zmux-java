package io.zmux;

/** A set of directions relative to this endpoint. */
public enum Directions {
    NONE, SEND, RECEIVE, BOTH;

    public boolean sends() { return this == SEND || this == BOTH; }

    public boolean receives() { return this == RECEIVE || this == BOTH; }
}
