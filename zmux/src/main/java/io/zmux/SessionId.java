package io.zmux;

/**
 * A session's identity within this process, unique for its lifetime and assigned in creation order
 * ({@link ZmuxSession#id()}). Tracer callbacks carry it, so one tracer shared by many sessions tells them apart.
 *
 * @param value the number, from 1 upwards
 */
public record SessionId(long value) {
    @Override public String toString() { return "session #" + value; }
}
