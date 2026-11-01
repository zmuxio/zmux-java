package io.zmux;

/** No stream credit now (tryOpenStream). */
public final class StreamLimitException extends StreamException {
    public StreamLimitException() {
        super("stream limit reached", -1);
    }
}
