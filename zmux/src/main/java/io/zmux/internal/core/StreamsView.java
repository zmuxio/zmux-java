package io.zmux.internal.core;

/** What the path layer sees of the session's streams and may do to their sending parts. */
interface StreamsView {
    /** The live stream with identifier id, or null. */
    Stream get(long id);

    /** The live streams. */
    Iterable<Stream> live();

    /** Whether STREAMS_BLOCKED of type t still reports the current limit. */
    boolean streamsBlockedAtLimit(int t);

    /** Releases acknowledged data of st below upTo and wakes its writer. */
    void releaseAcked(Stream st, long upTo);

    /** Records a terminal sending state of st and releases its buffers. */
    void sendTerminal(Stream st, int sendState);
}
