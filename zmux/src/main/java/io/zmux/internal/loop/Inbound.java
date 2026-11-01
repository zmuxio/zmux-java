package io.zmux.internal.loop;

import io.zmux.internal.core.Batch;
import io.zmux.internal.core.ReadBuf;
import java.nio.ByteBuffer;

/**
 * What a {@link Transport} reports to, on its loop: a gate of the listener, a fallback
 * target, or a session's attachment. A delivery that moves or closes the transport returns
 * {@link #GONE}; the transport then stops touching its input.
 */
public interface Inbound {
    /** Returned by {@link #bytes} when the transport left its owner (closed or moved). */
    int GONE = -1;

    /** Input bytes [pos, lim) of buf: the count consumed, or {@link #GONE}. */
    int bytes(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim);

    /**
     * Whether this is a session's attachment: it takes whole messages of a message-framed
     * carrier through {@link #message} and may keep references to lent payloads.
     */
    default boolean session() { return false; }

    /** One complete message [pos, lim) of buf (only when {@link #session()}). */
    default void message(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
        throw new UnsupportedOperationException("not a session attachment");
    }

    /** The input half ended: gracefully (the peer ended it) or not, partial with unconsumed bytes. */
    void ended(Transport t, boolean graceful, boolean partial);

    /** The output half's writability changed. */
    default void writable(Transport t, boolean on) {}

    /** The outcome of the write of b this inbound handed over. */
    default void written(Transport t, Batch b, Throwable cause) { b.discard(); }

    /**
     * A failure inside the endpoint while serving the transport (not an I/O failure):
     * true when the inbound handled it (a session closes with INTERNAL_ERROR).
     */
    default boolean failed(Transport t, Throwable error) { return false; }
}
