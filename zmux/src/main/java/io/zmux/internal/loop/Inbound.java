package io.zmux.internal.loop;

import io.zmux.internal.core.Batch;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.ReceiveSpans;
import io.zmux.internal.wire.VarInt;
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

    /** Whether the inbound can consume a byte record that spans separately owned blocks. */
    default boolean segmentedInput() { return false; }

    default int bytes(Transport t, ReceiveSpans spans) {
        throw new UnsupportedOperationException("inbound requires contiguous records");
    }

    /** Bytes needed to make progress on the partial record left by {@link #bytes}. */
    default int missingRecordBytes(ByteBuffer data, int pos, int lim) {
        int have = lim - pos;
        int head = VarInt.lengthOf(data.get(pos));
        if (have < head) return head - have;
        return Math.clamp(head + VarInt.get(data, pos, lim) - have, 1, Integer.MAX_VALUE);
    }

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

    /** The transport's write side is shut: FIN, close_notify or a stream FIN went to the carrier. */
    default void outputShut(Transport t) {}

    /** Newly proved cumulative prefix of the outstanding write, on its owning loop. */
    default void progress(Transport t, Batch b, int confirmed) {}

    /** The outcome of the write of b this inbound handed over. */
    default void written(Transport t, Batch b, int confirmed, Throwable cause) { b.discard(); }

    /**
     * A failure inside the endpoint while serving the transport (not an I/O failure):
     * true when the inbound handled it (a session closes with INTERNAL_ERROR).
     */
    default boolean failed(Transport t, Throwable error) { return false; }
}
