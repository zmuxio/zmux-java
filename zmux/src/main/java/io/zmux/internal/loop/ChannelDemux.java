package io.zmux.internal.loop;

import io.zmux.internal.wire.ChannelDatagram;
import io.zmux.carrier.VarInt;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Routes the channel datagrams of one carrier connection shared by several pipes, such as a
 * QUIC connection, to those pipes by their Pipe Stream ID (EXTENSIONS.md Section 6.8); the
 * pipes may belong to several sessions. Used on the connection's loop; {@link #dropped} from
 * any thread.
 */
public final class ChannelDemux {
    private final HashMap<Long, CarrierChannel.Sink> pipes = new HashMap<>();
    private final AtomicLong dropped = new AtomicLong();

    /** Hands the datagrams for Pipe Stream ID id to sink until the returned stop runs; a later registration of id replaces it. */
    public Runnable register(long id, CarrierChannel.Sink sink) {
        pipes.put(id, sink);
        return () -> pipes.remove(id, sink);
    }

    /**
     * Routes one channel datagram, b[pos, end), to the pipe its Pipe Stream ID names; one that
     * names no registered pipe (unknown, not yet attached, or closed) or whose ID is truncated
     * is dropped and counted.
     */
    public void deliver(ByteBuffer b, int pos, int end) {
        long id = ChannelDatagram.pipeStreamId(b, pos, end);
        CarrierChannel.Sink sink = id < 0 ? null : pipes.get(id);
        if (sink == null) {
            dropped.incrementAndGet();
            return;
        }
        sink.datagram(b, pos + VarInt.lengthOf(b.get(pos)), end);
    }

    /** The datagrams {@link #deliver} dropped because they named no registered pipe. */
    public long dropped() {
        return dropped.get();
    }
}
