package io.zmux;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.StreamRecv;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * An owned, read-only byte range. Close it after reading, or transfer it to
 * {@link ZmuxSendStream#writeChunk(Chunk)}. Transfer consumes it on every outcome.
 *
 * <p>Close or transfer every chunk to return its storage. Any borrowed view must stop
 * being used before the chunk is closed or transferred.
 */
public final class Chunk implements AutoCloseable {
    private static final VarHandle VALUE;
    static {
        try {
            VALUE = MethodHandles.lookup().findVarHandle(Chunk.class, "value", StreamRecv.Chunk.class);
        } catch (ReflectiveOperationException impossible) {
            throw new ExceptionInInitializerError(impossible);
        }
        io.zmux.internal.loop.ChunkAccess.install(Chunk::take, Chunk::new);
    }

    // VALUE clears this field atomically when ownership is transferred or released.
    @SuppressWarnings("FieldMayBeFinal")
    private volatile StreamRecv.Chunk value;

    Chunk(StreamRecv.Chunk chunk) {
        Objects.requireNonNull(chunk.buf());
        value = chunk;
    }

    /** Transfers ownership of the buffer's remaining bytes, without copying. Do not mutate them afterwards. */
    public static Chunk of(ByteBuffer data) {
        ByteBuffer view = Objects.requireNonNull(data).slice();
        return new Chunk(new StreamRecv.Chunk(view, new ReadBuf(view, data.capacity(), null), false));
    }

    /** Owns bytes and their release callback; useful for pooled carrier buffers.
     * Release must not block: the last reference may be dropped on an event loop. */
    public static Chunk of(ByteBuffer data, Runnable release) {
        ByteBuffer view = Objects.requireNonNull(data).slice();
        Objects.requireNonNull(release);
        return new Chunk(new StreamRecv.Chunk(view, new ReadBuf(view, data.capacity(), ignored -> release.run()), false));
    }

    /** The length of this chunk. */
    public int remaining() {
        return owned().data().remaining();
    }

    /** Copies this chunk's bytes into a new array. */
    public byte[] copy() {
        ByteBuffer data = owned().data();
        byte[] copy = new byte[data.remaining()];
        data.get(data.position(), copy);
        return copy;
    }

    /**
     * Runs the action with a read-only view with independent position. The view and all
     * derived views must stay within the action. Close or transfer the chunk afterwards.
     * Operations on the same chunk must not overlap.
     */
    public void withBuffer(java.util.function.Consumer<? super ByteBuffer> action) {
        action.accept(owned().data().asReadOnlyBuffer());
    }

    private StreamRecv.Chunk owned() {
        StreamRecv.Chunk chunk = value;
        if (chunk == null) throw new IllegalStateException("chunk already closed or transferred");
        return chunk;
    }

    StreamRecv.Chunk take() {
        StreamRecv.Chunk result = (StreamRecv.Chunk) VALUE.getAndSet(this, null);
        if (result == null) throw new IllegalStateException("chunk already closed or transferred");
        return result;
    }

    /** Releases this holder; idempotent. */
    @Override public void close() {
        StreamRecv.Chunk chunk = (StreamRecv.Chunk) VALUE.getAndSet(this, null);
        if (chunk != null) chunk.buf().unref();
    }
}
