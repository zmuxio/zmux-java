package io.zmux.internal.loop;

import io.zmux.Chunk;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.StreamRecv;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.function.Function;

/** Ownership transfer from a carrier's public chunk to its bound host. */
public final class ChunkAccess {
    private record Access(Function<Chunk, StreamRecv.Chunk> take, Function<StreamRecv.Chunk, Chunk> make) {}
    private static volatile Access access;

    private ChunkAccess() {}

    public static void install(Function<Chunk, StreamRecv.Chunk> transfer, Function<StreamRecv.Chunk, Chunk> make) {
        if (access != null) throw new IllegalStateException("chunk transfer already installed");
        access = new Access(transfer, make);
    }

    public static StreamRecv.Chunk take(Chunk chunk) {
        // The chunk's class initializer installed the accessor before creating this instance.
        return access.take.apply(chunk);
    }

    /** Retains a range for an owner on another host, preserving the original backing. */
    static Chunk retain(ReadBuf owner, ByteBuffer data, int pos, int length) {
        owner.ref();
        try {
            return owned(owner, data.slice(pos, length));
        } catch (RuntimeException | Error failure) {
            owner.unref();
            throw failure;
        }
    }

    /** Transfers the caller's backing reference into a chunk. */
    public static Chunk owned(ReadBuf owner, ByteBuffer view) {
        if (access == null) {
            try {
                MethodHandles.lookup().ensureInitialized(Chunk.class);
            } catch (IllegalAccessException impossible) {
                throw new ExceptionInInitializerError(impossible);
            }
        }
        return access.make.apply(new StreamRecv.Chunk(view, owner, false));
    }
}
