package io.zmux.internal.spi;

import io.zmux.Chunk;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.loop.ChunkAccess;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.function.LongConsumer;

/** Interns a provider's known allocation identity for as long as zmux holds any view. */
public final class ProviderBackings {
    private final IdentityHashMap<Object, ReadBuf> roots = new IdentityHashMap<>();
    private final LongConsumer backingBytes;

    public ProviderBackings() { this(null); }

    public ProviderBackings(LongConsumer backingBytes) { this.backingBytes = backingBytes; }

    /** Takes one provider reference on the first view of the allocation. */
    public Chunk chunk(Object allocation, int capacity, ByteBuffer data, Runnable retain, Runnable release) {
        if (capacity < data.remaining()) throw new IllegalArgumentException("view bigger than provider backing");
        ByteBuffer view = data.slice();
        ReadBuf owner = account(allocation, capacity, view, retain, release);
        try { return ChunkAccess.owned(owner, view); }
        catch (RuntimeException | Error failure) { owner.unref(); throw failure; }
    }

    private synchronized ReadBuf account(Object allocation, int capacity, ByteBuffer view, Runnable retain, Runnable release) {
        ReadBuf root = roots.get(allocation);
        if (root != null && root.tryRef()) {
            if (root.capacity() == capacity) return root;
            root.unref();
            throw new IllegalArgumentException("live provider allocation changed capacity");
        }
        retain.run();
        try {
            root = new ReadBuf(view, capacity, owner -> released(allocation, owner, release));
            roots.put(allocation, root);
        } catch (RuntimeException | Error failure) {
            release.run();
            throw failure;
        }
        if (backingBytes != null) backingBytes.accept(capacity);
        return root;
    }

    private void released(Object allocation, ReadBuf owner, Runnable release) {
        synchronized (this) { roots.remove(allocation, owner); }
        try { release.run(); }
        finally { if (backingBytes != null) backingBytes.accept(-owner.capacity()); }
    }
}
