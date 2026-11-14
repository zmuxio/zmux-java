package io.zmux;

import io.zmux.internal.core.ReadBuf;
import io.zmux.internal.core.StreamRecv;
import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An owned, read-only byte range. Close it after reading, or transfer it to
 * {@link ZmuxSendStream#writeChunk(Chunk)}. Transfer consumes it on every outcome.
 *
 * <p>A chunk that is neither closed nor transferred keeps its buffer (a session's receive
 * buffer, a carrier's read buffer) until it is. As a safety net, one chunk in 128 is watched by
 * default: when the garbage collector finds one of those dropped, zmux logs a warning and
 * releases its buffer, so a leak shows in the log at the cost of watching a sample. The system
 * property {@code zmux.leakDetection} changes the share: {@code paranoid} watches every chunk,
 * with the stack that created it, and releases every dropped one; {@code off} watches none.
 */
public final class Chunk implements AutoCloseable {
    private final AtomicReference<StreamRecv.Chunk> value;
    /** The leak watch of a sampled chunk, or null. */
    private final Cleaner.Cleanable leak;

    Chunk(StreamRecv.Chunk chunk) {
        ByteBuffer data = chunk.data().asReadOnlyBuffer();
        value = new AtomicReference<>(new StreamRecv.Chunk(data, chunk.buf() == null ? new ReadBuf(data, null) : chunk.buf(), chunk.fin()));
        leak = Leaks.track(this, value);
    }

    /** Transfers ownership of the buffer's remaining bytes, without copying. Do not mutate them afterwards. */
    public static Chunk of(ByteBuffer data) {
        return new Chunk(new StreamRecv.Chunk(Objects.requireNonNull(data).slice(), null, false));
    }

    /** Owns bytes and their release callback; useful for pooled carrier buffers.
     * Release must not block: the last reference may be dropped on an event loop. */
    public static Chunk of(ByteBuffer data, Runnable release) {
        ByteBuffer view = Objects.requireNonNull(data).slice();
        Objects.requireNonNull(release);
        return new Chunk(new StreamRecv.Chunk(view, new ReadBuf(view, ignored -> release.run()), false));
    }

    /** A read-only view, valid until close or transfer. Its position is independent. */
    public ByteBuffer buffer() {
        StreamRecv.Chunk chunk = value.get();
        if (chunk == null) throw new IllegalStateException("chunk already closed or transferred");
        return chunk.data().duplicate();
    }

    StreamRecv.Chunk take() {
        StreamRecv.Chunk result = value.getAndSet(null);
        if (result == null) throw new IllegalStateException("chunk already closed or transferred");
        if (leak != null) leak.clean();
        return result;
    }

    /** Releases this holder; idempotent. */
    @Override public void close() {
        StreamRecv.Chunk chunk = value.getAndSet(null);
        if (chunk != null) chunk.buf().unref();
        if (leak != null) leak.clean();
    }

    /** Sampled detection and release of chunks that were dropped without close or transfer. */
    private static final class Leaks {
        /** 0: off; 1: every chunk; n: one chunk in n. */
        private static final int RATE = switch (System.getProperty("zmux.leakDetection", "sample")) {
            case "paranoid" -> 1;
            case "off" -> 0;
            default -> 128;
        };
        private static final Cleaner CLEANER = RATE == 0 ? null : Cleaner.create(Thread.ofVirtual().name("zmux-leak-detector").factory());
        private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger(Chunk.class.getName());

        static Cleaner.Cleanable track(Chunk holder, AtomicReference<StreamRecv.Chunk> value) {
            if (RATE == 0 || (RATE > 1 && java.util.concurrent.ThreadLocalRandom.current().nextInt(RATE) != 0)) return null;
            Throwable site = RATE == 1 ? new Throwable("chunk created here") : null;
            return CLEANER.register(holder, () -> {
                // Closing or transferring empties value first, so only a dropped chunk reports.
                StreamRecv.Chunk chunk = value.getAndSet(null);
                if (chunk == null) return;
                LOG.log(java.util.logging.Level.WARNING, "zmux Chunk dropped without close or transfer; released now", site);
                chunk.buf().unref();
            });
        }
    }
}
