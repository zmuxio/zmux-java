package io.zmux.internal.loop;

import io.zmux.internal.core.ReadBuf;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An owned input buffer a transport lends to its session: the session may keep references
 * to it, and its release runs once the last one goes. The host counts it as retained from
 * the moment the transport lets go while the session still holds it.
 */
final class OwnedInput {
    final ReadBuf buf;
    private final SessionHost host;
    private final int size;
    private final AtomicBoolean kept = new AtomicBoolean();

    OwnedInput(SessionHost host, ByteBuffer data, Runnable release) {
        this.host = host;
        this.size = data.remaining();
        this.buf = new ReadBuf(data, ignored -> {
            if (kept.get()) host.retainedInput(-size);
            release.run();
        });
    }

    /** Drops the transport's reference (once). */
    void release() {
        if (buf.refs() > 1 && kept.compareAndSet(false, true)) host.retainedInput(size);
        buf.unref();
    }
}
