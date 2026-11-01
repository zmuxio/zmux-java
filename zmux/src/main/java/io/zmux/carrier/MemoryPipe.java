package io.zmux.carrier;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;

/** In-memory pipe inputs connected back to back, for tests and in-process sessions. */
public final class MemoryPipe {
    private MemoryPipe() {}

    /** Two connected pipe inputs: what one writes, the other reads. */
    public static PipeIo[] pair(Framing framing) {
        End a = new End(), b = new End();
        return new PipeIo[] {
            new PipeIo(a, new Out(b, framing), framing, "memory", null),
            new PipeIo(b, new Out(a, framing), framing, "memory", null)
        };
    }

    /** The receiving end: buffers what arrives before start. */
    private static final class End implements PipeReader {
        private PipeReader.Sink sink;
        private final ArrayDeque<Object> early = new ArrayDeque<>();
        private boolean closed;

        @Override
        public synchronized void start(Sink s) {
            sink = s;
            while (!early.isEmpty()) deliver(early.poll());
        }

        @Override
        public synchronized void close() {
            closed = true;
            for (Object item; (item = early.poll()) != null; ) if (item instanceof Data d) d.release().run();
        }

        synchronized void push(Object item) {
            if (closed) { if (item instanceof Data d) d.release().run(); return; }
            if (sink == null) early.add(item);
            else deliver(item);
        }

        private void deliver(Object item) {
            if (item instanceof Data(ByteBuffer bytes, Runnable release)) sink.onData(bytes, release);
            else if (item instanceof Throwable t) sink.onError(t);
            else sink.onEnd();
        }
    }

    private static final Object EOF = new Object();

    /** A copied write; release completes the writer once the reader consumed every part. */
    private record Data(ByteBuffer bytes, Runnable release) {}

    private static Runnable once(Runnable r) {
        var done = new java.util.concurrent.atomic.AtomicBoolean();
        return () -> { if (done.compareAndSet(false, true)) r.run(); };
    }

    /** The sending end: copies each write to the peer, one copy per message (or per write of a byte stream). */
    private static final class Out implements PipeWriter {
        private final End peer;
        private final Framing framing;
        private boolean done;

        Out(End peer, Framing framing) {
            this.peer = peer;
            this.framing = framing;
        }

        @Override
        public void write(OutboundBatch batch, Completion c) {
            synchronized (this) {
                if (done) {
                    c.failed(new java.io.IOException("pipe closed"));
                    return;
                }
            }
            ByteBuffer[] items;
            if (framing == Framing.MESSAGES) {
                items = new ByteBuffer[batch.messageCount()];
                for (int m = 0, from = 0; m < items.length; m++) {
                    int to = batch.messageEnd(m);
                    items[m] = gather(batch, from, to);
                    from = to;
                }
            } else {
                items = batch.bytes() == 0 ? new ByteBuffer[0] : new ByteBuffer[] {gather(batch, 0, batch.segmentCount())};
            }
            if (items.length == 0) { c.completed(); return; }
            // The write completes when the reader released every part: the writer's backpressure.
            var left = new java.util.concurrent.atomic.AtomicInteger(items.length);
            Runnable release = () -> { if (left.decrementAndGet() == 0) c.completed(); };
            for (ByteBuffer d : items) peer.push(new Data(d, once(release)));
        }

        private static ByteBuffer gather(OutboundBatch batch, int from, int to) {
            int n = 0;
            for (int i = from; i < to; i++) n += batch.segment(i).remaining();
            ByteBuffer copy = ByteBuffer.allocate(n);
            for (int i = from; i < to; i++) copy.put(batch.segment(i));
            return copy.flip();
        }

        @Override
        public synchronized void close() {
            if (done) return;
            done = true;
            peer.push(EOF);
        }

        @Override
        public synchronized void abort(Throwable cause) {
            if (done) return;
            done = true;
            peer.push(cause == null ? new java.io.IOException("aborted") : cause);
        }
    }
}
