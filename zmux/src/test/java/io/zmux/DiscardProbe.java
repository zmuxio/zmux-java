package io.zmux;

import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Records which operations reached a carrier that should be discarded before Join. */
public final class DiscardProbe implements PipeWriter {
    private final CompletableFuture<Void> discarded = new CompletableFuture<>();
    private final AtomicInteger closes = new AtomicInteger();
    private final AtomicInteger writes = new AtomicInteger();

    public DiscardProbe() {}

    public PipeIo carrier() {
        return PipeIo.of(new PipeReader() {
            @Override public void start(Sink sink) {}
            @Override public void close() {}
        }, this);
    }

    @Override public void close() {
        closes.incrementAndGet();
        discarded.complete(null);
    }
    @Override public void write(OutboundBatch batch, Completion completion) {
        writes.incrementAndGet();
        completion.failed(new java.io.IOException("discarded carrier got a handshake"));
    }

    public void awaitDiscard() throws Exception { discarded.get(30, TimeUnit.SECONDS); }
    public long closes() { return closes.get(); }
    public long writes() { return writes.get(); }

    void await() throws Exception {
        awaitDiscard();
        assertEquals(1, closes.get());
        assertEquals(0, writes.get());
    }
}
