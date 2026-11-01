package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import io.zmux.internal.diag.Diagnostics;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Random;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** How much carrier input the transports copy (IMPLEMENTATION.md Section 2.8 and design §8.5). */
@Timeout(60)
class CarrierCopyTest {
    /**
     * A byte-stream pipe whose reader hands what was written on as owned chunks of 256 KiB,
     * cut wherever the records fall; what is left is handed on once the writer pauses (a
     * millisecond) or makes a small write (control traffic, the end of a burst).
     */
    static PipeIo[] chunkPair() {
        ChunkEnd a = new ChunkEnd(), b = new ChunkEnd();
        // The reader hands a whole chunk on at once: it declares the memory that may take.
        return new PipeIo[] {PipeIo.of(a, b.writer()).withReceiveMemory(1 << 20), PipeIo.of(b, a.writer()).withReceiveMemory(1 << 20)};
    }

    static final class ChunkEnd implements PipeReader {
        static final int CHUNK = 256 << 10;
        private static final ScheduledExecutorService FLUSH = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "chunk-flush"); t.setDaemon(true); return t;
        });
        private ScheduledFuture<?> flush;
        private final ArrayDeque<Object> queue = new ArrayDeque<>();
        private final java.io.ByteArrayOutputStream pending = new java.io.ByteArrayOutputStream();
        private PipeReader.Sink sink;
        private boolean closed;

        @Override public synchronized void start(Sink s) { sink = s; drain(); }
        @Override public synchronized void close() { closed = true; queue.clear(); }

        private synchronized void push(byte[] bytes, boolean end) {
            if (closed) return;
            pending.writeBytes(bytes);
            if (flush != null) flush.cancel(false);
            flush = null;
            boolean now = end || bytes.length < 4096;
            byte[] all = pending.toByteArray();
            int at = 0;
            for (; all.length - at >= CHUNK || now && at < all.length; at += Math.min(CHUNK, all.length - at)) {
                queue.add(ByteBuffer.wrap(all, at, Math.min(CHUNK, all.length - at)).slice());
            }
            pending.reset();
            pending.write(all, at, all.length - at);
            if (end) queue.add(Boolean.TRUE);
            else if (pending.size() > 0) flush = FLUSH.schedule(() -> push(new byte[0], false), 1, TimeUnit.MILLISECONDS);
            drain();
        }

        private void drain() {
            if (sink == null) return;
            for (Object o; (o = queue.poll()) != null; ) {
                if (Boolean.TRUE.equals(o)) sink.onEnd();
                else sink.onChunk(Chunk.of((ByteBuffer) o), () -> {});
            }
        }

        PipeWriter writer() {
            ChunkEnd peer = this;
            return new PipeWriter() {
                @Override public void write(OutboundBatch batch, Completion done) {
                    byte[] all = new byte[batch.bytes()];
                    ByteBuffer out = ByteBuffer.wrap(all);
                    for (int i = 0; i < batch.segmentCount(); i++) out.put(batch.segment(i));
                    peer.push(all, false);
                    done.completed();
                }
                @Override public void close() { peer.push(new byte[0], true); }
            };
        }
    }

    static void echo(ZmuxSession client, ZmuxSession server, byte[] data) throws Exception {
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var echo = workers.submit(() -> {
                try (ZmuxStream s = server.acceptStream()) {
                    ByteBuffer b = ByteBuffer.allocate(64 << 10);
                    while (s.read(b.clear()) >= 0) s.write(b.flip());
                    s.finish();
                }
                return null;
            });
            byte[] back = new byte[data.length];
            try (ZmuxStream s = client.openStream()) {
                var reader = workers.submit(() -> {
                    ByteBuffer b = ByteBuffer.wrap(back);
                    while (b.hasRemaining() && s.read(b) >= 0) { }
                    return null;
                });
                s.write(ByteBuffer.wrap(data));
                s.finish();
                reader.get(30, TimeUnit.SECONDS);
            }
            echo.get(30, TimeUnit.SECONDS);
            assertArrayEquals(data, back);
        }
    }

    @Test void ownedChunksAreParsedInPlaceAndOnlyTheCutRecordIsCopied() throws Exception {
        byte[] data = new byte[8 << 20];
        new Random(3).nextBytes(data);
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            long copied0 = Diagnostics.copiedInputBytes(loops), inPlace0 = Diagnostics.inPlaceInputBytes(loops);
            PipeIo[] pair = chunkPair();
            var server = CompletableFuture.supplyAsync(() -> {
                try { return Zmux.open(pair[1], Role.SERVER, config); } catch (Exception e) { throw new CompletionException(e); }
            });
            try (var client = Zmux.open(pair[0], Role.CLIENT, config); var accepted = server.get(10, TimeUnit.SECONDS)) {
                echo(client, accepted, data);
                long copied = Diagnostics.copiedInputBytes(loops) - copied0, inPlace = Diagnostics.inPlaceInputBytes(loops) - inPlace0;
                assertTrue(copied + inPlace >= 2L * data.length, copied + " + " + inPlace);
                // A chunk's end cuts one record (of at most 64 KiB): it is carried, and the records
                // after it are parsed in place again. (Copying everything after the first cut, as
                // before, comes to nearly all of it.)
                assertTrue(copied * 4 < copied + inPlace, "copied " + copied + " of " + (copied + inPlace));
            }
            assertEquals(0, Diagnostics.retainedInputBytes(loops));
            assertEquals(0, Diagnostics.outstandingBuffers(loops));
        }
    }

    @Test void lentMessagesAreParsedInPlaceOneRecordEach() throws Exception {
        byte[] data = new byte[2 << 20];
        new Random(4).nextBytes(data);
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            long copied0 = Diagnostics.copiedInputBytes(loops), inPlace0 = Diagnostics.inPlaceInputBytes(loops);
            PipeIo[] pair = MemoryPipe.pair(Framing.MESSAGES);
            var server = CompletableFuture.supplyAsync(() -> {
                try { return Zmux.open(pair[1], Role.SERVER, config); } catch (Exception e) { throw new CompletionException(e); }
            });
            try (var client = Zmux.open(pair[0], Role.CLIENT, config); var accepted = server.get(10, TimeUnit.SECONDS)) {
                echo(client, accepted, data);
                long copied = Diagnostics.copiedInputBytes(loops) - copied0, inPlace = Diagnostics.inPlaceInputBytes(loops) - inPlace0;
                assertEquals(0, copied, "a lent message went through the read buffer");
                assertTrue(inPlace >= 2L * data.length, String.valueOf(inPlace));
            }
            assertEquals(0, Diagnostics.outstandingBuffers(loops));
        }
    }

    @Test void theWritersUnsentCountIsTheQueuedBytesOfThePipe() throws Exception {
        try (EventLoops loops = EventLoops.create(1)) {
            var config = ZmuxConfig.builder().eventLoops(loops).build();
            PipeIo[] pair = MemoryPipe.pair(Framing.BYTE_STREAM);
            PipeWriter inner = pair[0].writer();
            PipeWriter counting = new PipeWriter() {
                @Override public void write(OutboundBatch batch, Completion done) { inner.write(batch, done); }
                @Override public java.util.OptionalLong unsent() { return java.util.OptionalLong.of(4096); }
                @Override public void close() { inner.close(); }
                @Override public void abort(Throwable cause) { inner.abort(cause); }
            };
            PipeIo client = new PipeIo(pair[0].reader(), counting, pair[0].framing(), "", null);
            var server = CompletableFuture.supplyAsync(() -> {
                try { return Zmux.open(pair[1], Role.SERVER, config); } catch (Exception e) { throw new CompletionException(e); }
            });
            try (var c = Zmux.open(client, Role.CLIENT, config); var s = server.get(10, TimeUnit.SECONDS)) {
                assertEquals(java.util.OptionalLong.of(4096), c.paths().list().getFirst().stats().orElseThrow().queuedBytes());
                assertTrue(s.paths().list().getFirst().stats().orElseThrow().queuedBytes().isEmpty(), "a writer without the query reports none");
            }
        }
    }
}
