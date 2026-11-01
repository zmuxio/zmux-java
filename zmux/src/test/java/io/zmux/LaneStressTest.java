package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;

/**
 * Races on the stream lanes (design 8.4): many writers and readers of one stream, blocking and
 * asynchronous mixed, cancellation storms and a session ending under them. Each write is one
 * self-describing record, so a torn or reordered write shows up as a bad record.
 */
@Timeout(60)
class LaneStressTest {
    private static final int WRITERS = 8, RECORDS = 200;

    /** A record: length, writer, sequence, then the writer's byte repeated. */
    private static ByteBuffer record(int writer, int seq, int len) {
        ByteBuffer b = ByteBuffer.allocate(len);
        b.putInt(len).putInt(writer).putInt(seq);
        while (b.hasRemaining()) b.put((byte) writer);
        return b.flip();
    }

    @RepeatedTest(3)
    void concurrentWritersKeepEachWriteWholeAndInOrderPerWriter() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair p = Pair.memory(cfg)) {
                ZmuxStream out = p.client().openStream();
                out.setSendBuffer(64 << 10);
                out.id();
                ZmuxStream in = p.server().acceptStream();
                List<Future<?>> writers = new ArrayList<>();
                for (int w = 0; w < WRITERS; w++) {
                    int writer = w;
                    writers.add(ex.submit(() -> {
                        SplittableRandom r = new SplittableRandom(writer);
                        for (int seq = 0; seq < RECORDS; seq++) {
                            ByteBuffer rec = record(writer, seq, 12 + r.nextInt(40000));
                            switch (r.nextInt(3)) {
                                case 0 -> out.write(rec);
                                case 1 -> out.writeAsync(rec).get(20, TimeUnit.SECONDS);
                                default -> out.writeChunk(Chunk.of(rec));
                            }
                        }
                        return null;
                    }));
                }
                var reader = ex.submit(() -> {
                    int[] next = new int[WRITERS];
                    var stream = new java.io.DataInputStream(new java.io.BufferedInputStream(in.inputStream(), 1 << 16));
                    long records = 0;
                    while (true) {
                        int len;
                        try { len = stream.readInt(); } catch (java.io.EOFException end) { break; }
                        int writer = stream.readInt(), seq = stream.readInt();
                        assertEquals(next[writer]++, seq, "writer " + writer + " out of order");
                        byte[] body = stream.readNBytes(len - 12);
                        assertEquals(len - 12, body.length);
                        for (byte b : body) if (b != (byte) writer) fail("torn write of writer " + writer);
                        records++;
                    }
                    return records;
                });
                for (var f : writers) f.get(30, TimeUnit.SECONDS);
                out.finish();
                assertEquals((long) WRITERS * RECORDS, reader.get(30, TimeUnit.SECONDS));
                out.close();
                in.close();
            }
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    @RepeatedTest(3)
    void concurrentReadersTakeEveryByteOnce() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            try (Pair p = Pair.memory(cfg)) {
                ZmuxStream out = p.client().openStream();
                out.id();
                ZmuxStream in = p.server().acceptStream();
                long total = 8L << 20;
                AtomicLong sum = new AtomicLong(), count = new AtomicLong();
                List<Future<?>> readers = new ArrayList<>();
                for (int r = 0; r < 6; r++) {
                    int kind = r % 3;
                    readers.add(ex.submit(() -> {
                        ByteBuffer b = ByteBuffer.allocate(9000);
                        while (true) {
                            long n;
                            switch (kind) {
                                case 0 -> n = in.read(b.clear().limit(7000));
                                case 1 -> n = in.readAsync(b.clear().limit(7000)).get(20, TimeUnit.SECONDS);
                                default -> {
                                    Chunk c = in.readChunk(9000);
                                    if (c == null) { n = -1; break; }
                                    try (c) { b.clear(); ByteBuffer v = c.buffer(); n = v.remaining(); b.put(v); }
                                }
                            }
                            if (n < 0) return null;
                            b.flip();
                            long s = 0;
                            while (b.hasRemaining()) s += b.get() & 0xff;
                            sum.addAndGet(s);
                            count.addAndGet(n);
                        }
                    }));
                }
                byte[] data = new byte[1 << 16];
                for (int i = 0; i < data.length; i++) data[i] = (byte) i;
                long expected = 0;
                for (byte d : data) expected += d & 0xff;
                for (long sent = 0; sent < total; sent += data.length) out.write(data);
                out.finish();
                for (var f : readers) f.get(30, TimeUnit.SECONDS);
                assertEquals(total, count.get());
                assertEquals(expected * (total / data.length), sum.get());
                out.close();
                in.close();
            }
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }

    @RepeatedTest(3)
    void cancellationStormsAndASessionEndLeaveNothingWaitingOrHeld() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var ex = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            Pair p = Pair.memory(cfg);
            ZmuxStream out = p.client().openStream();
            out.setSendBuffer(16 << 10);
            out.id();
            ZmuxStream in = p.server().acceptStream();
            in.setReceiveWindow(16 << 10, 16 << 10);
            List<Future<?>> tasks = new ArrayList<>();
            List<CompletableFuture<?>> pending = new CopyOnWriteArrayList<>();
            for (int t = 0; t < 8; t++) {
                int seed = t;
                tasks.add(ex.submit(() -> {
                    SplittableRandom r = new SplittableRandom(seed);
                    for (int i = 0; i < 200; i++) {
                        switch (r.nextInt(5)) {
                            case 0 -> { var f = out.writeAsync(ByteBuffer.allocate(r.nextInt(1, 50000))); pending.add(f); if (r.nextBoolean()) f.cancel(true); }
                            case 1 -> { var f = in.readAsync(ByteBuffer.allocate(r.nextInt(1, 50000))); pending.add(f); if (r.nextBoolean()) f.cancel(true); }
                            case 2 -> { var f = out.writeChunkAsync(Chunk.of(ByteBuffer.allocate(r.nextInt(1, 30000)))); pending.add(f); if (r.nextBoolean()) f.cancel(true); }
                            case 3 -> { var f = in.readChunkAsync(r.nextInt(1, 30000)); pending.add(f); if (r.nextBoolean()) f.cancel(true); else f.thenAccept(c -> { if (c != null) c.close(); }); }
                            default -> { try { out.tryWrite(ByteBuffer.allocate(r.nextInt(1, 9000))); } catch (IOException ignored) { } }
                        }
                    }
                    return null;
                }));
            }
            for (var f : tasks) f.get(30, TimeUnit.SECONDS);
            p.client().close();
            p.client().awaitClosed();
            p.server().awaitClosed();
            for (var f : pending) {
                try { f.get(10, TimeUnit.SECONDS); }
                catch (ExecutionException | CancellationException ended) { /* Failed by the session's end or cancelled. */ }
                Object v = f.isCompletedExceptionally() ? null : f.getNow(null);
                if (v instanceof Chunk c) c.close();
            }
            out.close();
            in.close();
            System.gc();
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (loops.outstandingBuffers() != 0 && System.nanoTime() < until) Thread.sleep(5);
            assertEquals(0, loops.outstandingBuffers(), "read buffers leaked");
        }
    }
}
