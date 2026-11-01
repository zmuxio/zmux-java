package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.loop.EventLoop;
import io.zmux.internal.loop.Operation;
import java.io.ByteArrayOutputStream;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class AsyncApiTest {
    private static <T> T get(Future<T> value) throws Exception { return value.get(5, TimeUnit.SECONDS); }
    private static void latch(CountDownLatch value) {
        try { if (!value.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("test latch timed out"); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException(error); }
    }
    private static byte[] read(ZmuxReceiveStream stream) throws Exception {
        var bytes = new ByteArrayOutputStream();
        for (Chunk chunk; (chunk = get(stream.readChunkAsync(65536))) != null;) {
            try (Chunk owned = chunk) { var data = owned.buffer(); byte[] part = new byte[data.remaining()]; data.get(part); bytes.write(part); }
        }
        return bytes.toByteArray();
    }
    @Test void fifoAsyncWritesAndSynchronousFinUseOneDirectionOwner() throws Exception {
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
             var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var sender = get(pair.client().openStreamAsync());
            var incoming = pair.server().acceptStreamAsync();
            byte[] first = new byte[1 << 20], second = new byte[1 << 20];
            Arrays.fill(first, (byte) 17); Arrays.fill(second, (byte) 29);
            var one = ByteBuffer.wrap(first); var two = ByteBuffer.wrap(second);
            var a = sender.writeAsync(one); var b = sender.writeAsync(two);
            var fin = workers.submit(() -> { sender.finish(); return null; });
            var receiver = get(incoming); byte[] actual = read(receiver);
            get(a); get(b); get(fin);
            assertEquals(first.length + second.length, actual.length);
            assertArrayEquals(first, Arrays.copyOfRange(actual, 0, first.length));
            assertArrayEquals(second, Arrays.copyOfRange(actual, first.length, actual.length));
            assertEquals(one.limit(), one.position()); assertEquals(two.limit(), two.position());
            assertEquals(sender.id(), get(sender.idAsync()));
            assertFalse(a.cancel(true)); sender.close(); receiver.close();
        }
    }
    @Test void unidirectionalOpenAcceptAndAsyncFinShareCreditAndEof() throws Exception {
        try (var loops = EventLoops.create(2); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var incoming = pair.server().acceptReceiveStreamAsync();
            var sender = get(pair.client().openSendStreamAsync(Preamble.EMPTY));
            var written = sender.writeAsync(ByteBuffer.wrap(new byte[] {3, 4, 5}));
            var fin = sender.finishAsync();
            var receiver = get(incoming);
            assertArrayEquals(new byte[] {3, 4, 5}, read(receiver)); get(written); get(fin);
            sender.close(); receiver.close();
        }
    }
    @Test void cancelledAcceptAndCreditWaitDoNotConsumeAnotherStreamsSlot() throws Exception {
        try (var loops = EventLoops.create(2); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).limits(Limits.defaults().withIncomingStreams(4)).build())) {
            var accept = pair.server().acceptStreamAsync(); assertTrue(accept.cancel(true));
            var held = new ArrayList<ZmuxStream>();
            for (int i = 0; i < 4; i++) held.add(get(pair.client().openStreamAsync()));
            var first = held.getFirst();
            var waiting = pair.client().openStreamAsync(); assertTrue(waiting.cancel(true));
            assertTrue(waiting.isCancelled()); first.close();
            var next = get(pair.client().openStreamAsync());
            get(next.writeAsync(ByteBuffer.wrap(new byte[] {8})));
            var receiver = get(pair.server().acceptStreamAsync());
            ByteBuffer bytes = ByteBuffer.allocate(1); assertEquals(1, get(receiver.readAsync(bytes)));
            assertEquals(8, bytes.get(0)); next.close(); receiver.close();
            for (var stream : held) stream.close();
        }
    }
    @Test void concurrentCancellationAcknowledgementsDoNotLoseWakeupsOrConsumeData() throws Exception {
        try (var loops = EventLoops.create(2); var workers = Executors.newVirtualThreadPerTaskExecutor();
             var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var sender = pair.client().openStream(); sender.id();
            var receiver = pair.server().acceptStream(); ByteBuffer abandoned = ByteBuffer.allocate(8);
            var pending = receiver.readAsync(abandoned);
            receiver.session.run(op -> true);
            var entered = new CountDownLatch(1); var proceed = new CountDownLatch(1);
            receiver.session.driver.executeCarrier(() -> { entered.countDown(); latch(proceed); });
            latch(entered);
            var callers = new ArrayList<Future<Boolean>>(); var started = new CountDownLatch(8);
            try {
                for (int i = 0; i < 8; i++) callers.add(workers.submit(() -> { started.countDown(); return pending.cancel(true); }));
                latch(started);
            } finally { proceed.countDown(); }
            boolean accepted = false;
            for (var caller : callers) accepted |= get(caller);
            assertTrue(accepted, "at least one cancelling caller must win");
            assertTrue(pending.isDone()); assertTrue(pending.isCancelled()); assertEquals(0, abandoned.position());
            get(sender.writeAsync(ByteBuffer.wrap(new byte[] {7})));
            var received = ByteBuffer.allocate(1); assertEquals(1, get(receiver.readAsync(received)));
            assertEquals(7, received.get(0)); assertEquals(0, abandoned.position()); sender.close(); receiver.close();
        }
    }
    @Test void decidedOutcomeRefusesCancellationButACallerCompletionStillWins() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var entered = new CountDownLatch(1); var proceed = new CountDownLatch(1);
            var result = pair.client().async(op -> 42, null, null, Long.MAX_VALUE, raw -> {
                assertFalse(io.zmux.internal.loop.SessionHost.onHostThread()); entered.countDown(); latch(proceed); return (Integer) raw;
            });
            try {
                latch(entered); assertFalse(result.cancel(true)); assertFalse(result.isCancelled());
                assertTrue(result.complete(99)); // CompletableFuture contract: the caller's completion wins.
            } finally { proceed.countDown(); }
            assertEquals(99, get(result));
        }
    }
    @Test void aReadQueuedBehindAnotherTimesOutWithoutTouchingItsBuffer() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var sender = pair.client().openStream(); sender.id();
            var receiver = pair.server().acceptStream();
            var first = receiver.readAsync(ByteBuffer.allocate(1));
            receiver.setReadTimeout(Duration.ofMillis(100));
            var destination = ByteBuffer.allocate(1); var next = receiver.readAsync(destination);
            var error = assertThrows(ExecutionException.class, () -> get(next));
            assertInstanceOf(SocketTimeoutException.class, error.getCause()); assertEquals(0, destination.position());
            assertTrue(first.cancel(true));
            receiver.setReadTimeout(null); sender.write(new byte[] {3});
            var got = ByteBuffer.allocate(1); assertEquals(1, get(receiver.readAsync(got))); assertEquals(3, got.get(0));
        }
    }
    @Test void ownedChunkCancelledBehindAWriteIsReleasedOnceWithoutBeingRead() throws Exception {
        try (var loops = EventLoops.create(2); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var sender = pair.client().openStream(); sender.setSendBuffer(16384);
            var first = sender.writeAsync(ByteBuffer.allocate(4 << 20));
            var receiver = pair.server().acceptStream();
            var released = new AtomicInteger();
            var chunk = Chunk.of(ByteBuffer.wrap(new byte[] {5, 6}), released::incrementAndGet);
            var second = sender.writeChunkAsync(chunk);
            assertThrows(IllegalStateException.class, chunk::buffer);
            assertTrue(second.cancel(true)); // Never waits: the loop releases the chunk afterwards.
            ListenerResourcesTest.until(() -> released.get() == 1);
            assertTrue(first.cancel(true)); sender.close(); receiver.close(); assertEquals(1, released.get());
        }
    }
    @Test void aFailingWithdrawalCannotStrandTheRemainingPendingOperations() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var withdrawn = new AtomicInteger();
            var first = pair.client().async(op -> Operation.PENDING,
                    op -> { withdrawn.incrementAndGet(); throw new IllegalStateException("withdraw"); }, null, Long.MAX_VALUE, raw -> raw);
            var second = pair.client().acceptStreamAsync();
            pair.client().run(op -> true); pair.client().close();
            assertThrows(ExecutionException.class, () -> get(first));
            assertThrows(ExecutionException.class, () -> get(second));
            get(pair.client().closed().toCompletableFuture()); assertEquals(1, withdrawn.get());
        }
    }

    @Test void completionsNeverRunOnALoopAndADecidedOutcomeCannotBeCancelled() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var pending = pair.client().acceptStreamAsync();
            var callback = pending.handle((value, error) -> { assertFalse(io.zmux.internal.loop.SessionHost.onHostThread()); return true; });
            var opened = pair.server().openStream(); opened.write(new byte[] {1});
            assertTrue(get(callback)); assertFalse(pending.cancel(true)); assertFalse(pending.isCancelled());
        }
    }

    @Test void closingLoopWaitsForSynchronousAttemptBeforeReturningItsBuffer() throws Exception {
        try (var loops = EventLoops.create(1); var workers = Executors.newVirtualThreadPerTaskExecutor();
             var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var entered = new CountDownLatch(1); var proceed = new CountDownLatch(1); var returned = new CountDownLatch(1);
            var outcome = new CompletableFuture<Object>(); ByteBuffer buffer = ByteBuffer.allocate(1);
            Thread caller = Thread.ofVirtual().start(() -> {
                try {
                    try { outcome.complete(pair.client().call(op -> { entered.countDown(); latch(proceed); buffer.put((byte) 9); return 1; }, null, Long.MAX_VALUE)); }
                    catch (java.io.IOException error) { outcome.completeExceptionally(error); }
                } finally { returned.countDown(); }
            });
            latch(entered);
            var stopping = workers.submit(loops::close);
            try {
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (pair.client().driver.executeCarrier(() -> {}) && System.nanoTime() < end) Thread.sleep(1);
                caller.interrupt();
                assertFalse(returned.await(30, TimeUnit.MILLISECONDS), "caller released its buffer while owner was still executing");
            } finally { proceed.countDown(); }
            assertEquals(1, get(outcome)); assertEquals(1, buffer.position()); get(stopping); caller.join(2000);
            assertFalse(caller.isAlive());
        }
    }

    @Test void publicReadTimeoutLeavesNoLateAccessToTheBorrowedBuffer() throws Exception {
        try (var loops = EventLoops.create(2); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var sender = pair.client().openStream(); sender.id();
            var receiver = pair.server().acceptStream(); receiver.setReadTimeout(Duration.ofMillis(10));
            var destination = ByteBuffer.allocate(8); var timed = receiver.readAsync(destination);
            var failure = assertThrows(ExecutionException.class, () -> get(timed));
            assertInstanceOf(SocketTimeoutException.class, failure.getCause());
            assertFalse(timed.isCancelled()); assertEquals(0, destination.position());
            receiver.setReadTimeout(null); sender.write(new byte[] {11});
            var next = ByteBuffer.allocate(1); assertEquals(1, get(receiver.readAsync(next)));
            assertEquals(11, next.get(0)); assertEquals(0, destination.position()); sender.close(); receiver.close();
        }
    }

    @Test void readOnlyDestinationValidationIsSharedWithoutFailingTheSession() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var sender = pair.client().openStream(); sender.id(); var receiver = pair.server().acceptStream();
            var invalid = ByteBuffer.allocate(1).asReadOnlyBuffer();
            assertThrows(java.nio.ReadOnlyBufferException.class, () -> receiver.readAsync(invalid));
            assertThrows(java.nio.ReadOnlyBufferException.class, () -> receiver.read(invalid));
            assertEquals(0, receiver.read(ByteBuffer.allocate(0).asReadOnlyBuffer()));
            assertEquals(0, get(receiver.readAsync(ByteBuffer.allocate(0).asReadOnlyBuffer())));
            sender.write(new byte[] {4}); var valid = ByteBuffer.allocate(1);
            assertEquals(1, get(receiver.readAsync(valid))); assertEquals(4, valid.get(0)); sender.close(); receiver.close();
        }
    }

    @Test void submissionAfterLoopClosureReturnsOwnedChunksWithoutRegisteringWaiters() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var sender = pair.client().openStream(); var released = new AtomicInteger();
            loops.close();
            var chunk = Chunk.of(ByteBuffer.wrap(new byte[] {2}), released::incrementAndGet);
            var write = sender.writeChunkAsync(chunk);
            assertThrows(ExecutionException.class, () -> get(write)); assertEquals(1, released.get());
            assertThrows(IllegalStateException.class, chunk::buffer);
            assertThrows(ExecutionException.class, () -> get(pair.client().acceptStreamAsync()));
            assertEquals(0, loops.outstandingBuffers()); sender.close();
        }
    }
    @Test void writeOwnerNeverBlocksTryWriteAndBoundsShutdownOutput() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var sender = pair.client().openStream();
            var pending = sender.writeAsync(ByteBuffer.allocate(256 << 20)); // Far beyond credit: stays in progress.
            long started = System.nanoTime();
            assertEquals(0, sender.tryWrite(ByteBuffer.allocate(1)));
            sender.setWriteTimeout(Duration.ofMillis(100));
            assertThrows(SocketTimeoutException.class, sender::finish);
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5));
            assertFalse(pending.isDone());
            sender.close();
            assertThrows(ExecutionException.class, () -> get(pending));
        }
    }
    @Test void completionsOfOneStreamArriveInOrderAndCancellationNeverWaitsForTheLoop() throws Exception {
        try (var loops = EventLoops.create(1); var pair = Pair.memory(ZmuxConfig.builder().eventLoops(loops).build())) {
            var sender = pair.client().openStream();
            var order = new java.util.concurrent.ConcurrentLinkedQueue<Integer>();
            var writes = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < 200; i++) { int n = i; writes.add(sender.writeAsync(ByteBuffer.allocate(64)).thenRun(() -> order.add(n))); }
            for (var write : writes) get(write);
            assertEquals(java.util.stream.IntStream.range(0, 200).boxed().toList(), new ArrayList<>(order));
            var receiver = pair.server().acceptStream();
            var pending = pair.server().acceptStreamAsync(); pair.server().run(op -> true);
            var entered = new CountDownLatch(1); var proceed = new CountDownLatch(1);
            pair.server().driver.executeCarrier(() -> { entered.countDown(); latch(proceed); });
            try { latch(entered); assertTrue(pending.cancel(true)); assertTrue(pending.isCancelled()); }
            finally { proceed.countDown(); }
            sender.close(); receiver.close();
        }
    }
}
