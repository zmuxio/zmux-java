package io.zmux.internal;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

final class ByteArrayQueueTest {
    private static void setLongField(ByteArrayQueue queue, String name, long value) throws Exception {
        Field field = ByteArrayQueue.class.getDeclaredField(name);
        field.setAccessible(true);
        field.setLong(queue, value);
    }

    @Test
    void readsAcrossQueuedSlicesWithoutRepacking() {
        ByteArrayQueue queue = new ByteArrayQueue();
        byte[] source = "0123456789".getBytes(StandardCharsets.US_ASCII);

        queue.add(source, 2, 5);
        queue.add(source, 8, 2);

        byte[] first = new byte[3];
        assertEquals(3, queue.read(first, 0, first.length), "first read byte count mismatch");
        assertArrayEquals("234".getBytes(StandardCharsets.US_ASCII), first, "first read payload mismatch");

        byte[] second = new byte[4];
        assertEquals(4, queue.read(second, 0, second.length), "second read byte count mismatch");
        assertArrayEquals("5689".getBytes(StandardCharsets.US_ASCII), second, "second read payload mismatch");
        assertTrue(queue.isEmpty(), "queue should be empty after consuming both slices");
    }

    @Test
    void discardAllReturnsQueuedSliceBytes() {
        ByteArrayQueue queue = new ByteArrayQueue();
        byte[] source = "abcdefgh".getBytes(StandardCharsets.US_ASCII);

        queue.add(source, 1, 3);
        queue.add(source, 5, 2);

        assertEquals(5, queue.discardAll(), "discardAll should report queued slice bytes");
        assertEquals(0, queue.size(), "discardAll should empty the queue");
    }

    @Test
    void readValidatesDestinationSliceBounds() {
        ByteArrayQueue queue = new ByteArrayQueue();

        assertThrows(IndexOutOfBoundsException.class, () -> queue.read(new byte[4], 3, 2));
    }

    @Test
    void retainedSliceOnlyReleasesBackingWhenChunkIsFullyConsumed() {
        ByteArrayQueue queue = new ByteArrayQueue();
        AtomicInteger releases = new AtomicInteger();
        byte[] source = "abcdefgh".getBytes(StandardCharsets.US_ASCII);

        queue.addRetained(source, 2, 3, source.length, releases::incrementAndGet);

        byte[] first = new byte[2];
        ByteArrayQueue.ReadResult firstRead = queue.readDetailed(first, 0, first.length);
        assertArrayEquals("cd".getBytes(StandardCharsets.US_ASCII), first, "partial read payload mismatch");
        assertEquals(2, firstRead.bytes(), "partial read byte count mismatch");
        assertEquals(0, firstRead.releasedStorageBytes(), "partial read must not release retained backing early");
        assertEquals(source.length, queue.storageBytes(), "queue should keep the retained backing pinned until the chunk is drained");
        assertEquals(0, releases.get(), "retained backing should not be released on a partial consume");

        byte[] second = new byte[2];
        ByteArrayQueue.ReadResult secondRead = queue.readDetailed(second, 0, second.length);
        assertArrayEquals("e".getBytes(StandardCharsets.US_ASCII), new byte[]{second[0]}, "tail read payload mismatch");
        assertEquals(1, secondRead.bytes(), "tail read byte count mismatch");
        assertEquals(source.length, secondRead.releasedStorageBytes(), "final drain should release the full retained backing size");
        assertEquals(0, queue.storageBytes(), "queue should release retained storage once the chunk is drained");
        assertEquals(1, releases.get(), "retained backing should be released exactly once");
    }

    @Test
    void partialReadShrinksTinyTailOutOfLargeRetainedBacking() {
        ByteArrayQueue queue = new ByteArrayQueue();
        AtomicInteger releases = new AtomicInteger();
        byte[] source = new byte[300 * 1024];
        int tailLength = 1024;
        source[source.length - tailLength] = 42;

        queue.addRetained(source, 0, source.length, source.length, releases::incrementAndGet);

        byte[] consumed = new byte[source.length - tailLength];
        ByteArrayQueue.ReadResult firstRead = queue.readDetailed(consumed, 0, consumed.length);
        assertEquals(consumed.length, firstRead.bytes(), "partial read byte count mismatch");
        assertEquals(source.length - tailLength, firstRead.releasedStorageBytes(), "partial read should release the oversized backing delta");
        assertEquals(tailLength, queue.sizeLong(), "logical tail length mismatch");
        assertEquals(tailLength, queue.storageBytes(), "queue should retain only the copied tail storage");
        assertEquals(1, releases.get(), "retained backing should be released after tail tightening");

        byte[] tail = new byte[tailLength];
        ByteArrayQueue.ReadResult secondRead = queue.readDetailed(tail, 0, tail.length);
        assertEquals(tailLength, secondRead.bytes(), "tail read byte count mismatch");
        assertEquals(tailLength, secondRead.releasedStorageBytes(), "final drain should release the copied tail storage");
        assertEquals(42, tail[0], "copied tail payload mismatch");
        assertEquals(1, releases.get(), "copied tail is heap-owned and must not release the original backing twice");
    }

    @Test
    void discardAllReleasesRetainedBackingStorage() {
        ByteArrayQueue queue = new ByteArrayQueue();
        AtomicInteger releases = new AtomicInteger();
        byte[] source = "abcdefgh".getBytes(StandardCharsets.US_ASCII);

        queue.addRetained(source, 1, 4, source.length, releases::incrementAndGet);

        ByteArrayQueue.DiscardResult discardResult = queue.discardAllDetailed();
        assertEquals(4, discardResult.bytes(), "discardAll should still report logical queued bytes");
        assertEquals(source.length, discardResult.releasedStorageBytes(), "discardAll should release the full retained backing size");
        assertEquals(0, queue.storageBytes(), "discardAll should drop retained storage accounting");
        assertEquals(1, releases.get(), "discardAll should release retained backings exactly once");
    }

    @Test
    void accountingUsesLongSaturationInsteadOfIntWrap() throws Exception {
        ByteArrayQueue queue = new ByteArrayQueue();
        long nearIntMax = Integer.MAX_VALUE - 1L;
        setLongField(queue, "size", nearIntMax);
        setLongField(queue, "storageBytes", nearIntMax);

        queue.addRetained(new byte[]{1, 2, 3, 4}, 0, 4, 4, null);

        assertEquals(Integer.MAX_VALUE + 3L, queue.sizeLong(), "logical queue size should not wrap at the int boundary");
        assertEquals(Integer.MAX_VALUE, queue.size(), "legacy int size view should saturate for Java read APIs");
        assertEquals(Integer.MAX_VALUE + 3L, queue.storageBytes(), "storage accounting should not wrap at the int boundary");
    }
}
