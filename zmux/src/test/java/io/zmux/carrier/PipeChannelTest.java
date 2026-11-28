package io.zmux.carrier;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class PipeChannelTest {
    @Test void closingAWriterIncludesItsActiveWorkerCallback() throws Exception {
        var channels = java.nio.channels.Pipe.open();
        try (var source = channels.source(); var selectable = channels.sink();
             var blocking = java.nio.channels.Channels.newChannel(new java.io.ByteArrayOutputStream())) {
            assertTrue(source.isOpen());
            for (var output : java.util.List.of(selectable, blocking)) {
                var halves = PipeChannel.pipe(null, output, Framing.BYTE_STREAM, 1);
                var entered = new java.util.concurrent.CountDownLatch(1);
                var release = new CompletableFuture<Void>();
                var retired = io.zmux.internal.spi.AdapterPorts.released(halves);
                halves.writer().write(OutboundBatch.of(ByteBuffer.allocate(1)), new PipeWriter.Completion() {
                    @Override public void completed() { entered.countDown(); release.join(); }
                    @Override public void failed(Throwable cause) { fail(cause); }
                });
                try {
                    assertTrue(entered.await(5, TimeUnit.SECONDS));
                    halves.writer().close();
                    assertFalse(retired.isDone());
                } finally { release.complete(null); }
                retired.get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test void unstartedBlockingHalvesRetireAndCannotStartARead() throws Exception {
        var channels = java.nio.channels.Pipe.open();
        try (var source = channels.source(); var sink = channels.sink()) {
            var halves = PipeChannel.pipe(source, sink, Framing.BYTE_STREAM, 1);
            var released = io.zmux.internal.spi.AdapterPorts.released(halves);
            halves.reader().close();
            halves.writer().close();
            released.get(5, TimeUnit.SECONDS);
            assertThrows(IllegalStateException.class, () -> halves.reader().start(null));
        }
    }

    /**
     * A socket's halves never wait on the virtual-thread poller: on Windows (JDK 25 and later) a
     * virtual thread waiting to write is not woken while another one waits to read the same
     * socket, which held a session's last write back until the session ended.
     */
    @Test void aSocketsHalvesRunOnPlatformThreads() throws Exception {
        try (var server = ServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
             SocketChannel ch = SocketChannel.open(server.getLocalAddress());
             SocketChannel peer = server.accept()) {
            PipeIo io = PipeChannel.pipe(ch, ch, Framing.BYTE_STREAM, 1 << 16);
            var readOn = new CompletableFuture<Thread>();
            io.reader().start(new PipeReader.Sink() {
                @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) { Runnable release = () -> { chunk.close(); consumed.run(); }; readOn.complete(Thread.currentThread()); release.run(); }
                @Override public void onEnd() { }
                @Override public void onError(Throwable cause) { readOn.completeExceptionally(cause); }
            });
            var wroteOn = new CompletableFuture<Thread>();
            // More than a send buffer, so the write waits for the peer to read.
            int n = 8 << 20;
            io.writer().write(OutboundBatch.of(ByteBuffer.allocate(n)), new PipeWriter.Completion() {
                @Override public void completed() { wroteOn.complete(Thread.currentThread()); }
                @Override public void failed(Throwable cause) { wroteOn.completeExceptionally(cause); }
            });
            ByteBuffer sink = ByteBuffer.allocate(1 << 16);
            for (long got = 0; got < n; got += peer.read(sink.clear())) { }
            peer.write(ByteBuffer.wrap(new byte[] {1}));
            Thread writer = wroteOn.get(10, TimeUnit.SECONDS), reader = readOn.get(10, TimeUnit.SECONDS);
            assertFalse(writer.isVirtual(), "write on " + writer);
            assertFalse(reader.isVirtual(), "read on " + reader);
            io.writer().close();
            io.reader().close();
            writer.join(5_000);
            reader.join(5_000);
            assertFalse(writer.isAlive(), "the write thread ends with its half");
            assertFalse(reader.isAlive(), "the read thread ends with its half");
        }
    }
    /** One channel without a half-close: closing the sending half leaves it open, so the input still reads. */
    @Test void aChannelWithoutHalfCloseStaysOpenForTheInput() throws Exception {
        try (var server = ServerSocketChannel.open().bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))) {
            SocketChannel ch = SocketChannel.open(server.getLocalAddress());
            java.nio.channels.ByteChannel whole = new java.nio.channels.ByteChannel() {
                @Override public int read(ByteBuffer dst) throws java.io.IOException { return ch.read(dst); }
                @Override public int write(ByteBuffer src) throws java.io.IOException { return ch.write(src); }
                @Override public boolean isOpen() { return ch.isOpen(); }
                @Override public void close() throws java.io.IOException { ch.close(); }
            };
            PipeIo io = PipeChannel.pipe(whole, whole, Framing.BYTE_STREAM, 1 << 16);
            SocketChannel peer = server.accept();
            var got = new java.util.concurrent.LinkedBlockingQueue<Object>();
            io.reader().start(new PipeReader.Sink() {
                @Override public void onChunk(io.zmux.Chunk chunk, Runnable consumed) { Runnable release = () -> { chunk.close(); consumed.run(); }; got.add(chunk.remaining()); release.run(); }
                @Override public void onEnd() { got.add("end"); }
                @Override public void onError(Throwable cause) { got.add(cause); }
            });
            io.writer().close();
            assertTrue(ch.isOpen(), "the sending half's close leaves the channel to the input");
            peer.write(ByteBuffer.wrap(new byte[5]));
            assertEquals(5, got.poll(5, TimeUnit.SECONDS), "the peer's last write still arrives");
            io.reader().close();
            assertFalse(ch.isOpen(), "the second half closes the channel");
            peer.close();
        }
    }
}
