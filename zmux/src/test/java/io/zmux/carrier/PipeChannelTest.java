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
                @Override public void onData(ByteBuffer data, Runnable release) { readOn.complete(Thread.currentThread()); release.run(); }
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
}
