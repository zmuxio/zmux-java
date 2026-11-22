package io.zmux.http3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.Channel;
import io.zmux.Chunk;
import io.zmux.ZmuxSession;
import io.zmux.adapter.Handled;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.http.HttpCarrierClient;
import io.zmux.http.internal.HttpTransport;
import io.zmux.nettytest.TestTls;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

final class H3TestSupport {
    private H3TestSupport() {}

    static KeyManagerFactory kmf() throws Exception { return TestTls.kmf(); }
    static TrustManagerFactory tmf() throws Exception { return TestTls.tmf(); }

    static void exchange(ZmuxSession from, ZmuxSession to, int size) throws Exception {
        byte[] data = new byte[size];
        new Random(size).nextBytes(data);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor(); var stream = from.openStream()) {
            var written = executor.submit(() -> { stream.write(data); stream.finish(); return null; });
            try (var in = to.acceptStream(Duration.ofSeconds(10))) {
                assertNotNull(in);
                assertArrayEquals(data, in.readAll(1 << 24));
            }
            written.get(10, TimeUnit.SECONDS);
        }
    }

    static BlockingQueue<String> reading(PipeReader reader) {
        var events = new LinkedBlockingQueue<String>();
        reader.start(new PipeReader.Sink() {
            @Override public void onChunk(Chunk chunk, Runnable consumed) {
                var buf = chunk.buffer();
                byte[] data = new byte[buf.remaining()];
                buf.get(data);
                chunk.close();
                consumed.run();
                events.add(new String(data, StandardCharsets.US_ASCII));
            }
            @Override public void onEnd() { events.add("end"); }
            @Override public void onError(Throwable cause) { events.add("error " + cause); }
        });
        return events;
    }

    static String take(BlockingQueue<String> events, int bytes) throws InterruptedException {
        var result = new StringBuilder();
        while (result.length() < bytes) {
            String text = events.poll(5, TimeUnit.SECONDS);
            assertNotNull(text, "only " + result);
            result.append(text);
        }
        return result.toString();
    }

    static void write(PipeWriter writer, String text) {
        var result = new CompletableFuture<Void>();
        writer.write(OutboundBatch.of(ByteBuffer.wrap(text.getBytes(StandardCharsets.US_ASCII))), new PipeWriter.Completion() {
            @Override public void completed() { result.complete(null); }
            @Override public void failed(Throwable cause) { result.completeExceptionally(cause); }
        });
        result.orTimeout(5, TimeUnit.SECONDS).join();
    }

    static void await(BooleanSupplier condition) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < limit, "condition not reached");
            Thread.sleep(2);
        }
    }

    // Tests run on the classpath. Observe the real pool without exposing raw requests on the public client API.
    static Handled currentSetup(Channel stream) {
        var route = stream.pipeline().get(HttpTransport.Route.class);
        if (route == null) return null;
        try {
            var field = route.getClass().getDeclaredField("h3Setup");
            field.setAccessible(true);
            return (Handled) field.get(route);
        } catch (ReflectiveOperationException failure) {
            throw new LinkageError("HTTP router setup field unavailable", failure);
        }
    }

    private static Object connections(HttpCarrierClient client) throws ReflectiveOperationException {
        var field = HttpCarrierClient.class.getDeclaredField("conns");
        field.setAccessible(true);
        return field.get(client);
    }

    static CompletableFuture<Channel> open(HttpCarrierClient client, URI uri) throws ReflectiveOperationException {
        Object pool = connections(client);
        var open = pool.getClass().getDeclaredMethod("open", URI.class, boolean.class, boolean.class);
        open.setAccessible(true);
        return ((CompletableFuture<?>) open.invoke(pool, uri, false, false)).thenApply(Channel.class::cast);
    }

    static int openConnections(HttpCarrierClient client) throws ReflectiveOperationException {
        Object pool = connections(client);
        var count = pool.getClass().getDeclaredMethod("open");
        count.setAccessible(true);
        return (int) count.invoke(pool);
    }
}
