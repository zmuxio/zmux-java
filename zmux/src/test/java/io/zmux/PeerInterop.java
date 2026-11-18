package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.helper.PacketStream;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Explicit selection only: requires a real scripted peer, never silently skips. */
@Timeout(120)
class PeerInterop {
    static final byte[] PSK = "zmux interop pre-shared key 0123".getBytes(StandardCharsets.US_ASCII);
    static InetSocketAddress address(String text) {
        int split = text.lastIndexOf(':'); return new InetSocketAddress(text.substring(0, split), Integer.parseInt(text.substring(split + 1)));
    }
    static <T> T get(Future<T> task) throws Exception { return task.get(20, TimeUnit.SECONDS); }
    static byte[] read(ZmuxReceiveStream stream) throws Exception {
        var out = new ByteArrayOutputStream();
        for (Chunk chunk; (chunk = get(stream.readChunkAsync(65536))) != null;) {
            try (Chunk owned = chunk) { var view = owned.buffer(); byte[] bytes = new byte[view.remaining()]; view.get(bytes); out.write(bytes); }
        }
        return out.toByteArray();
    }
    static String command(ZmuxSession session, String command, byte[] body) throws Exception {
        try (var stream = get(session.openStreamAsync(Preamble.builder().openInfo(command.getBytes(StandardCharsets.UTF_8)).build()))) {
            var writing = stream.writeAsync(ByteBuffer.wrap(body)); var fin = stream.finishAsync();
            String result = new String(read(stream), StandardCharsets.UTF_8); get(writing); get(fin); return result;
        }
    }
    static void echo(ZmuxSession session, int size, int seed) throws Exception {
        byte[] bytes = new byte[size]; new Random(seed).nextBytes(bytes);
        try (var stream = get(session.openStreamAsync(Preamble.builder().openInfo("echo".getBytes(StandardCharsets.UTF_8)).build()))) {
            var writing = stream.writeAsync(ByteBuffer.wrap(bytes)); var fin = stream.finishAsync();
            assertArrayEquals(bytes, read(stream)); get(writing); get(fin);
        }
    }
    /** Packet streams (EXTENSIONS.md Section 12) against the peer's packets commands. */
    static void packets(ZmuxSession session) throws Exception {
        var options = new PacketStream.Options(PacketStream.SendPolicy.PREFER_DATAGRAM, 1 << 20);
        // Small packets as datagrams, two above the largest datagram as capsules, Context IDs 0 and 5.
        try (var stream = session.openStream(Preamble.builder().openInfo("packets".getBytes(StandardCharsets.UTF_8)).build())) {
            var packets = PacketStream.of(stream, options);
            int large = stream.maxDatagramPayload(5) + 100;
            var small = new HashSet<String>(); var big = new HashSet<String>();
            for (int i = 0; i < 8; i++) { long context = 5L * (i % 2); String payload = "small-" + i; small.add(context + "/" + payload); packets.send(context, ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8))); }
            for (int i = 0; i < 2; i++) { long context = 5L * i; String payload = String.valueOf((char) ('A' + i)).repeat(large + i); big.add(context + "/" + payload); packets.send(context, ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8))); }
            assertEquals(8, packets.stats().sentAsDatagrams()); assertEquals(2, packets.stats().sentAsCapsules());
            packets.finishAsync().get(10, TimeUnit.SECONDS);
            int smallBack = 0;
            for (StreamDatagram packet; (packet = packets.receive(Duration.ofSeconds(10))) != null;) {
                String key = packet.context() + "/" + new String(packet.payload(), StandardCharsets.UTF_8);
                if (big.remove(key)) continue;
                assertTrue(small.remove(key), "unexpected packet of " + packet.payload().length + " bytes");
                smallBack++;
            }
            // The carrier is reliable and the datagrams precede the FIN on both directions' wires, so every one comes back as a datagram.
            assertTrue(big.isEmpty(), "capsule packets lost"); assertEquals(8, smallBack, () -> "datagram packets lost: " + packets.stats());
            assertTrue(packets.stats().receivedAsDatagrams() == 8 && packets.stats().receivedAsCapsules() == 2, packets.stats().toString());
            packets.close();
        }
        // Capsules only, both ways, in order.
        try (var stream = session.openStream(Preamble.builder().openInfo("packets:stream".getBytes(StandardCharsets.UTF_8)).build())) {
            var packets = PacketStream.of(stream, new PacketStream.Options(PacketStream.SendPolicy.STREAM_ONLY, 1 << 20));
            int[] sizes = {0, 1, 100, 1500, 20000, 65535};
            for (int i = 0; i < sizes.length; i++) { byte[] bytes = new byte[sizes[i]]; Arrays.fill(bytes, (byte) i); packets.send(5L * (i % 2), ByteBuffer.wrap(bytes)); }
            packets.finishAsync().get(10, TimeUnit.SECONDS);
            for (int i = 0; i < sizes.length; i++) {
                var packet = packets.receive(Duration.ofSeconds(10)); assertNotNull(packet);
                byte[] bytes = new byte[sizes[i]]; Arrays.fill(bytes, (byte) i);
                assertEquals(5L * (i % 2), packet.context()); assertArrayEquals(bytes, packet.payload());
            }
            assertNull(packets.receive(Duration.ofSeconds(10)));
            assertEquals(0, packets.stats().receivedAsDatagrams());
            packets.close();
        }
        // A truncated capsule then FIN: the peer ends the stream with ZMUX_PACKET_ERROR.
        try (var stream = session.openStream(Preamble.builder().openInfo("packets".getBytes(StandardCharsets.UTF_8)).build())) {
            stream.write(new byte[] {0x00, 0x0a, 0x00, (byte) 0xab}); stream.finish();
            var failure = assertThrows(ExecutionException.class, () -> read(stream));
            assertEquals(PacketStream.PACKET_ERROR, assertInstanceOf(StreamResetException.class, failure.getCause()).code());
        }
        // The peer writes one: this side ends the stream, and the peer reports the code it saw.
        var report = session.acceptReceiveStreamAsync().orTimeout(10, TimeUnit.SECONDS);
        try (var stream = session.openStream(Preamble.builder().openInfo("packets-bad".getBytes(StandardCharsets.UTF_8)).build())) {
            stream.id(); var packets = PacketStream.of(stream, options);
            assertThrows(PacketErrorException.class, () -> packets.receive(Duration.ofSeconds(10)));
            try (var back = get(report)) { assertEquals("packet-error=" + PacketStream.PACKET_ERROR, new String(read(back), StandardCharsets.UTF_8)); }
            packets.close();
        }
    }
    private static void run(boolean initiating, boolean multipath) throws Exception {
        String binary = InteropProcess.required("ZMUX_INTEROP_PEER");
        try (var loops = EventLoops.create(2); var jobs = Executors.newVirtualThreadPerTaskExecutor()) {
            var builder = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).maxIncomingUniStreams(16);
            // The peer joins in Trusted mode, which an end admits only when told to.
            if (multipath) builder.maxPipes(4).pipeAuth(PipeAuth.psk(PSK).withEndToEnd()).allowRecordMode((carrier, mode, peer) -> true);
            var config = builder.build();
            try (var endpoint = initiating ? Zmux.listener(ListenerConfig.of(config)) : Zmux.listen(address("127.0.0.1:0"), ListenerConfig.of(config));
                 var child = initiating ? new InteropProcess(binary, multipath ? "mp-server" : "server", "127.0.0.1:0")
                         : new InteropProcess(binary, multipath ? "mp-client" : "client", endpoint.localAddress().getHostString() + ":" + endpoint.localAddress().getPort())) {
                var target = initiating ? address(child.expect("LISTEN ")) : endpoint.localAddress();
                try (var session = initiating ? endpoint.connect(target) : endpoint.accept(Duration.ofSeconds(15))) {
                    assertNotNull(session); child.expect("ESTABLISHED");
                    assertEquals(multipath, session.info().multipath());
                    if (multipath) {
                        if (initiating) session.addPipe(target, PipeOptions.defaults());
                        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while ((session.pipeStats().size() < 2 || session.pipeStats().stream().anyMatch(p -> !p.eligible())) && System.nanoTime() < end) Thread.sleep(2);
                        assertTrue(session.pipeStats().size() >= 2 && session.pipeStats().stream().allMatch(p -> p.eligible()), session.pipeStats().toString());
                        session.setScheduler(Scheduler.SPREAD_ROUND_ROBIN);
                        var before = session.pipeStats(); echo(session, 16 << 20, 1);
                        for (var pipe : session.pipeStats()) {
                            long prior = before.stream().filter(p -> p.pipe().value() == pipe.pipe().value()).findFirst().orElseThrow().bytesSent();
                            assertTrue(pipe.bytesSent() - prior >= 1 << 20, "pipe did not carry spread data: " + pipe);
                        }
                        assertEquals("scheduled", command(session, "schedule:round-robin", new byte[0]));
                        var inbound = session.pipeStats(); echo(session, 8 << 20, 2);
                        for (var pipe : session.pipeStats()) {
                            long prior = inbound.stream().filter(p -> p.pipe().value() == pipe.pipe().value()).findFirst().orElseThrow().bytesReceived();
                            assertTrue(pipe.bytesReceived() - prior >= 1 << 20, "reverse pipe did not carry spread data: " + pipe);
                        }
                        assertEquals("dropped", command(session, "drop-bootstrap", new byte[0]));
                        echo(session, 4 << 20, 3);
                        assertFalse(session.closed().toCompletableFuture().isDone());
                    }
                    var bulk = new ArrayList<Future<?>>();
                    try {
                        for (int i = 0; i < 4; i++) { int seed = i; bulk.add(jobs.submit(() -> { echo(session, 2 << 20, seed); return null; })); }
                        for (var task : bulk) get(task);
                    } finally { for (var task : bulk) if (!task.isDone()) task.cancel(true); }
                    try (var stream = session.openStream(Preamble.builder().openInfo("info".getBytes(StandardCharsets.UTF_8)).build())) {
                        stream.setPeerPriority(new Priority(5, true)); stream.finish();
                        assertEquals("info=info;prio=5,true", new String(read(stream), StandardCharsets.UTF_8));
                    }
                    try (var stream = session.openStream(Preamble.builder().openInfo("reset".getBytes(StandardCharsets.UTF_8)).build())) {
                        stream.write(new byte[] {1}); stream.markReliableBoundary(); stream.reset(77);
                        assertEquals("reset=77", new String(read(stream), StandardCharsets.UTF_8));
                    }
                    try (var stream = session.openStream(Preamble.builder().openInfo("stop:9".getBytes(StandardCharsets.UTF_8)).build())) {
                        stream.id(); assertEquals("stopped", new String(read(stream), StandardCharsets.UTF_8));
                        var stopped = assertThrows(StreamStoppedException.class, () -> stream.write(new byte[] {1})); assertEquals(9, stopped.code());
                    }
                    try (var stream = session.openStream(Preamble.builder().openInfo("sendreset:33".getBytes(StandardCharsets.UTF_8)).build())) {
                        stream.finish(); var failure = assertThrows(ExecutionException.class, () -> read(stream));
                        var reset = assertInstanceOf(StreamResetException.class, failure.getCause()); assertEquals(33, reset.code());
                    }
                    var back = jobs.submit(() -> {
                        try (var stream = get(session.acceptStreamAsync().orTimeout(10, java.util.concurrent.TimeUnit.SECONDS))) {
                            assertEquals("back", new String(stream.preamble().openInfo(), StandardCharsets.UTF_8));
                            byte[] bytes = read(stream); stream.write(("back:" + new String(bytes, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8)); stream.finish();
                        }
                        return null;
                    });
                    try { assertEquals("opened=back:hello", command(session, "open-bidi:hello", new byte[0])); get(back); }
                    finally { if (!back.isDone()) back.cancel(true); }
                    var uni = session.acceptReceiveStreamAsync().orTimeout(10, java.util.concurrent.TimeUnit.SECONDS);
                    assertEquals("sent", command(session, "open-uni:uni", new byte[0]));
                    try (var stream = get(uni)) { assertArrayEquals("uni".getBytes(StandardCharsets.UTF_8), read(stream)); }
                    var returned = session.acceptReceiveStreamAsync().orTimeout(10, java.util.concurrent.TimeUnit.SECONDS);
                    try (var stream = get(session.openSendStreamAsync(Preamble.EMPTY))) { get(stream.writeAsync(ByteBuffer.wrap(new byte[] {4, 5}))); get(stream.finishAsync()); }
                    try (var stream = get(returned)) { assertArrayEquals(new byte[] {4, 5}, read(stream)); }
                    session.datagrams().send(ByteBuffer.wrap(new byte[] {3})); assertArrayEquals(new byte[] {3}, session.datagrams().receive());
                    try (var stream = session.openStream(Preamble.builder().openInfo("dgram:1".getBytes(StandardCharsets.UTF_8)).build())) {
                        stream.id(); stream.sendDatagram(4, ByteBuffer.wrap(new byte[] {7}));
                        var value = stream.receiveDatagram(); assertEquals(5, value.context()); assertArrayEquals(new byte[] {7}, value.payload());
                        stream.finish(); assertEquals("dgram-done", new String(read(stream), StandardCharsets.UTF_8));
                    }
                    packets(session);
                    try (var stream = session.openStream(Preamble.builder().openInfo("close:42:bye".getBytes(StandardCharsets.UTF_8)).build())) {
                        stream.finish();
                    }
                    var closed = assertInstanceOf(SessionError.ApplicationClosed.class, session.awaitClosed());
                    assertEquals(42, closed.code()); assertEquals("bye", closed.reason()); assertTrue(closed.remote());
                    child.expect("CLOSED"); child.awaitExit();
                }
            }
            assertEquals(0, loops.outstandingBuffers(), () -> io.zmux.internal.diag.Diagnostics.outstandingTrace(loops));
        }
    }
    @Test void javaInitiates() throws Exception { run(true, false); }
    @Test void javaAccepts() throws Exception { run(false, false); }
    @Test void javaInitiatesMultipath() throws Exception { run(true, true); }
    @Test void javaAcceptsMultipath() throws Exception { run(false, true); }
}
