package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Explicitly selected tests against two independent, unmodified QMux libraries. */
@Timeout(90)
class QmuxInterop {
    private static byte[] text(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String command(ZmuxSession session, String command) throws Exception {
        try (var stream = session.openStream()) {
            stream.write(text(command + "\n")); stream.finish();
            return new String(PeerInterop.read(stream), StandardCharsets.UTF_8);
        }
    }
    /** The peer's STOP_SENDING and its reply travel independently: writes go on until it arrives. */
    private static StreamStoppedException stopped(ZmuxStream stream) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < end) {
            try { stream.write(text("late")); Thread.sleep(1); }
            catch (StreamStoppedException stopped) { return stopped; }
        }
        return fail("no STOP_SENDING arrived");
    }
    private void run(String environment, boolean initiating) throws Exception {
        String binary = InteropProcess.required(environment);
        try (var loops = EventLoops.create(2)) {
            // Offer zmux extensions: a plain peer must disable them through negotiation.
            var config = ZmuxConfig.builder().eventLoops(loops).datagrams(true).incomingUniStreams(16).maxPipes(4).pipeAuth(PipeAuth.psk(PeerInterop.PSK).withEndToEnd()).build();
            try (var endpoint = initiating ? ZmuxListener.of(ListenerConfig.of(config))
                    : Zmux.listen(new InetSocketAddress("127.0.0.1", 0), ListenerConfig.of(config));
                 var child = new InteropProcess(binary, initiating ? "server" : "client",
                         initiating ? "127.0.0.1:0" : endpoint.localAddress().getHostString() + ":" + endpoint.localAddress().getPort())) {
                var target = initiating ? PeerInterop.address(child.expect("LISTEN ")) : endpoint.localAddress();
                try (var session = initiating ? endpoint.connect(target) : endpoint.accept(Duration.ofSeconds(15))) {
                    assertNotNull(session); child.expect("ESTABLISHED");
                    var state = session.info();
                    assertTrue(state.established()); assertFalse(state.peerIsZmux());
                    assertFalse(state.multipath()); assertFalse(state.preamble()); assertFalse(state.priority());
                    assertFalse(state.pipeAck()); assertFalse(state.datagramFlows()); assertFalse(state.resume()); assertTrue(state.peerGoAway().isEmpty());
                    byte[] data = new byte[4 << 20]; new Random(7).nextBytes(data);
                    try (var stream = session.openStream()) {
                        stream.write(text("echo\n"));
                        var writing = stream.writeAsync(ByteBuffer.wrap(data)); var fin = stream.finishAsync();
                        assertArrayEquals(data, PeerInterop.read(stream)); PeerInterop.get(writing); PeerInterop.get(fin);
                    }
                    assertEquals("count=5", command(session, "count\n1234"));
                    try (var stream = session.openStream()) {
                        stream.write(text("stop:9\n"));
                        assertEquals("stopped", new String(PeerInterop.read(stream), StandardCharsets.UTF_8));
                        assertEquals(9, stopped(stream).code());
                    }
                    try (var stream = session.openStream()) {
                        stream.write(text("reset:33\n")); stream.finish();
                        var failure = assertThrows(ExecutionException.class, () -> PeerInterop.read(stream));
                        assertEquals(33, assertInstanceOf(StreamResetException.class, failure.getCause()).code());
                    }
                    var incoming = session.acceptReceiveStreamAsync().orTimeout(10, java.util.concurrent.TimeUnit.SECONDS);
                    try (var stream = session.openSendStream(Preamble.EMPTY)) { stream.write(text("uni")); stream.finish(); }
                    try (var stream = PeerInterop.get(incoming)) { assertArrayEquals(text("uni"), PeerInterop.read(stream)); }
                    assertTrue(state.maxDatagramPayload() > 0);
                    session.datagrams().send(ByteBuffer.wrap(text("datagram")));
                    assertArrayEquals(text("datagram"), session.datagrams().receive());
                    session.abort(42, "bye");
                    assertEquals("app 42 bye", child.expect("CLOSED ")); child.awaitExit();
                }
            }
            assertEquals(0, loops.outstandingBuffers());
        }
    }
    @Test void rustQmuxResponder() throws Exception { run("ZMUX_QMUX_RS_PEER", true); }
    @Test void rustQmuxInitiator() throws Exception { run("ZMUX_QMUX_RS_PEER", false); }
    @Test void goQmuxResponder() throws Exception { run("ZMUX_QMUX_GO_PEER", true); }
    @Test void goQmuxInitiator() throws Exception { run("ZMUX_QMUX_GO_PEER", false); }
}
