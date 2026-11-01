package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ExecutionException;
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
    private void run(String environment, boolean initiating) throws Exception {
        String binary = InteropProcess.required(environment);
        try (var loops = EventLoops.create(2)) {
            // Offer zmux extensions: a plain peer must disable them through negotiation.
            var config = ZmuxConfig.builder().eventLoops(loops).datagrams(true).limits(Limits.defaults().withIncomingUniStreams(16)).multipath(MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.psk(PeerInterop.PSK))).build();
            try (var endpoint = initiating ? Zmux.listener(ListenerConfig.of(config))
                    : Zmux.listen(new InetSocketAddress("127.0.0.1", 0), ListenerConfig.of(config));
                 var child = new InteropProcess(binary, initiating ? "server" : "client",
                         initiating ? "127.0.0.1:0" : endpoint.localAddress().getHostString() + ":" + endpoint.localAddress().getPort())) {
                var target = initiating ? PeerInterop.address(child.expect("LISTEN ")) : endpoint.localAddress();
                try (var session = initiating ? endpoint.connect(target) : endpoint.accept(Duration.ofSeconds(15))) {
                    assertNotNull(session); child.expect("ESTABLISHED");
                    var state = session.info();
                    assertTrue(state.established()); assertFalse(state.peerIsZmux());
                    assertFalse(state.multipath()); assertFalse(state.preamble()); assertFalse(state.priority());
                    assertFalse(state.pipeAck()); assertFalse(state.datagramFlows()); assertFalse(state.goAway());
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
                        assertEquals(9, assertThrows(StreamStoppedException.class, () -> stream.write(text("late"))).code());
                    }
                    try (var stream = session.openStream()) {
                        stream.write(text("reset:33\n")); stream.finish();
                        var failure = assertThrows(ExecutionException.class, () -> PeerInterop.read(stream));
                        assertEquals(33, assertInstanceOf(StreamResetException.class, failure.getCause()).code());
                    }
                    var incoming = session.acceptReceiveStreamAsync(Duration.ofSeconds(10));
                    try (var stream = session.openSendStream(Preamble.EMPTY)) { stream.write(text("uni")); stream.finish(); }
                    try (var stream = PeerInterop.get(incoming)) { assertArrayEquals(text("uni"), PeerInterop.read(stream)); }
                    assertTrue(state.maxDatagramPayload() > 0);
                    session.datagrams().send(ByteBuffer.wrap(text("datagram")));
                    assertArrayEquals(text("datagram"), session.datagrams().receive());
                    session.close(new ApplicationError(42, "bye"));
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
