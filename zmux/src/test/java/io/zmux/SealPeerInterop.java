package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.seal.Peer;
import io.zmux.seal.Profile;
import io.zmux.seal.Seal;
import io.zmux.seal.ServerConfig;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Establish with another implementation's scripted peer, named by {@code ZMUX_SEAL_INTEROP_PEER}: {@code server PROFILE}
 * prints {@code LISTEN addr}, {@code client PROFILE addr} dials, both print {@code ESTABLISHED <profile>}; the server
 * echoes one stream and the client checks one. Both ends use {@link SealTest}'s Handle, pre-shared key and node key.
 * Explicit selection only: requires a real peer, never silently skips.
 */
@Timeout(120)
class SealPeerInterop {
    private static final Duration WAIT = Duration.ofSeconds(15);

    @ParameterizedTest
    @EnumSource(Profile.class)
    void javaInitiates(Profile profile) throws Exception {
        String binary = InteropProcess.required("ZMUX_SEAL_INTEROP_PEER");
        try (EventLoops loops = EventLoops.create(1); var child = new InteropProcess(binary, "server", Long.toString(profile.value()))) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            InetSocketAddress at = PeerInterop.address(child.expect("LISTEN "));
            var b = SealTest.client().allowedProfiles(profile);
            if (!profile.isPskBased()) b.psk(null, null);
            var p = SealTest.establish(SealTest.tcp(at), b.build());
            assertEquals(profile, Seal.peerOf(p).orElseThrow().profile());
            try (var session = Zmux.open(p, Role.CLIENT, cfg)) {
                assertEquals(Long.toString(profile.value()), child.expect("ESTABLISHED "));
                byte[] data = new byte[1 << 20];
                new java.util.Random(profile.value()).nextBytes(data);
                ZmuxStream st = session.openStream();
                st.write(data);
                st.finish();
                assertArrayEquals(data, st.inputStream().readAllBytes());
            }
            child.expect("CLOSED");
            child.awaitExit();
        }
    }

    @ParameterizedTest
    @EnumSource(Profile.class)
    void javaAccepts(Profile profile) throws Exception {
        String binary = InteropProcess.required("ZMUX_SEAL_INTEROP_PEER");
        try (EventLoops loops = EventLoops.create(1)) {
            ZmuxConfig cfg = ZmuxConfig.builder().eventLoops(loops).build();
            var server = ServerConfig.builder(SealTest.gate(null), SealTest.NODE).acceptedProfiles(Profile.values()).build();
            var lc = ListenerConfig.builder().session(cfg).inboundTransform(Seal.server(server)).build();
            try (var l = Zmux.listen(NetworkTest.any(), lc);
                 var child = new InteropProcess(binary, "client", Long.toString(profile.value()), l.localAddress().getHostString() + ":" + l.localAddress().getPort());
                 var s = l.accept(WAIT)) {
                assertNotNull(s);
                assertEquals(profile, ((Peer) s.admitted().orElseThrow()).profile());
                assertEquals(Long.toString(profile.value()), child.expect("ESTABLISHED "));
                ZmuxStream st = s.acceptStream();
                byte[] got = st.inputStream().readAllBytes();
                st.write(got);
                st.finish();
                assertEquals(1 << 20, got.length);
                assertEquals("", child.expect("OK"));
                child.awaitExit();
            }
        }
    }
}
