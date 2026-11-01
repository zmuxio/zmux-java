package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Explicit selection only: requires ZMUX_SHAPING_PEER, a process speaking the {@link ShapingPeer}
 * contract, and never silently skips. Every combination of shaped and unshaped sides runs in both
 * directions with each definition.
 */
@Timeout(90)
class ShapingInterop {
    static final String RECORDS = """
            {"version": 1, "machines": [
             {"states": [{"record": {"max_len": {"range": [200, 1400]}, "pad": {"range": [0, 64]}}}]},
             {"states": [{"enter": [{"op": "inject", "len": {"range": [50, 300]}, "after": {"const": 0.01}}], "next": [{"on": "injected", "to": 0}]}]}
            ]}""";
    static final String DELAYED = """
            {"version": 1, "machines": [
             {"states": [{"record": {"max_len": {"range": [200, 1400]}, "pad": {"range": [0, 64]}, "delay": {"range": [0, 0.002]}}}]},
             {"states": [{"enter": [{"op": "inject", "len": {"range": [50, 300]}, "after": {"const": 0.01}}], "next": [{"on": "injected", "to": 0}]}]}
            ]}""";

    @TempDir Path dir;

    static Stream<Arguments> cases() {
        var out = Stream.<Arguments>builder();
        for (String def : List.of("records", "delayed"))
            for (boolean javaServer : List.of(true, false))
                for (boolean javaShaped : List.of(false, true))
                    for (boolean peerShaped : List.of(false, true)) out.add(Arguments.of(def, javaServer, javaShaped, peerShaped));
        return out.build();
    }

    @ParameterizedTest(name = "{0} javaServer={1} javaShaped={2} peerShaped={3}")
    @MethodSource("cases")
    void shapedSession(String def, boolean javaServer, boolean javaShaped, boolean peerShaped) throws Exception {
        String binary = System.getenv("ZMUX_SHAPING_PEER");
        assertNotNull(binary, "ZMUX_SHAPING_PEER must name a shaping peer");
        Path file = dir.resolve(def + ".json");
        Files.writeString(file, def.equals("records") ? RECORDS : DELAYED);
        var factory = javaShaped ? ShapingPeer.factory(file.toString()) : null;
        try (var loops = EventLoops.create(2); var jobs = Executors.newVirtualThreadPerTaskExecutor()) {
            var cfg = ZmuxConfig.builder().eventLoops(loops).build();
            if (javaServer) {
                try (var listener = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), ListenerConfig.of(cfg))) {
                    var bound = listener.localAddress();
                    var peer = start(binary, peerShaped ? file : null, "client", bound.getHostString() + ":" + bound.getPort());
                    try (var session = listener.accept(Duration.ofSeconds(20))) {
                        assertNotNull(session, "no session");
                        var lines = jobs.submit(() -> drain(peer));
                        ShapingPeer.serve(session, factory);
                        finish(peer, lines);
                    } finally { peer.destroyForcibly(); }
                }
            } else {
                var peer = start(binary, peerShaped ? file : null, "server");
                try {
                    var reader = new BufferedReader(new InputStreamReader(peer.getInputStream(), StandardCharsets.UTF_8));
                    var listen = jobs.submit(() -> {
                        for (String line; (line = reader.readLine()) != null;) if (line.startsWith("LISTEN ")) return line.substring(7);
                        throw new IOException("peer ended before LISTEN");
                    });
                    String text = listen.get(20, TimeUnit.SECONDS);
                    int split = text.lastIndexOf(':');
                    var remote = new InetSocketAddress(text.substring(0, split), Integer.parseInt(text.substring(split + 1)));
                    var lines = jobs.submit(() -> drain(reader));
                    try (var session = Zmux.connect(remote, cfg)) { ShapingPeer.drive(session, factory); }
                    finish(peer, lines);
                } finally { peer.destroyForcibly(); }
            }
        }
    }

    private Process start(String binary, Path def, String... args) throws IOException {
        var command = new java.util.ArrayList<String>(); command.add(binary); command.addAll(List.of(args));
        var builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().put("SHAPE_DEF", def == null ? "" : def.toString());
        return builder.start();
    }

    private static String drain(Process p) throws IOException {
        return drain(new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8)));
    }

    private static String drain(BufferedReader reader) throws IOException {
        var out = new StringBuilder();
        for (String line; (line = reader.readLine()) != null;) out.append(line).append('\n');
        return out.toString();
    }

    private static void finish(Process peer, Future<String> lines) throws Exception {
        assertTrue(peer.waitFor(30, TimeUnit.SECONDS), "peer did not exit");
        String output = lines.get(5, TimeUnit.SECONDS);
        assertEquals(0, peer.exitValue(), output);
        assertTrue(output.contains("\nOK\n") || output.startsWith("OK\n"), output);
    }
}
