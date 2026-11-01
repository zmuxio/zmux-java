package io.zmux.shaping.machine;

import io.zmux.*;
import io.zmux.shaping.ShaperFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.concurrent.*;

/**
 * Shaped echo peer over TCP. {@code server}: prints {@code LISTEN host:port}, accepts one session,
 * echoes eight streams and waits for the peer to close. {@code client ADDR}: opens eight streams,
 * checks each echo, then closes. The definition named by {@code SHAPE_DEF} (when set and not empty)
 * shapes the local side; a shaped side must report padding (injections may merge into held or data records) and no
 * faults. Prints {@code STATS ...} and {@code OK}; exits non-zero on any failure.
 */
public final class ShapingPeer {
    static final int STREAMS = 8;
    private ShapingPeer() {}

    public static void main(String[] args) throws Exception {
        Thread.ofPlatform().daemon(true).start(() -> {
            try { Thread.sleep(Duration.ofSeconds(60)); } catch (InterruptedException e) { return; }
            System.err.println("deadline"); Runtime.getRuntime().halt(1);
        });
        try {
            var factory = factory(System.getenv("SHAPE_DEF"));
            try (var loops = EventLoops.create(2)) {
                var cfg = ZmuxConfig.builder().eventLoops(loops).build();
                if (args[0].equals("server")) {
                    try (var listener = Zmux.listen(new InetSocketAddress("127.0.0.1", 0), ListenerConfig.of(cfg))) {
                        var bound = listener.localAddress();
                        System.out.println("LISTEN " + bound.getHostString() + ":" + bound.getPort());
                        try (var session = listener.accept(Duration.ofSeconds(20))) {
                            if (session == null) throw new IOException("no session");
                            System.out.println(serve(session, factory));
                        }
                    }
                } else {
                    int split = args[1].lastIndexOf(':');
                    var remote = new InetSocketAddress(args[1].substring(0, split), Integer.parseInt(args[1].substring(split + 1)));
                    try (var session = Zmux.connect(remote, cfg)) { System.out.println(drive(session, factory)); }
                }
            }
            System.out.println("OK");
            System.exit(0);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    static ShaperFactory factory(String path) throws IOException, DefinitionException {
        if (path == null || path.isEmpty()) return null;
        return Machines.factory(Files.readString(Path.of(path)), Options.defaults());
    }

    static byte[] payload(int i) {
        byte[] b = new byte[200_000 + i * 7919];
        new SplittableRandom(i).nextBytes(b);
        return b;
    }

    /** Echoes {@link #STREAMS} streams, waits for the peer to close and returns the STATS line. */
    static String serve(ZmuxSession session, ShaperFactory factory) throws Exception {
        if (factory != null) session.setShaper(factory);
        try (var jobs = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Future<?>>();
            for (int n = 0; n < STREAMS; n++) {
                var stream = session.acceptStream(Duration.ofSeconds(20));
                tasks.add(jobs.submit(() -> {
                    try (stream) { stream.write(stream.readAll(1 << 24)); stream.finish(); }
                    return null;
                }));
            }
            for (var t : tasks) t.get(30, TimeUnit.SECONDS);
        }
        session.closed().get(30, TimeUnit.SECONDS);
        return report(session, factory != null, false);
    }

    /** Sends {@link #STREAMS} payloads, checks their echoes, closes and returns the STATS line. */
    static String drive(ZmuxSession session, ShaperFactory factory) throws Exception {
        if (factory != null) session.setShaper(factory);
        try (var jobs = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Future<?>>();
            for (int i = 0; i < STREAMS; i++) {
                int n = i;
                tasks.add(jobs.submit(() -> {
                    try (var stream = session.openStream()) {
                        byte[] p = payload(n);
                        var writing = jobs.submit(() -> { stream.write(p); stream.finish(); return null; });
                        byte[] b = stream.readAll(1 << 24);
                        writing.get(30, TimeUnit.SECONDS);
                        if (!Arrays.equals(b, p)) throw new IOException("stream " + n + ": " + b.length + " of " + p.length + " bytes");
                    }
                    return null;
                }));
            }
            for (var t : tasks) t.get(30, TimeUnit.SECONDS);
        }
        Thread.sleep(300); // injections run on a timer
        String line = report(session, factory != null, true);
        session.close();
        session.closed().get(5, TimeUnit.SECONDS);
        return line;
    }

    static String report(ZmuxSession session, boolean shaped, boolean client) throws IOException {
        var st = session.shapingStats();
        String line = "STATS padding=" + st.paddingBytes() + " injected=" + st.injected() + " dropped=" + st.injectDropped()
                + " clamped=" + st.clamped() + " faults=" + st.faults();
        if (shaped && (st.paddingBytes() == 0 || st.faults() != 0)) throw new IOException("not shaped: " + line);
        return line;
    }
}
