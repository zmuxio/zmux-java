package io.zmux;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/** Scripted test peer; command contract is shared with zmux-rs/interop/peer. No wire parser. */
public final class InteropPeer {
    private static final byte[] PSK = "zmux interop pre-shared key 0123".getBytes(StandardCharsets.US_ASCII);
    private InteropPeer() {}
    public static void main(String[] args) throws Exception {
        if (args.length < 1) throw new IllegalArgumentException("server|client|mp-server|mp-client [address]");
        Thread watchdog = Thread.ofPlatform().daemon(true).start(() -> {
            try { Thread.sleep(Duration.ofSeconds(90)); Runtime.getRuntime().halt(124); }
            catch (InterruptedException expected) { /* completed */ }
        });
        try { run(args); } finally { watchdog.interrupt(); }
    }
    private static InetSocketAddress address(String text) {
        int split = text.lastIndexOf(':');
        return new InetSocketAddress(text.substring(0, split), Integer.parseInt(text.substring(split + 1)));
    }
    private static void run(String[] args) throws Exception {
        boolean mp = args[0].startsWith("mp-");
        String mode = mp ? args[0].substring(3) : args[0];
        if (!List.of("server", "client").contains(mode)) throw new IllegalArgumentException("unknown mode");
        var remote = address(args.length > 1 ? args[1] : "127.0.0.1:0");
        try (var loops = EventLoops.create(2); var jobs = Executors.newVirtualThreadPerTaskExecutor()) {
            var builder = ZmuxConfig.builder().eventLoops(loops).datagrams(true).limits(Limits.defaults().withIncomingUniStreams(16));
            if (mp) builder.multipath(MultipathConfig.of(4, PipeAuth.psk(PSK)));
            var config = builder.build();
            try (var endpoint = mode.equals("server") ? Zmux.listen(remote, ListenerConfig.of(config)) : Zmux.listener(ListenerConfig.of(config))) {
                ZmuxSession session;
                if (mode.equals("server")) {
                    var bound = endpoint.localAddress(); System.out.println("LISTEN " + bound.getHostString() + ":" + bound.getPort());
                    session = endpoint.accept(Duration.ofSeconds(20));
                    if (session == null) throw new IOException("no session");
                } else {
                    session = endpoint.connect(remote);
                    if (mp) session.paths().add(remote, PipeOptions.defaults());
                }
                try (session) {
                    System.out.println("ESTABLISHED");
                    var failure = new AtomicReference<Throwable>();
                    submit(session, jobs, failure, () -> {
                        try { while (true) { var stream = session.acceptStream(); submit(session, jobs, failure, () -> handle(session, stream)); } }
                        catch (IOException closed) { if (!(closed instanceof SessionClosedException)) throw closed; }
                    });
                    submit(session, jobs, failure, () -> {
                        try {
                            while (true) {
                                var stream = session.acceptReceiveStream();
                                submit(session, jobs, failure, () -> {
                                    try (stream; var output = session.openSendStream(Preamble.EMPTY)) {
                                        output.write(read(stream)); output.finish();
                                    }
                                });
                            }
                        } catch (IOException closed) { if (!(closed instanceof SessionClosedException)) throw closed; }
                    });
                    submit(session, jobs, failure, () -> {
                        try { while (true) session.datagrams().send(ByteBuffer.wrap(session.datagrams().receive())); }
                        catch (IOException closed) { if (!(closed instanceof SessionClosedException)) throw closed; }
                    });
                    var error = session.awaitClosed(); System.out.println("CLOSED " + error.message());
                    jobs.shutdown();
                    if (!jobs.awaitTermination(5, TimeUnit.SECONDS)) throw new IOException("peer workers did not terminate");
                    if (failure.get() != null) throw new IOException("scripted peer failed", failure.get());
                }
            }
        }
    }
    private static byte[] read(ZmuxReceiveStream stream) throws IOException {
        var output = new ByteArrayOutputStream(); var bytes = ByteBuffer.allocate(65536);
        for (int n; (n = stream.read(bytes.clear())) >= 0;) output.write(bytes.array(), 0, n);
        return output.toByteArray();
    }
    private static void reply(ZmuxStream stream, String value) throws IOException {
        stream.write(value.getBytes(StandardCharsets.UTF_8)); stream.finish();
    }
    private static void handle(ZmuxSession session, ZmuxStream stream) throws Exception {
        try (stream) {
            String command = stream.preamble().openInfo() == null ? "" : new String(stream.preamble().openInfo(), StandardCharsets.UTF_8);
            String[] parts = command.split(":", 2); String arg = parts.length > 1 ? parts[1] : "";
            switch (parts[0]) {
                case "echo" -> {
                    var buffer = ByteBuffer.allocate(65536);
                    while (stream.read(buffer.clear()) >= 0) stream.write(buffer.flip());
                    stream.finish();
                }
                case "info" -> {
                    // Test adapter observes the core's already-decoded immutable opening metadata.
                    var preamble = stream.st.peerPreamble(); read(stream);
                    reply(stream, "info=" + command + ";prio=" + (preamble != null && preamble.hasPriority()
                            ? preamble.urgency() + "," + preamble.incremental() : "none"));
                }
                case "reset" -> {
                    String result;
                    try { read(stream); result = "fin"; }
                    catch (StreamResetException error) { result = "reset=" + error.code(); }
                    reply(stream, result);
                }
                case "stop" -> { stream.stop(Long.parseLong(arg)); reply(stream, "stopped"); }
                case "sendreset" -> { byte[] bytes = new byte[1000]; java.util.Arrays.fill(bytes, (byte) 7); stream.write(bytes); Thread.sleep(20); stream.reset(Long.parseLong(arg)); read(stream); }
                case "open-bidi" -> {
                    try (var back = session.openStream(Preamble.openInfo("back".getBytes(StandardCharsets.UTF_8)))) {
                        back.write(arg.getBytes(StandardCharsets.UTF_8)); back.finish();
                        reply(stream, "opened=" + new String(read(back), StandardCharsets.UTF_8));
                    }
                }
                case "open-uni" -> {
                    try (var back = session.openSendStream(Preamble.EMPTY)) { back.write(arg.getBytes(StandardCharsets.UTF_8)); back.finish(); }
                    reply(stream, "sent");
                }
                case "dgram" -> {
                    int count = Integer.parseInt(arg);
                    for (int i = 0; i < count; i++) { var value = stream.receiveDatagram(); stream.sendDatagram(value.context() + 1, ByteBuffer.wrap(value.payload())); }
                    read(stream); reply(stream, "dgram-done");
                }
                case "schedule" -> { if (!arg.equals("round-robin")) throw new IOException("unknown policy"); session.setScheduler(Scheduler.SPREAD_ROUND_ROBIN); reply(stream, "scheduled"); }
                case "drop-bootstrap" -> { session.paths().list().stream().filter(p -> p.id() == 0).findFirst().orElseThrow().abort(); reply(stream, "dropped"); }
                case "goaway" -> { session.run(op -> { session.core.shutdown(Long.parseLong(arg), null); return Boolean.TRUE; }); read(stream); reply(stream, "draining"); }
                case "close" -> {
                    String[] close = arg.split(":", 2); read(stream);
                    session.close(new ApplicationError(Long.parseLong(close[0]), close.length > 1 ? close[1] : ""));
                }
                default -> reply(stream, "unknown=" + command);
            }
        }
    }
    @FunctionalInterface private interface Work { void run() throws Exception; }
    private static void submit(ZmuxSession session, ExecutorService jobs, AtomicReference<Throwable> failure, Work work) {
        jobs.execute(() -> {
            try { work.run(); }
            catch (SessionClosedException expected) { /* pending operations end with the session */ }
            catch (Throwable error) {
                failure.compareAndSet(null, error);
                session.close(new ApplicationError(1, "scripted peer failed"));
            }
        });
    }
}
