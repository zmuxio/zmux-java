package io.zmux;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP relay standing for an intermediary on a carrier's path. It can drop traffic,
 * delay bytes or cut every connection.
 */
final class TcpRelay implements AutoCloseable {
    private final ServerSocket acceptor = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Link> links = ConcurrentHashMap.newKeySet();
    /** Swallow every byte, keeping connections open. */
    volatile boolean blackhole;
    /** Hold each byte this many ms in each direction; 0: forward at once. Read when a connection opens. */
    volatile long delayMs;
    final AtomicInteger accepted = new AtomicInteger();

    private final class Link {
        final Socket client, upstream;
        Link(Socket client, Socket upstream) { this.client = client; this.upstream = upstream; }
        void close() {
            links.remove(this);
            try { client.close(); } catch (IOException ignored) { }
            try { upstream.close(); } catch (IOException ignored) { }
        }
        void pump(InputStream in, OutputStream out) {
            long delay = delayMs;
            if (delay > 0) { delayedPump(in, out, delay * 1_000_000); return; }
            byte[] b = new byte[16384];
            try {
                for (int n; (n = in.read(b)) >= 0;) {
                    if (blackhole) continue;
                    out.write(b, 0, n);
                }
            } catch (IOException ignored) { }
            close();
        }
        private record Held(long at, byte[] bytes) { }
        void delayedPump(InputStream in, OutputStream out, long delayNanos) {
            var held = new LinkedBlockingQueue<Held>();
            workers.submit(() -> {
                try {
                    while (true) {
                        Held h = held.take();
                        if (h.bytes.length == 0) break;
                        long wait = h.at - System.nanoTime();
                        if (wait > 0) Thread.sleep(Duration.ofNanos(wait));
                        out.write(h.bytes);
                    }
                } catch (IOException | InterruptedException ignored) { }
                close();
            });
            byte[] b = new byte[16384];
            try {
                for (int n; (n = in.read(b)) >= 0;) {
                    held.add(new Held(System.nanoTime() + delayNanos, Arrays.copyOf(b, n)));
                }
            } catch (IOException ignored) { }
            held.add(new Held(0, new byte[0]));
        }
    }

    TcpRelay(InetSocketAddress target) throws IOException {
        workers.submit(() -> {
            while (!acceptor.isClosed()) {
                Socket c;
                try { c = acceptor.accept(); } catch (IOException closed) { return; }
                accepted.incrementAndGet();
                workers.submit(() -> {
                    Socket u = new Socket();
                    try { u.connect(target, 2000); } catch (IOException e) { try { c.close(); u.close(); } catch (IOException ignored) { } return; }
                    var l = new Link(c, u); links.add(l);
                    workers.submit(() -> { try { l.pump(c.getInputStream(), u.getOutputStream()); } catch (IOException e) { l.close(); } });
                    try { l.pump(u.getInputStream(), c.getOutputStream()); } catch (IOException e) { l.close(); }
                });
            }
        });
    }

    InetSocketAddress address() { return new InetSocketAddress(InetAddress.getLoopbackAddress(), acceptor.getLocalPort()); }

    /** Cuts every connection. */
    void cutAll() { for (Link l : links) l.close(); }

    int open() { return links.size(); }

    @Override public void close() throws IOException {
        acceptor.close(); cutAll(); workers.shutdownNow(); workers.close();
    }
}
