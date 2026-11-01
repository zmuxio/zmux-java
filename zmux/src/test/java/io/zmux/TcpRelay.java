package io.zmux;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP relay standing for an intermediary on a carrier's path: it may end connections idle
 * for a limit, end connections at an age limit, swallow bytes while keeping connections open
 * (a black hole), refuse new connections, or cut every connection.
 */
final class TcpRelay implements AutoCloseable {
    private final ServerSocket acceptor = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Link> links = ConcurrentHashMap.newKeySet();
    /** End a connection after this many ms without a byte in either direction; 0: never. */
    volatile long idleLimitMs;
    /** End a connection this many ms after it was accepted; 0: never. */
    volatile long ageLimitMs;
    /** Accept and close new connections at once. */
    volatile boolean refuse;
    /** Swallow every byte, keeping connections open. */
    volatile boolean blackhole;
    final AtomicInteger accepted = new AtomicInteger(), idleCuts = new AtomicInteger(), ageCuts = new AtomicInteger();

    private final class Link {
        final Socket client, upstream;
        final long born = System.nanoTime();
        volatile long last = born;
        Link(Socket client, Socket upstream) { this.client = client; this.upstream = upstream; }
        void close() {
            links.remove(this);
            try { client.close(); } catch (IOException ignored) { }
            try { upstream.close(); } catch (IOException ignored) { }
        }
        void pump(InputStream in, OutputStream out) {
            byte[] b = new byte[16384];
            try {
                for (int n; (n = in.read(b)) >= 0;) {
                    if (blackhole) continue;
                    last = System.nanoTime();
                    out.write(b, 0, n);
                }
            } catch (IOException ignored) { }
            close();
        }
    }

    TcpRelay(InetSocketAddress target) throws IOException {
        workers.submit(() -> {
            while (!acceptor.isClosed()) {
                Socket c;
                try { c = acceptor.accept(); } catch (IOException closed) { return; }
                accepted.incrementAndGet();
                if (refuse) { try { c.close(); } catch (IOException ignored) { } continue; }
                workers.submit(() -> {
                    Socket u = new Socket();
                    try { u.connect(target, 2000); } catch (IOException e) { try { c.close(); u.close(); } catch (IOException ignored) { } return; }
                    var l = new Link(c, u); links.add(l);
                    workers.submit(() -> { try { l.pump(c.getInputStream(), u.getOutputStream()); } catch (IOException e) { l.close(); } });
                    try { l.pump(u.getInputStream(), c.getOutputStream()); } catch (IOException e) { l.close(); }
                });
            }
        });
        workers.submit(() -> {
            while (!acceptor.isClosed()) {
                try { Thread.sleep(20); } catch (InterruptedException e) { return; }
                long now = System.nanoTime();
                for (Link l : links) {
                    long idle = idleLimitMs, age = ageLimitMs;
                    if (idle > 0 && now - l.last > idle * 1_000_000) { idleCuts.incrementAndGet(); l.close(); }
                    else if (age > 0 && now - l.born > age * 1_000_000) { ageCuts.incrementAndGet(); l.close(); }
                }
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
