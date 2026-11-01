package io.zmux.http;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

/**
 * A reverse proxy for tests (HTTP/1.1 on both sides, one upstream connection per request) that
 * behaves as common deployments in front of a server do: a reverse proxy with its default
 * configuration, a CDN that buffers whole bodies, a front that passes WebSocket only; and, for
 * the exchange carrier, one that loses, repeats, delays and replays requests and responses.
 */
final class TestProxy implements Closeable {
    /**
     * The behaviour of a proxy.
     *
     * @param bufferRequests reads every request body whole and forwards it with a Content-Length
     * @param bufferResponses reads every response body whole and forwards it with a Content-Length,
     *     unless honorAccel and the upstream sent X-Accel-Buffering: no
     * @param webSocket forwards WebSocket upgrades; otherwise Upgrade and Connection are dropped
     *     and the request goes up as a plain GET
     * @param webSocketOnly refuses every request but a WebSocket upgrade (403)
     * @param responseTimeoutMillis answers 504 when a buffered response has not arrived whole within it (0: none)
     * @param maxBody answers 413 for a request body above it (0: no limit)
     */
    record Profile(boolean bufferRequests, boolean bufferResponses, boolean honorAccel, boolean webSocket,
                   boolean webSocketOnly, long responseTimeoutMillis, long maxBody) {
        static final Profile PASS = new Profile(false, false, false, true, false, 0, 0);

        static Profile nginxDefaults() { return new Profile(true, true, true, false, false, 0, 1 << 20); }

        static Profile bufferingCdn(long timeoutMillis) { return new Profile(true, true, false, false, false, timeoutMillis, 0); }

        static Profile webSocketFront() { return new Profile(false, false, false, true, true, 0, 0); }
    }

    /**
     * Faults on buffered exchanges, each with its probability: the request is lost before it is
     * forwarded; the response is lost after it was forwarded; the request is forwarded twice;
     * the exchange is delayed (up to delayMillis); an earlier response is replayed instead.
     * respond, when not null, may answer a request body itself (non-null result) instead of
     * forwarding it.
     */
    record Faults(double dropRequest, double dropResponse, double duplicate, double delay, long delayMillis,
                  double replay, UnaryOperator<byte[]> respond) {
        static final Faults NONE = new Faults(0, 0, 0, 0, 0, 0, null);
    }

    private final URI upstream;
    private final Profile p;
    private final Faults f;
    private final ServerSocket server;
    private final Set<Socket> open = ConcurrentHashMap.newKeySet();
    private final java.util.Random random;
    private volatile byte[] lastResponse;
    private volatile boolean closed;
    final AtomicLong maxRequestBody = new AtomicLong(), maxResponseBody = new AtomicLong(), requests = new AtomicLong();
    final URI base;

    TestProxy(URI upstream, Profile p, Faults f, long seed) throws IOException {
        this.upstream = upstream;
        this.p = p;
        this.f = f;
        random = new java.util.Random(seed);
        server = new ServerSocket(0, 64, InetAddress.getLoopbackAddress());
        base = URI.create("http://127.0.0.1:" + server.getLocalPort() + upstream.getRawPath());
        Thread.ofVirtual().start(this::accept);
    }

    TestProxy(URI upstream, Profile p) throws IOException { this(upstream, p, Faults.NONE, 1); }

    private void accept() {
        while (!closed) {
            try {
                Socket s = server.accept();
                open.add(s);
                Thread.ofVirtual().start(() -> { try (s) { serve(s); } catch (IOException ignored) { /* Gone. */ } finally { open.remove(s); } });
            } catch (IOException e) {
                return;
            }
        }
    }

    private final Set<java.util.concurrent.atomic.AtomicBoolean> streaming = ConcurrentHashMap.newKeySet();

    /**
     * Swallows the rest of every response body streaming now, keeping its connections open (a
     * black hole toward the client); responses that start later pass.
     */
    void silenceResponses() { for (var s : streaming) s.set(true); }

    /** Closes the client connections, as a proxy that restarts does. */
    void dropConnections() {
        for (Socket s : open) { try { s.close(); } catch (IOException ignored) { /* Gone. */ } }
    }

    @Override public void close() {
        closed = true;
        try { server.close(); } catch (IOException ignored) { /* Gone. */ }
        dropConnections();
    }

    // ---- HTTP/1.1 ----

    record Head(String line, List<String[]> fields) {
        String get(String name) {
            for (String[] h : fields) if (h[0].equalsIgnoreCase(name)) return h[1];
            return null;
        }

        boolean has(String name, String token) {
            for (String[] h : fields) {
                if (!h[0].equalsIgnoreCase(name)) continue;
                for (String t : h[1].split(",")) if (t.strip().equalsIgnoreCase(token)) return true;
            }
            return false;
        }

        long length() {
            String v = get("Content-Length");
            return v == null ? -1 : Long.parseLong(v.strip());
        }

        boolean chunked() { return has("Transfer-Encoding", "chunked"); }

        Head without(String... names) {
            List<String[]> out = new ArrayList<>();
            outer:
            for (String[] h : fields) {
                for (String n : names) if (h[0].equalsIgnoreCase(n)) continue outer;
                out.add(h);
            }
            return new Head(line, out);
        }

        Head with(String name, String value) {
            List<String[]> out = new ArrayList<>(fields);
            out.add(new String[] {name, value});
            return new Head(line, out);
        }

        byte[] bytes() {
            StringBuilder b = new StringBuilder(line).append("\r\n");
            for (String[] h : fields) b.append(h[0]).append(": ").append(h[1]).append("\r\n");
            return b.append("\r\n").toString().getBytes(StandardCharsets.ISO_8859_1);
        }

        int status() { return Integer.parseInt(line.split(" ")[1]); }
    }

    static String line(InputStream in) throws IOException {
        StringBuilder b = new StringBuilder();
        for (int c; (c = in.read()) >= 0; ) {
            if (c == '\n') {
                int n = b.length();
                if (n > 0 && b.charAt(n - 1) == '\r') b.setLength(n - 1);
                return b.toString();
            }
            b.append((char) c);
        }
        return b.isEmpty() ? null : b.toString();
    }

    static Head head(InputStream in) throws IOException {
        String l = line(in);
        if (l == null) return null;
        List<String[]> fs = new ArrayList<>();
        for (String h; (h = line(in)) != null && !h.isEmpty(); ) {
            int i = h.indexOf(':');
            fs.add(new String[] {h.substring(0, i).strip(), h.substring(i + 1).strip()});
        }
        return new Head(l, fs);
    }

    /** Reads one chunk's data (null at the last chunk, after its trailers). */
    static byte[] chunk(InputStream in) throws IOException {
        String l = line(in);
        if (l == null) throw new IOException("truncated chunk");
        int n = Integer.parseInt(l.split(";")[0].strip(), 16);
        if (n == 0) {
            while (true) { String t = line(in); if (t == null || t.isEmpty()) return null; }
        }
        byte[] b = in.readNBytes(n);
        if (b.length != n) throw new IOException("truncated chunk");
        line(in);
        return b;
    }

    /** Reads a whole body: by Content-Length, chunked, or (untilClose) to the end of the connection. */
    static byte[] body(InputStream in, Head h, boolean untilClose, long max) throws IOException {
        if (h.chunked()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (byte[] c; (c = chunk(in)) != null; ) {
                out.write(c);
                if (max > 0 && out.size() > max) throw new IOException("too large");
            }
            return out.toByteArray();
        }
        long n = h.length();
        if (n >= 0) {
            byte[] b = in.readNBytes((int) n);
            if (b.length != n) throw new IOException("truncated body");
            return b;
        }
        return untilClose ? in.readAllBytes() : new byte[0];
    }

    private static final String[] HOP = {"Connection", "Keep-Alive", "Proxy-Connection", "TE", "Trailer", "Transfer-Encoding", "Upgrade"};

    private static void simple(OutputStream out, int status, String reason) throws IOException {
        out.write(("HTTP/1.1 " + status + " " + reason + "\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
        out.flush();
    }

    private Socket upstream() throws IOException {
        Socket u = new Socket();
        u.connect(new InetSocketAddress(upstream.getHost(), upstream.getPort()), 5000);
        open.add(u);
        return u;
    }

    private void serve(Socket c) throws IOException {
        InputStream in = new BufferedInputStream(c.getInputStream());
        OutputStream out = c.getOutputStream();
        while (true) {
            Head h = head(in);
            if (h == null) return;
            requests.incrementAndGet();
            boolean upgrade = h.has("Upgrade", "websocket");
            if (p.webSocketOnly && !upgrade) { simple(out, 403, "Forbidden"); return; }
            h = h.with("X-Forwarded-For", c.getInetAddress().getHostAddress());
            if (upgrade && p.webSocket) { tunnel(c, in, h); return; }
            if (!exchange(in, out, h.chunked(), h.without(HOP))) return;
        }
    }

    /** One request; false: the client connection closes. */
    private boolean exchange(InputStream in, OutputStream out, boolean chunked, Head h) throws IOException {
        long cl = h.length();
        if (p.maxBody > 0 && cl > p.maxBody) { simple(out, 413, "Too Large"); return false; }
        if (!p.bufferRequests && chunked) return stream(in, out, h);
        Head framing = chunked ? new Head(h.line, List.<String[]>of(new String[] {"Transfer-Encoding", "chunked"})) : h;
        byte[] body = chunked || cl > 0 ? body(in, framing, false, p.maxBody) : new byte[0];
        if (p.maxBody > 0 && body.length > p.maxBody) { simple(out, 413, "Too Large"); return false; }
        maxRequestBody.accumulateAndGet(body.length, Math::max);
        Head up = h.without("Content-Length").with("Content-Length", Integer.toString(body.length)).with("Connection", "close");
        if (f.respond != null) {
            byte[] own = f.respond.apply(body);
            if (own != null) { respond(out, "HTTP/1.1 200 OK", own); return true; }
        }
        if (roll(f.delay)) sleep(f.delayMillis);
        if (roll(f.dropRequest)) return false;
        if (roll(f.duplicate)) {
            try (Socket u = upstream()) {
                u.setSoTimeout(30_000);
                u.getOutputStream().write(up.bytes());
                u.getOutputStream().write(body);
                InputStream ui = new BufferedInputStream(u.getInputStream());
                Head rh = head(ui);
                if (rh != null) body(ui, rh, true, 0);
            } catch (IOException ignored) {
                // The first copy's answer is lost anyway.
            }
        }
        try (Socket u = upstream()) {
            if (p.responseTimeoutMillis > 0) u.setSoTimeout((int) p.responseTimeoutMillis);
            long t0 = System.nanoTime();
            OutputStream uo = u.getOutputStream();
            uo.write(up.bytes());
            uo.write(body);
            uo.flush();
            InputStream ui = new BufferedInputStream(u.getInputStream());
            Head rh;
            try {
                rh = head(ui);
                if (rh == null) { simple(out, 502, "Bad Gateway"); return false; }
                boolean buffer = p.bufferResponses && !(p.honorAccel && "no".equalsIgnoreCase(rh.get("X-Accel-Buffering")));
                if (!buffer) return streamResponse(ui, out, rh);
                byte[] rb = body(ui, rh, true, 0);
                if (p.responseTimeoutMillis > 0 && (System.nanoTime() - t0) / 1_000_000 > p.responseTimeoutMillis) {
                    simple(out, 504, "Gateway Timeout");
                    return false;
                }
                maxResponseBody.accumulateAndGet(rb.length, Math::max);
                if (roll(f.dropResponse)) return false;
                if (roll(f.replay) && lastResponse != null) rb = lastResponse;
                else if (rh.status() == 200) lastResponse = rb;
                respond(out, rh.line, rh.without(HOP).without("Content-Length", "X-Accel-Buffering"), rb);
                return true;
            } catch (SocketTimeoutException e) {
                simple(out, 504, "Gateway Timeout");
                return false;
            }
        } catch (IOException e) {
            simple(out, 502, "Bad Gateway");
            return false;
        }
    }

    private boolean roll(double prob) {
        if (prob <= 0) return false;
        synchronized (random) { return random.nextDouble() < prob; }
    }

    private void sleep(long maxMillis) {
        long d;
        synchronized (random) { d = (long) (random.nextDouble() * maxMillis); }
        try { Thread.sleep(d); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private void respond(OutputStream out, String line, byte[] body) throws IOException {
        respond(out, line, new Head(line, List.<String[]>of(new String[] {"Content-Type", "application/octet-stream"})), body);
    }

    private static void respond(OutputStream out, String line, Head h, byte[] body) throws IOException {
        Head x = new Head(line, h.fields).with("Content-Length", Integer.toString(body.length));
        out.write(x.bytes());
        out.write(body);
        out.flush();
    }

    /** Relays a response body as it arrives (chunked), then closes. */
    private boolean streamResponse(InputStream ui, OutputStream out, Head rh) throws IOException {
        Head x = rh.without(HOP).without("X-Accel-Buffering");
        boolean chunked = rh.chunked();
        if (chunked) x = x.without("Content-Length").with("Transfer-Encoding", "chunked");
        out.write(x.with("Connection", "close").bytes());
        out.flush();
        var silenced = new java.util.concurrent.atomic.AtomicBoolean();
        streaming.add(silenced);
        try {
            if (chunked) {
                for (byte[] c; (c = chunk(ui)) != null; ) {
                    if (silenced.get()) continue;
                    out.write((Integer.toHexString(c.length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
                    out.write(c);
                    out.write("\r\n".getBytes(StandardCharsets.ISO_8859_1));
                    out.flush();
                }
                if (silenced.get()) return false;
                out.write("0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
            } else {
                byte[] b = new byte[16384];
                for (int n; (n = ui.read(b)) >= 0; ) {
                    if (silenced.get()) continue;
                    out.write(b, 0, n);
                    out.flush();
                }
            }
            out.flush();
        } finally {
            streaming.remove(silenced);
        }
        return false;
    }

    /** Forwards a request whose body streams (chunked) while its response comes back, then closes. */
    private boolean stream(InputStream in, OutputStream out, Head h) throws IOException {
        Socket u = upstream();
        try (u) {
            OutputStream uo = u.getOutputStream();
            uo.write(h.with("Transfer-Encoding", "chunked").with("Connection", "close").bytes());
            uo.flush();
            Thread up = Thread.ofVirtual().start(() -> {
                try {
                    for (byte[] c; (c = chunk(in)) != null; ) {
                        uo.write((Integer.toHexString(c.length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
                        uo.write(c);
                        uo.write("\r\n".getBytes(StandardCharsets.ISO_8859_1));
                        uo.flush();
                    }
                    uo.write("0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
                    uo.flush();
                } catch (IOException ignored) {
                    // The exchange ends with either side.
                }
            });
            InputStream ui = new BufferedInputStream(u.getInputStream());
            Head rh = head(ui);
            if (rh != null) {
                boolean buffer = p.bufferResponses && !(p.honorAccel && "no".equalsIgnoreCase(rh.get("X-Accel-Buffering")));
                if (buffer) respond(out, rh.line, rh.without(HOP).without("Content-Length", "X-Accel-Buffering"), body(ui, rh, true, 0));
                else streamResponse(ui, out, rh);
            }
            up.interrupt();
        } finally {
            open.remove(u);
        }
        return false;
    }

    /** Forwards a WebSocket upgrade, then the bytes both ways. */
    private void tunnel(Socket c, InputStream in, Head h) throws IOException {
        Socket u = upstream();
        try (u) {
            u.getOutputStream().write(h.bytes());
            u.getOutputStream().flush();
            Thread t = Thread.ofVirtual().start(() -> {
                try { in.transferTo(u.getOutputStream()); } catch (IOException ignored) { /* Either side ended. */ }
                try { u.shutdownOutput(); } catch (IOException ignored) { /* Gone. */ }
            });
            try { u.getInputStream().transferTo(c.getOutputStream()); } catch (IOException ignored) { /* Either side ended. */ }
            c.close();
            t.interrupt();
        } finally {
            open.remove(u);
        }
    }

    @Override public String toString() { return "proxy " + base + " " + p.toString().toLowerCase(Locale.ROOT); }
}
