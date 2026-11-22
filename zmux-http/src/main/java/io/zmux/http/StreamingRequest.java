package io.zmux.http;

import io.zmux.http.internal.HttpTransport;

import io.zmux.internal.spi.Loops;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.zmux.Admission;
import io.zmux.CarrierKind;
import io.zmux.CarrierCredential;
import io.zmux.HttpRedirectException;
import io.zmux.HttpTarget;
import io.zmux.PipeOptions;
import io.zmux.RequestHead;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * One streaming carrier ({@link HttpCarrierClient#pipe}) and the redirects of its request. Until
 * the first response that is not a redirect the uplink bytes are kept, up to
 * {@link Redirects#MAX_REPLAY}; a redirect {@link Redirects#check} allows repeats the whole
 * request at the new URL on a new request, with a fresh credential, and sends the kept bytes
 * first, so that the server sees no pipe byte lost or twice (a redirecting front delivered
 * none). Past the bound nothing more is kept and a later redirect fails the carrier with an
 * {@link HttpRedirectException}, as does a refused one. zmux sees one reader and one writer
 * throughout. The state is guarded by this; each request's carrier runs on its channel's loop.
 */
final class StreamingRequest {
    private final HttpCarrierClient client;
    private final Map<String, List<String>> headers;
    private final boolean get, reads, writes;
    private final CarrierCredential credential;
    private final HttpTarget target;

    // Guarded by this.
    private URI url;
    private int hops;
    private BodyCarrier current;
    /** Installed only while a session tracks this outbound request as a carrier. */
    private List<Runnable> carrierWatchers;
    /** The connection whose TLS a bidirectional carrier reports; null for a split half. */
    private Channel boundConn;
    /** Increases with every request: a write's failure on an earlier one is not the carrier's. */
    private int generation;
    /** A redirect is under way: the next request is not ready yet. */
    private boolean redirecting;
    /** A response that is not a redirect arrived: nothing more is kept or followed. */
    private boolean settled;
    /** Copies of the uplink bytes written so far; null once beyond the bound or settled. */
    private ArrayList<ByteBuf> kept = new ArrayList<>();
    private long keptBytes;
    /** Writes made while redirecting, completed once the next request took them. */
    private final ArrayList<PipeWriter.Completion> deferred = new ArrayList<>(0);
    private PipeReader.Sink sink;
    private boolean readerClosed, writerClosed, failed;
    /** Why a redirect was refused: the carrier's error, reported by a write that fails after it. */
    private Throwable refusal;

    private StreamingRequest(HttpCarrierClient client, URI url, Map<String, List<String>> headers, PipeOptions.Direction dir,
                             CarrierCredential credential, HttpTarget target) {
        this.client = client;
        this.url = url;
        this.headers = headers;
        this.get = dir == PipeOptions.Direction.RECEIVE_ONLY;
        this.reads = dir != PipeOptions.Direction.SEND_ONLY;
        this.writes = !get;
        this.credential = credential;
        this.target = target;
        if (!writes) kept = null;
    }

    private Runnable watchCarrier(Runnable watcher) {
        synchronized (this) {
            if (carrierWatchers == null) carrierWatchers = new ArrayList<>();
            carrierWatchers.add(watcher);
        }
        return () -> {
            synchronized (this) {
                if (carrierWatchers == null) return;
                carrierWatchers.removeIf(registered -> registered == watcher);
                if (carrierWatchers.isEmpty()) carrierWatchers = null;
            }
        };
    }

    private void carrierChanged() {
        List<Runnable> watchers;
        synchronized (this) {
            if (carrierWatchers == null) return;
            watchers = List.copyOf(carrierWatchers);
        }
        for (var watcher : watchers) watcher.run();
    }

    /** Opens the carrier at url (target's URL when there is a target). */
    static PipeIo open(HttpCarrierClient client, URI url, Map<String, List<String>> headers, PipeOptions.Direction dir,
                       CarrierCredential credential, HttpTarget target) throws IOException {
        if (target != null) url = URI.create(target.url());
        StreamingRequest r = new StreamingRequest(client, url, headers, dir, credential, target);
        Channel ch = client.await(client.conns.open(url, false, false), "connect");
        try {
            // Under the lock: a redirect answered on the first request may already be replacing current.
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (r) {
                // A bidirectional carrier reports the TLS of this connection: a redirect may only continue it there.
                if (dir == PipeOptions.Direction.BIDIRECTIONAL) r.boundConn = Requests.connection(ch);
                r.current = r.request(ch, url, List.of());
            }
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
        PipeIo io = new PipeIo(r.reads ? r.new Reader() : null, r.writes ? r.new Writer() : null, Framing.BYTE_STREAM, "http " + dir,
                Requests.connection(ch));
        boolean h3 = Requests.h3(ch);
        if (dir != PipeOptions.Direction.BIDIRECTIONAL) {
            return (h3 ? Requests.pipe(io, ch) : io.withReceiveMemory(BodyCarrier.RECEIVE_MEMORY)).withCarrier(CarrierKind.SPLIT);
        }
        return Requests.pipe(io, ch).withCarrier(h3 ? CarrierKind.HTTP3 : CarrierKind.STREAMING);
    }

    /** Sends the request head on ch, then the credential's prefix and replay, and returns its carrier. */
    private BodyCarrier request(Channel ch, URI u, List<ByteBuf> replay) throws IOException {
        boolean h3 = Requests.h3(ch);
        var address = Connections.address(u);
        var info = new Admission.CarrierInfo(address, address, "", h3 ? u.getHost() : "",
                h3 ? Requests.exporter(ch) : null, RequestHead.of(Map.of()))
                .at(get ? Admission.Stage.HEAD : Admission.Stage.CARRIER, Framing.BYTE_STREAM, false);
        byte[] prefix = null;
        CarrierCredential.HeaderField field = null;
        if (credential != null) {
            // Computed afresh for every request: a fresh prefix token.
            if (get) field = credential.header(info);
            else prefix = credential.prefix(info);
        }
        boolean h1 = !Requests.multiplexed(ch);
        HttpRequest req = get ? new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, HttpCarrierClient.target(u), Unpooled.EMPTY_BUFFER)
                : new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, HttpCarrierClient.target(u));
        req.headers().set(HttpHeaderNames.HOST, HttpCarrierClient.authority(u));
        headers.forEach((k, vs) -> { for (String v : vs) req.headers().add(k, v); });
        if (field != null) req.headers().add(field.name(), field.value());
        if (!get) {
            req.headers().set(HttpHeaderNames.CONTENT_TYPE, Requests.OCTETS);
            if (h1) HttpUtil.setTransferEncodingChunked(req, true);
        } else {
            HttpUtil.setContentLength(req, 0);
        }
        BodyCarrier c = new BodyCarrier(ch, Framing.BYTE_STREAM, reads, writes, new BodyCarrier.Out() {
            @Override public Object content(ByteBuf data) { return new DefaultHttpContent(data); }
            @Override public Object end() { return new DefaultLastHttpContent(); }
        }, ch::close, cause -> { HttpTransport.abort(ch); return null; });
        int gen;
        synchronized (this) { gen = generation; }
        ch.pipeline().addLast(new HttpCarrierClient.ResponseReader(c, reads, new HttpCarrierClient.ResponseHook() {
            @Override public boolean redirect(int status, String location) { return StreamingRequest.this.redirect(gen, c, status, location); }
            @Override public void response(boolean ok) { settle(gen, ok); }
        }));
        var head = ch.write(req);
        if (h3) head.addListener(done -> {
            if (!done.isSuccess()) {
                c.lost(done.cause());
                HttpTransport.abort(ch);
            }
        });
        if (prefix != null && prefix.length > 0) ch.write(new DefaultHttpContent(Unpooled.wrappedBuffer(prefix)));
        for (ByteBuf b : replay) ch.write(new DefaultHttpContent(b.retainedDuplicate()));
        ch.flush();
        return c;
    }

    /** The first response that is not a redirect: the carrier keeps no more. */
    private void settle(int gen, boolean ok) {
        URI settledUrl;
        synchronized (this) {
            if (gen != generation || settled) return;
            settled = true;
            dropKept();
            settledUrl = url;
        }
        if (ok && target != null) target.redirected(settledUrl.toString());
    }

    private void dropKept() {
        if (kept == null) return;
        for (ByteBuf b : kept) b.release();
        kept = null;
    }

    /**
     * A redirect answered request gen on c (its loop): true when it is followed, the request
     * repeated at the new URL; false fails c with the reason.
     */
    private boolean redirect(int gen, BodyCarrier c, int status, String location) {
        URI next;
        HttpRedirectException refused = null;
        synchronized (this) {
            if (gen != generation || settled || failed) return false;
            next = null;
            try {
                next = Redirects.check(url, get ? "GET" : "POST", status, location);
                if (hops >= Redirects.DEFAULT_MAX) throw new HttpRedirectException(status, next.toString(), "too many redirects");
                if (writes && kept == null) {
                    throw new HttpRedirectException(status, next.toString(), "more than " + Redirects.MAX_REPLAY + " uplink bytes sent");
                }
            } catch (HttpRedirectException e) {
                failed = true;
                dropKept();
                refused = e;
                refusal = e;
            }
            if (refused == null) {
                hops++;
                generation++;
                redirecting = true;
                url = next;
            }
        }
        carrierChanged();
        if (refused != null) {
            c.lost(refused); // the carrier's error, outside the lock
            return true;
        }
        // The old request goes without a word to zmux: its bytes are replayed.
        c.detach();
        HttpTransport.abort(c.ch);
        URI u = next;
        client.conns.open(u, false, false).whenComplete((ch, err) -> {
            if (err != null) {
                fail(err);
                return;
            }
            Loops.run(ch.eventLoop(), () -> {
                BodyCarrier nc;
                List<PipeWriter.Completion> done;
                PipeReader.Sink s;
                boolean closeReader, closeWriter;
                Throwable why = null;
                synchronized (this) {
                    // Every byte kept so far, the writes made while redirecting among them, goes
                    // first on the new request, and later writes go to it: one step under the lock.
                    try {
                        if (boundConn != null && Requests.connection(ch) != boundConn && Requests.tls(boundConn) != null) {
                            throw new HttpRedirectException(status, u.toString(), "redirect to another TLS connection");
                        }
                        nc = request(ch, u, kept == null ? List.of() : kept);
                    } catch (IOException | RuntimeException e) {
                        ch.close();
                        why = e;
                        nc = null;
                    }
                    if (nc == null) {
                        done = null;
                        s = null;
                        closeReader = closeWriter = false;
                    } else {
                        current = nc;
                        redirecting = false;
                        done = new ArrayList<>(deferred);
                        deferred.clear();
                        s = sink;
                        closeReader = readerClosed;
                        closeWriter = writerClosed;
                    }
                }
                if (nc == null) {
                    fail(why);
                    return;
                }
                carrierChanged();
                boolean gone;
                synchronized (this) { gone = failed; }
                if (gone) {
                    // Aborted while the request was made.
                    if (writes) writerOf(nc).abort(new java.nio.channels.ClosedChannelException());
                    if (reads) readerOf(nc).close();
                    for (PipeWriter.Completion d : done) d.failed(new java.nio.channels.ClosedChannelException());
                    return;
                }
                if (reads) {
                    if (closeReader) readerOf(nc).close();
                    else if (s != null) readerOf(nc).start(s);
                }
                if (writes && closeWriter) writerOf(nc).close();
                for (PipeWriter.Completion d : done) d.completed();
            }, () -> { ch.close(); fail(new java.nio.channels.ClosedChannelException()); });
        });
        return true;
    }

    /** The read half of a carrier of this request, which has one when the request reads. */
    private static PipeReader readerOf(BodyCarrier c) {
        return java.util.Objects.requireNonNull(c.reader(), "a reading request's carrier has a read half");
    }

    /** The write half of a carrier of this request, which has one when the request writes. */
    private static PipeWriter writerOf(BodyCarrier c) {
        return java.util.Objects.requireNonNull(c.writer(), "a writing request's carrier has a write half");
    }

    /** The next request could not be made: the carrier fails. */
    private void fail(Throwable cause) {
        PipeReader.Sink s;
        List<PipeWriter.Completion> done;
        synchronized (this) {
            failed = true;
            redirecting = false;
            dropKept();
            s = readerClosed ? null : sink;
            sink = null;
            done = new ArrayList<>(deferred);
            deferred.clear();
        }
        carrierChanged();
        for (PipeWriter.Completion d : done) d.failed(cause);
        if (s != null) s.onError(cause);
    }

    private final class Reader implements PipeReader {
        @Override public void start(Sink s) {
            BodyCarrier c;
            synchronized (StreamingRequest.this) {
                sink = s;
                c = redirecting || failed ? null : current;
            }
            if (c != null) readerOf(c).start(s);
        }

        @Override public void close() {
            BodyCarrier c;
            synchronized (StreamingRequest.this) {
                readerClosed = true;
                sink = null;
                c = redirecting ? null : current;
            }
            if (c != null) readerOf(c).close();
        }
    }

    private final class Writer implements Requests.CarrierWriter {
        @Override public Runnable watchCarrier(Runnable watcher) { return StreamingRequest.this.watchCarrier(watcher); }

        @Override public Channel carrierRequest() {
            synchronized (StreamingRequest.this) {
                return redirecting || failed || current == null ? null : current.ch;
            }
        }

        @Override public void write(OutboundBatch batch, Completion done) {
            BodyCarrier c;
            int gen;
            boolean keep;
            synchronized (StreamingRequest.this) {
                if (failed) { done.failed(refusal != null ? refusal : new java.nio.channels.ClosedChannelException()); return; }
                keep = kept != null && !settled;
                if (keep) {
                    long n = batch.bytes();
                    if (keptBytes + n > Redirects.MAX_REPLAY && !redirecting) {
                        // Beyond the bound: no more is kept, and a later redirect is refused.
                        dropKept();
                        keep = false;
                    } else {
                        ByteBuffer[] segs = batch.segments(null);
                        kept.add(Unpooled.copiedBuffer(java.util.Arrays.copyOf(segs, batch.segmentCount())));
                        keptBytes += n;
                    }
                }
                if (redirecting) {
                    // Kept: the next request sends it first.
                    deferred.add(done);
                    return;
                }
                c = current;
                gen = generation;
            }
            boolean kept = keep;
            writerOf(c).write(batch, new Completion() {
                @Override public void completed() { done.completed(); }

                @Override public void failed(Throwable cause) {
                    // The response may still be on its way (a front that answers and closes): it
                    // decides, read before the connection's end is.
                    // report is safe on any thread: a loop that refuses it has it here.
                    c.ch.closeFuture().addListener(x -> Loops.run(c.ch.eventLoop(), () -> report(cause), () -> report(cause)));
                }

                private void report(Throwable cause) {
                    boolean replaced;
                    Throwable why;
                    synchronized (StreamingRequest.this) {
                        replaced = gen != generation && !StreamingRequest.this.failed;
                        why = refusal;
                    }
                    // A request a redirect replaced: its bytes go again on the next one.
                    if (kept && replaced) done.completed();
                    else done.failed(why != null ? why : cause);
                }
            });
        }

        @Override public void close() {
            BodyCarrier c;
            synchronized (StreamingRequest.this) {
                writerClosed = true;
                c = redirecting ? null : current;
            }
            if (c != null) writerOf(c).close();
        }

        @Override public void abort(Throwable cause) {
            BodyCarrier c;
            List<Completion> done;
            synchronized (StreamingRequest.this) {
                writerClosed = true;
                failed = true;
                dropKept();
                c = redirecting ? null : current;
                done = new ArrayList<>(deferred);
                deferred.clear();
            }
            for (Completion d : done) d.failed(cause == null ? new java.nio.channels.ClosedChannelException() : cause);
            if (c != null) writerOf(c).abort(cause);
        }
    }
}
