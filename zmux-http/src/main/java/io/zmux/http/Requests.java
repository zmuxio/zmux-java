package io.zmux.http;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.ssl.SslHandler;
import io.zmux.RequestHead;
import io.zmux.carrier.PipeIo;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SSLEngine;

/** What the HTTP adapters read off a request and its connection. */
final class Requests {
    private Requests() {}

    /** The connection carrying ch: ch itself, or the connection of an HTTP/2 stream. */
    static Channel connection(Channel ch) {
        return ch instanceof Http2StreamChannel && ch.parent() != null ? ch.parent() : ch;
    }

    static int version(Channel ch, HttpRequest r) {
        return ch instanceof Http2StreamChannel ? 2 : r.protocolVersion().majorVersion();
    }

    static Map<String, List<String>> fields(HttpHeaders h) {
        Map<String, List<String>> m = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : h) m.computeIfAbsent(e.getKey(), k -> new ArrayList<>(1)).add(e.getValue());
        return m;
    }

    /** The head of r on ch, for classification. */
    static HttpClass.Head head(Channel ch, HttpRequest r, boolean full) {
        long cl = HttpUtil.getContentLength(r, -1L);
        int version = version(ch, r);
        if (cl < 0) {
            boolean chunked = HttpUtil.isTransferEncodingChunked(r);
            // An HTTP/1.1 request with neither has no body; so has an HTTP/2 request whose
            // HEADERS ended its stream (the codec makes it a full request).
            if (version == 1 && !chunked || full) cl = 0;
        }
        String protocol = r.headers().get(":protocol", "");
        return new HttpClass.Head(r.method().name(), version, protocol, cl, RequestHead.of(fields(r.headers()), false));
    }

    /** The address ch's connection arrived from directly; null when unknown. */
    static InetSocketAddress peer(Channel ch) {
        return connection(ch).remoteAddress() instanceof InetSocketAddress a ? a : null;
    }

    /**
     * The source of a carrier request: the request's source address ({@link HttpEndpoint#SOURCE}
     * on its connection, set by the host or by a trusted PROXY protocol header, else the peer),
     * then, with trusted, the client a trusted intermediary's forwarding field reports.
     */
    static InetSocketAddress source(Channel ch, HttpRequest r, TrustedProxies trusted) {
        Channel c = connection(ch);
        InetSocketAddress p = ch.attr(HttpEndpoint.SOURCE).get();
        if (p == null) p = c.attr(HttpEndpoint.SOURCE).get();
        if (p == null) p = peer(ch);
        if (p == null || p.getAddress() == null || trusted == null) return p;
        return trusted.source(p, RequestHead.of(fields(r.headers()), false));
    }

    /** The TLS engine of ch's connection, or null. */
    static SSLEngine tls(Channel ch) {
        SslHandler h = connection(ch).pipeline().get(SslHandler.class);
        return h == null ? null : h.engine();
    }

    /** A pipe input on ch's connection with its TLS metadata. */
    static PipeIo pipe(PipeIo io, Channel ch) {
        io = io.withReceiveMemory(BodyCarrier.RECEIVE_MEMORY);
        SSLEngine e = tls(ch);
        if (e == null) return io;
        PipeIo.Exporter x = exporter(e);
        String sni = "";
        if (e.getSession() instanceof ExtendedSSLSession s) {
            for (var n : s.getRequestedServerNames()) if (n instanceof javax.net.ssl.SNIHostName h) { sni = h.getAsciiName(); break; }
        }
        return io.withExporter(x).withServerName(sni);
    }

    /** The keying material exporter of a TLS connection (RFC 5705). */
    static PipeIo.Exporter exporter(SSLEngine e) {
        return (label, context, length) -> {
            try {
                return e.getSession() instanceof ExtendedSSLSession s ? s.exportKeyingMaterialData(label, context, length) : null;
            } catch (javax.net.ssl.SSLException | UnsupportedOperationException ex) {
                return null;
            }
        };
    }

    /** Whether an intermediary forwarded the request from early data: any Early-Data field of value 1 (RFC 8470). */
    static boolean earlyData(HttpRequest r) {
        for (String v : r.headers().getAll("Early-Data")) if (v.strip().equals("1")) return true;
        return false;
    }

    static boolean has(HttpRequest r, CharSequence name) {
        String v = r.headers().get(name);
        return v != null && !v.isEmpty();
    }

    static final CharSequence ACCEL = "X-Accel-Buffering";
    static final CharSequence NO_STORE = "no-store";
    static final CharSequence OCTETS = "application/octet-stream";
    static final CharSequence CACHE = HttpHeaderNames.CACHE_CONTROL;
}
