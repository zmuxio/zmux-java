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
import io.zmux.carrier.PipeWriter;
import io.zmux.http.internal.HttpTransport;
import io.zmux.internal.spi.CarrierProvider;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SSLEngine;

/**
 * What the HTTP adapters read off a request and its connection. Transport streams are attached
 * by the zmux-http3 module, so analysis of this module alone sees them as always absent.
 */
@SuppressWarnings("ConstantValue")
final class Requests {
    private Requests() {}

    static boolean h3(Channel ch) { return HttpTransport.stream(ch) != null; }

    static boolean multiplexed(Channel ch) {
        return ch instanceof Http2StreamChannel || h3(ch);
    }

    /** The physical HTTP connection carrying this request. */
    static Channel connection(Channel ch) {
        var stream = HttpTransport.stream(ch);
        if (stream != null) return stream.connection();
        return ch instanceof Http2StreamChannel && ch.parent() != null ? ch.parent() : ch;
    }

    /** The sending half reports the connection of its request rather than the pipe's combined identity. */
    interface CarrierWriter extends PipeWriter, CarrierProvider {
        Channel carrierRequest();

        @Override default Object carrierIdentity() {
            Channel request = carrierRequest();
            if (request == null) return null;
            var stream = HttpTransport.stream(request);
            return stream == null ? connection(request) : stream.carrierIdentity();
        }

        @Override default CarrierProvider.Query carrierQuery(Object identity) {
            Channel request = carrierRequest();
            var stream = request == null ? null : HttpTransport.stream(request);
            return stream == null ? null : stream.carrierQuery(identity);
        }
    }

    static int version(Channel ch, HttpRequest r) {
        if (h3(ch)) return 3;
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
            if ((version == 1 && !chunked) || full) cl = 0;
        }
        String protocol = h3(ch) ? HttpTransport.protocol(ch) : r.headers().get(":protocol", "");
        return new HttpClass.Head(r.method().name(), version, protocol, cl, RequestHead.of(fields(r.headers())));
    }

    /** The address ch's connection arrived from directly; null when unknown. */
    static InetSocketAddress peer(Channel ch) {
        var stream = HttpTransport.stream(ch);
        if (stream != null) return stream.peer();
        var address = connection(ch).remoteAddress();
        return address instanceof InetSocketAddress peer ? peer : null;
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
        return trusted.source(p, RequestHead.of(fields(r.headers())));
    }

    /** The TLS engine of ch's connection, or null. */
    static SSLEngine tls(Channel ch) {
        var stream = HttpTransport.stream(ch);
        if (stream != null) return stream.tls();
        SslHandler h = connection(ch).pipeline().get(SslHandler.class);
        return h == null ? null : h.engine();
    }

    /**
     * A pipe input on ch's connection with its TLS metadata. The TLS this process runs on the
     * connection (its own SslHandler) gives the exporter and, once its handshake completed on
     * TLS 1.3, the Secure declaration of the Bound contract; TLS that ends before this
     * process (a terminating proxy) is not seen here and declares nothing.
     */
    static PipeIo pipe(PipeIo io, Channel ch) {
        io = io.withReceiveMemory(BodyCarrier.RECEIVE_MEMORY);
        SSLEngine e = tls(ch);
        if (e == null) return io;
        PipeIo.Exporter x = exporter(ch, e);
        String sni = "";
        if (e.getSession() instanceof ExtendedSSLSession s) {
            for (var n : s.getRequestedServerNames()) if (n instanceof javax.net.ssl.SNIHostName h) { sni = h.getAsciiName(); break; }
        }
        // A connection whose handshake has not completed counts as without TLS; the exporter reads it late.
        boolean complete = e.getHandshakeStatus() == javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
        if (sni.isEmpty() && e.getUseClientMode() && e.getPeerHost() != null) sni = e.getPeerHost();
        io = io.withExporter(x).withServerName(sni).withTerminatedTls(complete);
        return complete && e.getSession().getProtocol().equals("TLSv1.3") ? io.withCarrierSecure() : io;
    }

    static PipeIo.Exporter exporter(Channel ch) {
        SSLEngine engine = tls(ch);
        return engine == null ? null : exporter(ch, engine);
    }

    private static PipeIo.Exporter exporter(Channel ch, SSLEngine engine) {
        var stream = HttpTransport.stream(ch);
        return stream == null ? exporter(engine) : stream.exporter();
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

    static final String ACCEL = "X-Accel-Buffering";
    static final String NO_STORE = "no-store";
    static final String OCTETS = "application/octet-stream";
    static final CharSequence CACHE = HttpHeaderNames.CACHE_CONTROL;
}
