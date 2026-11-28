package io.zmux.http3;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Objects;

record H3Authority(String host, int port) {
    static H3Authority origin(URI origin) {
        var authority = of(Objects.requireNonNull(origin, "origin"));
        if (authority == null || (!origin.getRawPath().isEmpty() && !origin.getRawPath().equals("/"))
                || origin.getRawQuery() != null) {
            throw new IllegalArgumentException("expected an HTTPS origin");
        }
        return authority;
    }

    static H3Authority of(URI uri) {
        if (!"https".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null
                || uri.getRawUserInfo() != null || uri.getRawFragment() != null
                || uri.getRawAuthority().endsWith(":") || uri.getPort() == 0 || uri.getPort() > 65535) return null;
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        if (host.startsWith("[")) {
            if (host.contains("%")) return null;
            try { host = InetAddress.getByName(host).getHostAddress(); }
            catch (UnknownHostException invalid) { return null; }
        }
        return new H3Authority(host, uri.getPort() < 0 ? 443 : uri.getPort());
    }

    boolean matches(URI uri) { return equals(of(uri)); }

    private boolean matchesHeader(String value) {
        try {
            return equals(origin(URI.create("https://" + value))) && value.indexOf('/') < 0;
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }

    ChannelOutboundHandlerAdapter guard() {
        return new ChannelOutboundHandlerAdapter() {
            @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
                if (message instanceof HttpRequest request) {
                    var hosts = request.headers().getAll(HttpHeaderNames.HOST);
                    if (hosts.size() != 1 || !matchesHeader(hosts.getFirst())) {
                        ReferenceCountUtil.release(message);
                        promise.tryFailure(new IllegalArgumentException("HTTP/3 Host does not match the connection authority"));
                        return;
                    }
                }
                ctx.write(message, promise);
            }
        };
    }
}
