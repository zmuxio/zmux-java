package io.zmux;

import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/**
 * The TLS side of a carrier. A client sends {@code serverName} as its server name (SNI)
 * unless it is an IP address literal, and verifies the server certificate for it with the
 * context's trust; a server has no server name. ALPN identifiers come from
 * {@link ZmuxConfig#alpn()}.
 */
public record TlsOptions(SSLContext context, String serverName, boolean secure) {
    public TlsOptions {
        Objects.requireNonNull(context, "context");
    }

    public TlsOptions(SSLContext context, String serverName) { this(context, serverName, false); }

    /**
     * These options declaring the Bound contract: the host states that the TLS
     * connection ends at the peer, with no proxy that terminates it. Only for a connection dialled or accepted
     * directly. The handshake's version is judged afterwards: TLS 1.3, or TLS 1.2, whose export needs the
     * extended master secret.
     */
    public TlsOptions withSecure() { return new TlsOptions(context, serverName, true); }

    public static TlsOptions client(SSLContext context, String serverName) {
        return new TlsOptions(context, Objects.requireNonNull(serverName, "serverName"));
    }

    public static TlsOptions server(SSLContext context) {
        return new TlsOptions(context, null);
    }

    SSLEngine clientEngine(int port, java.util.List<String> alpn) {
        if (serverName == null) throw new IllegalArgumentException("client TLS needs a server name");
        SSLEngine engine = context.createSSLEngine(serverName, port);
        engine.setUseClientMode(true);
        SSLParameters p = engine.getSSLParameters();
        p.setApplicationProtocols(alpn.toArray(String[]::new));
        p.setEndpointIdentificationAlgorithm("HTTPS");
        if (!literal(serverName)) {
            try { p.setServerNames(java.util.List.of(new javax.net.ssl.SNIHostName(serverName))); }
            catch (IllegalArgumentException notAHostName) { /* no SNI */ }
        }
        engine.setSSLParameters(p);
        return engine;
    }

    /** Whether name is an IP address literal, which TLS does not send as a server name. */
    private static boolean literal(String name) {
        return name.indexOf(':') >= 0 || name.chars().allMatch(c -> c == '.' || c >= '0' && c <= '9');
    }

    SSLEngine serverEngine(String[] protocols) {
        SSLEngine e = context.createSSLEngine();
        e.setUseClientMode(false);
        SSLParameters p = e.getSSLParameters();
        p.setApplicationProtocols(protocols);
        e.setSSLParameters(p);
        return e;
    }
}
