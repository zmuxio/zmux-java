package io.zmux.http3;

import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.zmux.http.HttpCarrierClient;
import io.zmux.http.internal.HttpTransport;
import io.zmux.quic.QuicOptions;
import io.zmux.CarrierCredential;
import io.zmux.ZmuxFuture;
import io.zmux.internal.spi.Spi;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Objects;
import javax.net.ssl.TrustManagerFactory;

/** Selects HTTP/3 for the shared streaming, Split and Exchange client. */
public final class Http3Client {
    private Http3Client() {}

    /** Configure the client with the system trust store and default QUIC limits. */
    public static HttpCarrierClient.Builder builder() { return builder(null, QuicOptions.defaults()); }

    /**
     * Configure an HTTPS client with custom QUIC options. A null TLS builder uses system trust.
     * The transport sets ALPN to h3, verifies the endpoint name and disables early data.
     */
    public static HttpCarrierClient.Builder builder(QuicSslContextBuilder tls, QuicOptions options) {
        return HttpTransport.clientBuilder(new Http3ClientTransport(
                tls == null ? defaultTls() : tls, Objects.requireNonNull(options, "options")));
    }

    /**
     * Binds requests to an established HTTP/3 client connection and one HTTPS origin. The
     * caller authorizes that origin and owns the connection and its UDP socket. Invoke from
     * the QUIC connection's loop when the handshake completes, before peer control streams
     * arrive, with no existing HTTP/3 handler. Repeated bindings to the same origin share the
     * HTTP/3 state. Closing a client ends its requests.
     */
    public static HttpCarrierClient.Builder builder(QuicChannel connection, URI origin) {
        var transport = new Http3ClientTransport(connection, origin, false);
        return HttpTransport.clientBuilder(transport).group(connection.eventLoop());
    }

    /** Binds WebTransport to a host connection using the receive limits applied to its QUIC codec. */
    public static HttpCarrierClient.Builder builder(QuicChannel connection, URI origin, QuicOptions options) {
        var transport = new Http3ClientTransport(connection, origin, false, Objects.requireNonNull(options));
        return HttpTransport.clientBuilder(transport).group(connection.eventLoop());
    }

    /** Opens an owned draft-16 session. The setup timeout covers the entire CONNECT attempt. */
    public static ZmuxFuture<WebTransportSession> openWebTransportAsync(
            HttpCarrierClient client, URI target, Map<String, List<String>> headers,
            CarrierCredential credential, Duration timeout) {
        Objects.requireNonNull(client, "client");
        if (!(HttpTransport.transport(client) instanceof Http3ClientTransport transport)) {
            throw new IllegalArgumentException("WebTransport requires an Http3Client");
        }
        if (Objects.requireNonNull(timeout, "timeout").isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("positive WebTransport setup timeout required");
        }
        var request = WebTransportConnect.request(target, headers);
        try {
            WebTransportProvider.require(transport.provider);
            return transport.openSession(client, target, request, credential, timeout);
        } catch (UnsupportedOperationException unavailable) {
            return Spi.future(CompletableFuture.failedFuture(unavailable), null);
        }
    }

    private static QuicSslContextBuilder defaultTls() {
        try {
            var trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init((KeyStore) null);
            return QuicSslContextBuilder.forClient().trustManager(trust);
        } catch (GeneralSecurityException failure) {
            throw new IllegalStateException("the default TLS trust store is unusable", failure);
        }
    }
}
