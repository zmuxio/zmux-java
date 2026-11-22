package io.zmux.http3;

import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.zmux.http.HttpCarrierClient;
import io.zmux.http.internal.HttpTransport;
import io.zmux.quic.QuicOptions;
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
