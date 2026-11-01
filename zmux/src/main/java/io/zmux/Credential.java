package io.zmux;

import java.io.IOException;
import java.util.Objects;

/**
 * What a client presents to a server's {@link Admission} on a carrier it opens: the client
 * side of an admission. {@link PrefixAuth} is one.
 *
 * <p>The dialers present it once per carrier, after the carrier's TLS or QUIC handshake
 * completed and before any zmux record: {@link Zmux#connect(java.net.InetSocketAddress, ZmuxConfig)},
 * {@link Zmux#connectTls}, {@link ZmuxListener#connect} and {@link ZmuxListener#connectTls}
 * take {@link ZmuxConfig#credential()}, as the zmux-quic session dialers and a session's own
 * TCP and TLS pipes ({@link Paths#add(java.net.InetSocketAddress, PipeOptions)}, {@link Paths#addTls}) do; the zmux-quic
 * pipe dialers take one per call. A pipe the application opened itself ({@link Zmux#open},
 * {@link ZmuxListener#open}) presents its credential where it was dialed: the prefix written
 * first, or the header field in its HTTP request.
 *
 * <p>info describes the carrier as the server's admission sees it: {@link
 * Admission.CarrierInfo#source()} is the server's address, the exporter that of the
 * carrier's handshake (null without TLS), and {@link Admission.CarrierInfo#head()} is set for
 * {@link #header}. The methods may be called concurrently, on an event loop: they must not
 * block. An exception fails the dial.
 */
public interface Credential {
    /**
     * The bytes a carrier starts with, before any zmux record: a byte stream (TCP, TLS), an
     * HTTP request body, a QUIC connection's first bidirectional stream.
     */
    byte[] prefix(Admission.CarrierInfo info) throws IOException;

    /**
     * The request header field of a carrier decided on its HTTP request head: an HTTP upgrade
     * (WebSocket, WebTransport), or an HTTP request without a body. info's exporter is that of
     * the connection carrying the request.
     */
    HeaderField header(Admission.CarrierInfo info) throws IOException;

    /** A request header field. */
    record HeaderField(String name, String value) {
        public HeaderField {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(value, "value");
        }
    }
}
