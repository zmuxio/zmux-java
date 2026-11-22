package io.zmux;

/** The native provider behind a carrier observation. */
public enum CarrierSource {
    LINUX_TCP_INFO,
    DARWIN_TCP_CONNECTION_INFO,
    WINDOWS_SIO_TCP_INFO,
    QUIC_GO,
    QUINN,
    NETTY_QUIC,
    UNKNOWN
}
