/**
 * Optional Netty QUIC adapter for {@link io.zmux.ZmuxSession}.
 *
 * <p>{@link io.zmux.adapter.quic.netty.NettyQuic} wraps an established Netty
 * QUIC connection. Open metadata uses a bounded per-stream prelude; post-open
 * metadata updates are not supported by QUIC.
 */
package io.zmux.adapter.quic.netty;
