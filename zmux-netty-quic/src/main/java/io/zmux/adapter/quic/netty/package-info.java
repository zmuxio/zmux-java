/**
 * Optional Netty QUIC adapter package for ZMux.
 *
 * <p>The core {@code io.zmux} API is transport-neutral, so applications that do
 * not use this module do not need any Netty or QUIC dependency. {@link
 * io.zmux.adapter.quic.netty.NettyQuic} wraps an established Netty QUIC
 * connection as a {@link io.zmux.ZmuxSession}: bidi and uni stream open/accept
 * operations map to QUIC streams, and QUIC stream cancellation codes are
 * surfaced as {@link io.zmux.ApplicationError} values where the transport
 * exposes them.
 *
 * <p>Open-time ZMux metadata is carried in a small adapter prelude on each
 * locally opened QUIC stream: {@code varint(metadata_len)} followed by metadata
 * TLVs. Accepted-stream prelude parsing is bounded and runs off the event loop,
 * so stalled or malformed preludes cannot block later ready streams
 * indefinitely. A fresh local bidi stream submits the prelude before read-side
 * terminal control such as {@code closeRead()} / {@code cancelRead(...)} sends
 * QUIC {@code STOP_SENDING}.
 *
 * <p>Fresh write-side reset or abort visibility is not a portable adapter
 * guarantee because QUIC {@code RESET_STREAM} may discard previously written
 * but unacknowledged stream data, including a just-submitted metadata prelude.
 *
 * <p>QUIC does not represent post-open ZMux metadata updates or stream-level
 * reason strings on cancellation frames. After the prelude is emitted, metadata
 * updates fail with {@link io.zmux.PriorityUpdateUnavailableException}; reason
 * text remains advisory and connection-level closes are the portable place for
 * peer-visible reason text.
 */
package io.zmux.adapter.quic.netty;
