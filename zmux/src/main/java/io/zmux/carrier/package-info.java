/**
 * The adapter interface of a carrier: a {@link io.zmux.carrier.PipeIo} of a {@link io.zmux.carrier.PipeReader} and a
 * {@link io.zmux.carrier.PipeWriter}, with its {@link io.zmux.carrier.Framing} and optional
 * {@link io.zmux.carrier.PipeIo.Capabilities}; the {@link io.zmux.carrier.OutboundBatch} a writer takes; the
 * {@link io.zmux.carrier.HandshakeShaper} hook; and the helpers {@link io.zmux.carrier.PipeChannel} and
 * {@link io.zmux.carrier.MemoryPipe}. Wire encodings and record protection are the library's own and not here.
 */
package io.zmux.carrier;
