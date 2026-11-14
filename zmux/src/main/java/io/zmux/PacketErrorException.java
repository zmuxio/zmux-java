package io.zmux;

/**
 * The bytes of a packet stream were malformed (EXTENSIONS.md Section 12): a capsule above the
 * receiver's bound, or the stream ended inside a capsule. The receiver has stopped the stream's
 * receiving part and reset its sending part with ZMUX_PACKET_ERROR
 * ({@link io.zmux.helper.PacketStream#PACKET_ERROR}).
 */
public final class PacketErrorException extends StreamException {
    public PacketErrorException(long streamId, String message) {
        super(message, streamId);
    }
}
