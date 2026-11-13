package io.zmux.seal;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Supplies the application data of a handshake message, opaque bytes of the layer above: a client's of messages 1
 * (Profiles 1 and 3 only) and 3, a server's of message 2. It is asked once per message, with what is known of
 * the peer then ({@link Known}). An empty result, or empty bytes, sends nothing. A failed stage, or an
 * exception, ends the handshake; the handshake timeout bounds the wait. Unauthenticated peers can make it run
 * (any client in Profiles 4 and 5, any holder of the pre-shared key in 1 and 3), so it is to be light and
 * without side effects.
 */
@FunctionalInterface
public interface AppDataSource {
    /**
     * What is known of the peer when a message is written.
     *
     * @param profile the handshake's Profile
     * @param input null for message 1; for message 2 the PSK identity, the client's data of message 1 and the
     *     local context (the client's static key and credential arrive in message 3); for message 3 the
     *     {@link VerifyInput} the server was judged by
     * @param verdict for message 3 the server's verdict, null when no anchor judged it; null otherwise
     */
    record Known(Profile profile, VerifyInput input, VerifyVerdict verdict) {}

    CompletionStage<Optional<byte[]>> data(int message, Known peer);
}
