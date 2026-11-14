package io.zmux.adapter;

import io.zmux.JoinPendingException;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxSession;
import io.zmux.internal.spi.AdapterPorts;
import java.io.IOException;

/**
 * The operations of a {@link ZmuxSession} that only a carrier adapter or a host that proves or
 * schedules the session's carriers itself needs. Reached through {@link ZmuxSession#adapter()};
 * thread-safe.
 */
public final class SessionAdapter {
    private final AdapterPorts.Session port;

    SessionAdapter(AdapterPorts.Session port) {
        this.port = port;
    }

    /**
     * Whether a local join may start now: the attachments this endpoint joined that have neither
     * failed nor been retired are below maxPipes, the join keys are in place and no join backoff
     * runs; only once the unretired attachments it joined reach maxPipes + pipeHeadroom, the
     * session must also be suspended with an attachment the cap may displace for a join that fills
     * a missing direction.
     */
    public boolean isJoinReady() throws IOException {
        return port.joinReady();
    }

    /**
     * States that the host has proven pipe 0 end to end: local, sends nothing. Until then a
     * session without a secure first pipe has no join keys: local joins fail with
     * {@link JoinPendingException} and the peer's are held. With a secure first pipe, a no-op.
     */
    public void declareEndToEnd() throws IOException {
        port.declareEndToEnd();
    }

    /**
     * Ends {@link ZmuxConfig.Builder#holdFirstFlight(boolean)}: everything queued so far is built into
     * records and written to the bootstrap carrier in one write, as far as one batch holds it.
     * Calls after the first, and on a session without the hold, do nothing. It never blocks.
     */
    public void releaseFirstFlight() {
        port.releaseFirstFlight();
    }
}
