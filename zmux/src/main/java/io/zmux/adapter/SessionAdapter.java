package io.zmux.adapter;

import io.zmux.ZmuxSession;
import io.zmux.internal.spi.AdapterPorts;
import java.io.IOException;

/**
 * The operations of a {@link ZmuxSession} that only a carrier adapter needs; the end-to-end declaration and the
 * release of a held first flight are application operations of the session itself. Reached through {@link ZmuxSession#adapter()};
 * thread-safe.
 */
public final class SessionAdapter {
    private final AdapterPorts.Session port;

    SessionAdapter(AdapterPorts.Session port) {
        this.port = port;
    }

    /**
     * Whether a local join may start now: the attachments this endpoint joined that have neither
     * failed nor been retired are below the steady attachment limit, the Join keys are in place and no Join backoff runs; only once the unretired attachments it joined reach the attachment limit including Join headroom, the
     * session must also be suspended with an attachment the cap may displace for a join that fills
     * a missing direction.
     */
    public boolean isJoinReady() throws IOException {
        return port.joinReady();
    }
}
