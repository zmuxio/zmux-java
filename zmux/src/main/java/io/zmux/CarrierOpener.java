package io.zmux;

import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

/** Opens carriers for a session attempt. Returned carriers transfer to the executor even on failure. */
@FunctionalInterface
public interface CarrierOpener {
    Opened open(Cancellation setup, Request request) throws IOException;

    /** The stage for which the executor needs the returned carriers. */
    enum Stage { BOOTSTRAP, JOIN }

    /** The actual layout and lifetime of one carrier handoff. */
    record Request(long attempt, Stage stage, Directions need, List<AttachmentSpec> layout,
                   CarrierCredential credential, Duration timeout, Cancellation lifetime) {
        public Request { layout = List.copyOf(layout); }
    }

    /** The resources opened by the callback and any failure obtained while opening them. */
    record Opened(List<PipeIo> carriers, Throwable failure) {
        public Opened { carriers = List.copyOf(carriers); }
        public Opened(List<PipeIo> carriers) { this(carriers, null); }
    }
}
