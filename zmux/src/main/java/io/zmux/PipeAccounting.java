package io.zmux;

import io.zmux.carrier.CarrierHalfSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Local carrier totals across all lanes of one attachment. */
public record PipeAccounting(PipeId pipe, List<Half> send, List<Half> receive) {
    public PipeAccounting {
        send = List.copyOf(send);
        receive = List.copyOf(receive);
    }

    /** The bytes of complete records and the known partial bytes, grouped by physical half. */
    public record Half(CarrierHalfSource source, long activeBytes, long retiredBytes, long records, long partialBytes,
                       boolean imprecise, boolean sourceOverflow) {
        public long bytes() { return io.zmux.internal.core.Time.add(activeBytes, retiredBytes); }
    }

    /** A live snapshot and a final receipt after all carrier I/O has been released. */
    public interface Ticket {
        PipeAccounting snapshot();
        CompletableFuture<PipeAccounting> completion();
    }
}
