package io.zmux.quic;

import java.util.concurrent.CompletionStage;

/** The admission and drain lifecycle of a served QUIC connection. */
public final class QuicConnectionService {
    private final Runnable stop;
    private final Runnable stopLanes;
    private final CompletionStage<Void> drained;

    QuicConnectionService(Runnable stop, Runnable stopLanes, CompletionStage<Void> drained) {
        this.stop = stop;
        this.stopLanes = stopLanes;
        this.drained = drained;
    }

    /** Withdraws uncommitted pipes and refuses new pipes. Attached pipes can still accept lanes. */
    public void stopAdmission() { stop.run(); }

    /** Stops both admission and pending lane acceptance. Handed-off streams keep their owner. */
    public void stop() { stopLanes.run(); }

    /** Completes when the service drains after admission stops, hands the connection back, or the connection ends. */
    public CompletionStage<Void> whenDrained() { return drained; }
}
