package io.zmux;

/**
 * The description of a carrier that {@link ZmuxConfig.Builder#recordPolicy} and
 * {@link ZmuxConfig.Builder#allowRecordMode} see.
 *
 * @param hasExporter the carrier has an exporter
 * @param secure the carrier declares the Bound contract ({@link io.zmux.carrier.PipeIo#secure()})
 * @param label the application's label, or null
 * @param firstPipe the carrier is the session's first
 */
public record RecordCarrier(boolean hasExporter, boolean secure, String label, boolean firstPipe) {
    /** Chooses the record mode of a carrier the local endpoint joins. */
    @FunctionalInterface
    public interface Policy {
        RecordMode choose(RecordCarrier carrier);
    }

    /** The acceptor's check of a Hello's mode; peer is the session's host value. */
    @FunctionalInterface
    public interface Allow {
        boolean allow(RecordCarrier carrier, RecordMode mode, Object peer);
    }
}
