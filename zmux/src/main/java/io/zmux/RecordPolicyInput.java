package io.zmux;

/**
 * The description of a carrier that a {@link RecordPolicy} and an {@link AllowRecordMode} see.
 *
 * @param hasExporter the carrier has an exporter
 * @param secure the carrier declares the Bound contract ({@link io.zmux.carrier.PipeIo#isSecure()})
 * @param label the application's label, or null
 * @param isFirstPipe the carrier is the session's first
 */
public record RecordPolicyInput(boolean hasExporter, boolean secure, String label, boolean isFirstPipe) {}
