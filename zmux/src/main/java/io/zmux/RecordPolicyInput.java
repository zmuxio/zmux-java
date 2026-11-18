package io.zmux;

import java.util.Objects;
import java.util.Optional;

/**
 * The description of a carrier that a {@link RecordPolicy} and an {@link AllowRecordMode} see.
 *
 * @param isExporterPresent the carrier has an exporter
 * @param isSecure the carrier declares the Bound contract ({@link io.zmux.carrier.PipeIo#isCarrierSecure()})
 * @param label the application's label; empty: none
 * @param isFirstPipe the carrier is the session's first
 */
public record RecordPolicyInput(boolean isExporterPresent, boolean isSecure, Optional<String> label, boolean isFirstPipe) {
    public RecordPolicyInput {
        Objects.requireNonNull(label, "label");
    }
}
