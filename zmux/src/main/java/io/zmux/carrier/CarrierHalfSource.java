package io.zmux.carrier;

import io.zmux.CarrierKind;
import java.util.Objects;

/** The host's stable attribution of a physical carrier half. */
public record CarrierHalfSource(String carrierId, CarrierKind kind, String route, String fateDomain) {
    public CarrierHalfSource {
        Objects.requireNonNull(carrierId);
        Objects.requireNonNull(kind);
        Objects.requireNonNull(route);
        Objects.requireNonNull(fateDomain);
    }
}
