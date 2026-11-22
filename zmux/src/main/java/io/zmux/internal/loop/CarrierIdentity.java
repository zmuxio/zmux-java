package io.zmux.internal.loop;

import io.zmux.CarrierId;

/** Allocated only when the driver first tracks a connection. */
public final class CarrierIdentity implements CarrierId {
    CarrierIdentity() {}
}
