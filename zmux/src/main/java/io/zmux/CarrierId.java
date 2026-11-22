package io.zmux;

import io.zmux.internal.loop.CarrierIdentity;

/** Opaque process-local equality and hash key for a transport connection. */
public sealed interface CarrierId permits CarrierIdentity {}
