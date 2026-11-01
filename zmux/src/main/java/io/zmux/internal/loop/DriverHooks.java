package io.zmux.internal.loop;

import io.zmux.helper.Limiter;
import io.zmux.internal.core.Event;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

/** Immutable driver callbacks; the core receives only the corresponding feature flags. */
public record DriverHooks(Consumer<Event> tracer, LongConsumer dropped, int tracerQueue, LongPredicate allowWindowGrowth,
                          Limiter datagramSend, Limiter datagramReceive, PickerBridge.Picker picker) {
    public static final DriverHooks NONE = new DriverHooks(null, null, 256, null, null, null, null);
}
