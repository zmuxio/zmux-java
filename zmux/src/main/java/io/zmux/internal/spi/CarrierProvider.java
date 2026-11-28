package io.zmux.internal.spi;

import io.zmux.CarrierCounts;
import io.zmux.CarrierSource;
import java.util.function.Consumer;

/** Internal adapter capability of the sending half of a carrier. */
public interface CarrierProvider {
    /** Stable object identity of the underlying connection, or null if unavailable. */
    Object carrierIdentity();

    /**
     * Watches replacement of a redirectable sender. A stable provider returns null. Changes may be delivered
     * on another thread; running the returned action unsubscribes the watcher.
     */
    default Runnable watchCarrier(Runnable changed) { return null; }

    /** Receives the time of the native call and its result, null when the pair is unavailable. */
    @FunctionalInterface
    interface SampleCompletion {
        void accept(long sampledAt, CarrierCounts counts);
    }

    /**
     * The native source and exact connection query, kept together through a replacement.
     * A known provider with no supported query supplies its source with a null collector.
     */
    record Query(CarrierSource source, Consumer<SampleCompletion> collect) {}

    /**
     * Returns the provider for this concrete connection, or null when the provider is unknown. The query must
     * stay bound to that connection even if a wrapper replaces its sender, and complete in the provider's
     * execution context when the read finishes. Collectors schedule work on their carrier event loop and return
     * immediately. The session loop and the carrier loop never wait for each other. The timestamp uses
     * System.nanoTime() and is sampled with the counters.
     */
    default Query carrierQuery(Object identity) { return null; }
}
