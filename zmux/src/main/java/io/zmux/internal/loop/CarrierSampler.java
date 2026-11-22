package io.zmux.internal.loop;

import io.zmux.CarrierCounts;
import io.zmux.CarrierId;
import io.zmux.CarrierSample;
import io.zmux.CarrierSource;
import io.zmux.Direction;
import io.zmux.QuicCounters;
import io.zmux.TcpPacketCounters;
import io.zmux.TcpSegmentCounters;
import io.zmux.internal.spi.CarrierProvider;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Connection-scoped publications, independent of session and pipe storage. */
public final class CarrierSampler {
    static final long INTERVAL = TimeUnit.SECONDS.toNanos(1);

    private CarrierSampler() {}

    private static final class Shared {
        private static final Registry REGISTRY = new Registry(System::nanoTime, Thread::startVirtualThread);
    }

    static Connection attach(Object identity, CarrierProvider.Query query) {
        return identity == null ? null : Shared.REGISTRY.attach(identity, query);
    }

    static void detach(Object identity) {
        if (identity != null) Shared.REGISTRY.detach(identity);
    }

    static Connection connection(Object identity) {
        return identity == null ? null : Shared.REGISTRY.connection(identity);
    }

    static Optional<CarrierSample> sample(Connection connection) {
        return connection == null ? Optional.empty() : connection.read();
    }

    private static final class Key extends WeakReference<Object> {
        private final int hash;

        Key(Object identity, ReferenceQueue<Object> queue) {
            super(identity, queue);
            hash = System.identityHashCode(identity);
        }

        @Override public int hashCode() { return hash; }

        @Override public boolean equals(Object other) {
            if (this == other) return true;
            Object identity = get();
            return identity != null && other instanceof Key key && identity == key.get();
        }
    }

    static final class Connection {
        private final CarrierId id;
        private final LongSupplier clock;
        private final Executor background;
        /** The latest publication, null before the first. */
        private volatile CarrierSample sample;
        private volatile CarrierProvider.Query query;
        private volatile Poll inFlight;
        private int users;
        private long generation;
        private long epoch;
        private boolean observed;
        private long lastObservation;
        private CarrierSource source;
        private boolean canCollect;
        private CarrierCounts baseline;

        Connection(CarrierId id, LongSupplier clock, Executor background) {
            this.id = id;
            this.clock = clock;
            this.background = background;
        }

        synchronized boolean attach(CarrierProvider.Query provider) {
            boolean firstUser = ++users == 1;
            var nextSource = provider == null ? CarrierSource.UNKNOWN : provider.source();
            if (source == null || source == CarrierSource.UNKNOWN) {
                if (source != nextSource) adopt(nextSource, provider != null && provider.collect() != null);
            } else if (!firstUser || source != nextSource) {
                // Keep the first known provider while attached and its source across reattachments.
                return false;
            }
            query = canCollect && provider != null && provider.collect() != null ? provider : null;
            if (query == null && sample == null) publish(clock.getAsLong(), null);
            return query != null && sample == null;
        }

        private void adopt(CarrierSource next, boolean collects) {
            source = next;
            canCollect = collects;
            sample = null;
            baseline = null;
            epoch++;
        }

        synchronized void detach() {
            if (users == 0 || --users != 0) return;
            epoch++;
            query = null;
        }

        Optional<CarrierSample> read() {
            var cached = sample;
            if (query != null && inFlight == null
                    && (cached == null || clock.getAsLong() - cached.sampledAt() >= INTERVAL)) refresh();
            return Optional.ofNullable(cached);
        }

        void refresh() {
            var poll = claim();
            if (poll == null) return;
            try {
                background.execute(() -> collect(poll));
            } catch (RuntimeException unavailable) {
                complete(poll, clock.getAsLong(), null);
            }
        }

        private synchronized Poll claim() {
            var provider = query;
            if (provider == null || inFlight != null
                    || (observed && clock.getAsLong() - lastObservation < INTERVAL)) return null;
            inFlight = new Poll(epoch, provider);
            return inFlight;
        }

        private void collect(Poll poll) {
            synchronized (this) {
                if (poll.epoch() != epoch) {
                    inFlight = null;
                    return;
                }
            }
            try {
                poll.query().collect().accept((at, counts) -> complete(poll, at, counts));
            } catch (RuntimeException unavailable) {
                complete(poll, clock.getAsLong(), null);
            }
        }

        private synchronized void complete(Poll poll, long sampledAt, CarrierCounts counts) {
            if (inFlight != poll) return;
            observed = true;
            lastObservation = sampledAt;
            if (epoch == poll.epoch()) publish(sampledAt, counts);
            inFlight = null;
        }

        private void publish(long sampledAt, CarrierCounts counts) {
            if (counts != null) {
                if (reset(baseline, counts)) generation++;
                baseline = counts;
            }
            sample = new CarrierSample(id, generation, Direction.OUTBOUND, source, sampledAt, Optional.ofNullable(counts));
        }
    }

    private record Poll(long epoch, CarrierProvider.Query query) {}

    static final class Registry {
        private final LongSupplier clock;
        private final Executor background;
        private final ReferenceQueue<Object> reclaimed = new ReferenceQueue<>();
        private final HashMap<Key, Connection> connections = new HashMap<>();

        Registry(LongSupplier clock, Executor background) {
            this.clock = clock;
            this.background = background;
        }

        Connection attach(Object identity, CarrierProvider.Query query) {
            Connection connection;
            boolean initial;
            synchronized (this) {
                for (var ref = reclaimed.poll(); ref != null; ref = reclaimed.poll()) {
                    if (ref instanceof Key key) connections.remove(key);
                }
                connection = connections.computeIfAbsent(new Key(identity, reclaimed),
                        ignored -> new Connection(new CarrierIdentity(), clock, background));
                initial = connection.attach(query);
            }
            if (initial) connection.refresh();
            return connection;
        }

        synchronized void detach(Object identity) {
            var connection = connection(identity);
            if (connection != null) connection.detach();
        }

        synchronized Connection connection(Object identity) {
            return connections.get(new Key(identity, null));
        }
    }

    private static boolean reset(CarrierCounts previous, CarrierCounts current) {
        return switch (current) {
            case QuicCounters pair -> previous instanceof QuicCounters old
                    && below(pair.packetsSent(), old.packetsSent());
            case TcpPacketCounters(long sent, long retransmitted)
                    -> previous instanceof TcpPacketCounters(long oldSent, long oldRetransmitted)
                    && (below(sent, oldSent) || below(retransmitted, oldRetransmitted));
            case TcpSegmentCounters(long segments, long retransmitted)
                    -> previous instanceof TcpSegmentCounters(long oldSegments, long oldRetransmitted)
                    && (below(segments, oldSegments) || below(retransmitted, oldRetransmitted));
        };
    }

    private static boolean below(long current, long previous) {
        return Long.compareUnsigned(current, previous) < 0;
    }
}
