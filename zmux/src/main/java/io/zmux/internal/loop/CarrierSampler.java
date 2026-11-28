package io.zmux.internal.loop;

import io.zmux.CarrierCounts;
import io.zmux.CarrierId;
import io.zmux.CarrierLoss;
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
        private CarrierCounts intervalBase;
        private long intervalStart;
        private volatile CarrierLoss loss;

        CarrierLoss publishedLoss() { return loss; }

        Connection(CarrierId id, LongSupplier clock, Executor background) {
            this.id = id;
            this.clock = clock;
            this.background = background;
        }

        synchronized void attach(CarrierProvider.Query provider) {
            boolean firstUser = ++users == 1;
            var nextSource = provider == null ? CarrierSource.UNKNOWN : provider.source();
            if (source == null || source == CarrierSource.UNKNOWN) {
                if (source != nextSource) adopt(nextSource, provider != null && provider.collect() != null);
            } else if (!firstUser || source != nextSource) {
                // Keep the first known provider while attached and its source across reattachments.
                return;
            }
            query = canCollect && provider != null && provider.collect() != null ? provider : null;
            if (query == null && sample == null) publish(clock.getAsLong(), null);
        }

        private void adopt(CarrierSource next, boolean collects) {
            source = next;
            canCollect = collects;
            sample = null;
            baseline = null;
            intervalBase = null;
            loss = null;
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
            inFlight = new Poll(epoch, provider, clock.getAsLong());
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
            inFlight = null;
            if (epoch != poll.epoch()) return;
            observed = true;
            lastObservation = poll.startedAt();
            if (sampledAt - lastObservation < 0 || clock.getAsLong() - sampledAt < 0) return;
            if (sample != null && sampledAt - sample.sampledAt() <= 0) return;
            publish(sampledAt, counts);
        }

        private void recordInterval(CarrierCounts counts, long at, boolean reset) {
            if (reset || intervalBase == null || intervalBase.getClass() != counts.getClass()) {
                loss = null;
            } else {
                long sent = sent(counts);
                if (Long.compareUnsigned(sent, sent(intervalBase)) <= 0) return;
                if (below(lost(counts), lost(intervalBase))) {
                    intervalBase = counts;
                    intervalStart = at;
                    return;
                }
                var unit = switch (counts) {
                    case QuicCounters q -> CarrierLoss.Unit.QUIC_PACKETS;
                    case TcpPacketCounters t -> CarrierLoss.Unit.TCP_PACKETS;
                    case TcpSegmentCounters t -> CarrierLoss.Unit.TCP_SEGMENTS;
                };
                loss = new CarrierLoss(id, generation, source, unit, intervalStart, at,
                        lost(counts) - lost(intervalBase), sent - sent(intervalBase));
            }
            intervalBase = counts;
            intervalStart = at;
        }

        private void publish(long sampledAt, CarrierCounts counts) {
            if (counts != null) {
                boolean reset = reset(baseline, counts);
                if (reset) generation++;
                recordInterval(counts, sampledAt, reset);
                // Publishers hold the connection monitor. Readers see one immutable snapshot.
                CarrierSample previous = sample;
                if (!reset && sameSent(baseline, counts) && previous != null && previous.counts().isPresent()) {
                    if (!baseline.equals(counts)) sample = new CarrierSample(id, generation, Direction.OUTBOUND,
                            source, previous.sampledAt(), Optional.of(counts));
                    baseline = counts;
                    return;
                }
                baseline = counts;
            }
            sample = new CarrierSample(id, generation, Direction.OUTBOUND, source, sampledAt, Optional.ofNullable(counts));
        }
    }

    private record Poll(long epoch, CarrierProvider.Query query, long startedAt) {}

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
            synchronized (this) {
                for (var ref = reclaimed.poll(); ref != null; ref = reclaimed.poll()) {
                    if (ref instanceof Key key) connections.remove(key);
                }
                connection = connections.computeIfAbsent(new Key(identity, reclaimed),
                        ignored -> new Connection(new CarrierIdentity(), clock, background));
                connection.attach(query);
            }
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

    private static long sent(CarrierCounts counts) {
        return switch (counts) {
            case QuicCounters c -> c.packetsSent();
            case TcpPacketCounters c -> c.packetsSent();
            case TcpSegmentCounters c -> c.dataSegsOut();
        };
    }

    private static long lost(CarrierCounts counts) {
        return switch (counts) {
            case QuicCounters c -> c.packetsLost();
            case TcpPacketCounters c -> c.packetsRetransmitted();
            case TcpSegmentCounters c -> c.totalRetrans();
        };
    }

    private static boolean sameSent(CarrierCounts previous, CarrierCounts current) {
        return switch (current) {
            case QuicCounters next -> previous instanceof QuicCounters old && next.packetsSent() == old.packetsSent();
            case TcpPacketCounters next -> previous instanceof TcpPacketCounters old && next.packetsSent() == old.packetsSent();
            case TcpSegmentCounters next -> previous instanceof TcpSegmentCounters old && next.dataSegsOut() == old.dataSegsOut();
        };
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
