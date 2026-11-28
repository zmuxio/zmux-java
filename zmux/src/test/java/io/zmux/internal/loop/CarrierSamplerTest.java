package io.zmux.internal.loop;

import io.zmux.CarrierCounts;
import io.zmux.CarrierSource;
import io.zmux.Direction;
import io.zmux.QuicCounters;
import io.zmux.TcpPacketCounters;
import io.zmux.TcpSegmentCounters;
import io.zmux.internal.spi.CarrierProvider;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.zmux.internal.loop.CarrierSampler.INTERVAL;
import static org.junit.jupiter.api.Assertions.*;

final class CarrierSamplerTest {
    private final AtomicLong clock = new AtomicLong();
    private final ArrayDeque<Runnable> work = new ArrayDeque<>();
    private final CarrierSampler.Registry registry = new CarrierSampler.Registry(clock::get, work::add);
    private final Object identity = new Object();
    private final ArrayDeque<CarrierProvider.SampleCompletion> queries = new ArrayDeque<>();

    @Test void cachesTheAcquisitionTimeAndPreservesNativeQuicLossCorrection() {
        var connection = registry.attach(identity, provider(CarrierSource.NETTY_QUIC));
        assertTrue(connection.read().isEmpty());
        work.remove().run();
        clock.set(5);
        respond(new QuicCounters(0, 0));
        var first = connection.read().orElseThrow();
        assertEquals(Direction.OUTBOUND, first.direction());
        assertEquals(CarrierSource.NETTY_QUIC, first.source());
        assertEquals(5, first.sampledAt());
        assertEquals(0, first.generation());
        assertEquals(new QuicCounters(0, 0), first.counts().orElseThrow());

        assertSame(connection, registry.attach(identity, provider(CarrierSource.NETTY_QUIC)));
        clock.addAndGet(INTERVAL - 1);
        assertSame(first, connection.read().orElseThrow());
        assertTrue(work.isEmpty());

        registry.detach(identity);
        clock.incrementAndGet();
        assertSame(first, connection.read().orElseThrow());
        work.remove().run();
        clock.addAndGet(200);
        queries.remove().accept(clock.get() - 200, new QuicCounters(10, 4));
        assertEquals(5 + INTERVAL, connection.read().orElseThrow().sampledAt());

        var interval = connection.publishedLoss();
        assertEquals(4, interval.lost());
        assertEquals(10, interval.sent());
        assertSame(interval, registry.connection(identity).publishedLoss());
        assertTrue(work.isEmpty());

        advance(connection, new QuicCounters(10, 3));
        var corrected = connection.read().orElseThrow();
        assertEquals(first.carrierId(), corrected.carrierId());
        assertEquals(0, corrected.generation());
        assertSame(interval, connection.publishedLoss());
        assertEquals(5 + INTERVAL, corrected.sampledAt());
        assertEquals(new QuicCounters(10, 3), corrected.counts().orElseThrow());

        advance(connection, null);
        var unavailable = connection.read().orElseThrow();
        assertEquals(CarrierSource.NETTY_QUIC, unavailable.source());
        assertTrue(unavailable.counts().isEmpty());
        advance(connection, new QuicCounters(1, 0));
        var reset = connection.read().orElseThrow();
        assertEquals(1, reset.generation());
        assertEquals(first.carrierId(), reset.carrierId());
    }

    @Test void detachesAndDistinguishesReplacementFromAReattachment() {
        var connection = registry.attach(identity, provider(CarrierSource.NETTY_QUIC));
        assertTrue(work.isEmpty());
        assertTrue(connection.read().isEmpty());
        work.remove().run();
        respond(new QuicCounters(1, 1));
        advance(connection, new QuicCounters(0, 0));
        var first = connection.read().orElseThrow();
        clock.addAndGet(INTERVAL);
        assertSame(first, connection.read().orElseThrow());
        work.remove().run();
        registry.detach(identity);
        assertSame(first, connection.read().orElseThrow());
        assertSame(connection, registry.attach(identity, provider(CarrierSource.NETTY_QUIC)));
        respond(new QuicCounters(7, 7));
        assertSame(first, connection.read().orElseThrow());
        registry.detach(identity);
        work.remove().run();
        assertTrue(queries.isEmpty());
        clock.addAndGet(INTERVAL);
        assertSame(first, connection.read().orElseThrow());
        assertTrue(work.isEmpty());

        var replacement = registry.attach(new Object(), null);
        var second = replacement.read().orElseThrow();
        assertNotEquals(first.carrierId(), second.carrierId());
        assertEquals(0, second.generation());
        assertEquals(CarrierSource.UNKNOWN, second.source());
        assertTrue(second.counts().isEmpty());
        clock.addAndGet(10 * INTERVAL);
        assertSame(second, replacement.read().orElseThrow());
        assertTrue(work.isEmpty());
    }

    @Test void publishesTaggedNativePairsAndKeepsKnownSourcesWithoutCounts() {
        var segments = new TcpSegmentCounters(10, 2);
        var linux = registry.attach(identity, provider(CarrierSource.LINUX_TCP_INFO));
        assertTrue(linux.read().isEmpty());
        work.remove().run();
        respond(segments);
        var tcp = linux.read().orElseThrow();
        assertEquals(CarrierSource.LINUX_TCP_INFO, tcp.source());
        assertEquals(segments, tcp.counts().orElseThrow());
        advance(linux, new TcpSegmentCounters(11, 1));
        assertEquals(1, linux.read().orElseThrow().generation());

        var packets = new TcpPacketCounters(20, 2);
        var darwin = registry.attach(new Object(), provider(CarrierSource.DARWIN_TCP_CONNECTION_INFO));
        assertTrue(darwin.read().isEmpty());
        work.remove().run();
        respond(packets);
        var mac = darwin.read().orElseThrow();
        assertEquals(CarrierSource.DARWIN_TCP_CONNECTION_INFO, mac.source());
        assertEquals(packets, mac.counts().orElseThrow());

        var windowsIdentity = new Object();
        var windowsSource = CarrierSource.WINDOWS_SIO_TCP_INFO;
        var windows = registry.attach(windowsIdentity, new CarrierProvider.Query(windowsSource, null));
        var unsupported = windows.read().orElseThrow();
        assertEquals(CarrierSource.WINDOWS_SIO_TCP_INFO, unsupported.source());
        assertTrue(unsupported.counts().isEmpty());
        registry.attach(windowsIdentity, provider(CarrierSource.NETTY_QUIC));
        clock.addAndGet(10 * INTERVAL);
        assertSame(unsupported, windows.read().orElseThrow());
        registry.detach(windowsIdentity);
        registry.detach(windowsIdentity);
        registry.attach(windowsIdentity, provider(windowsSource));
        assertSame(unsupported, windows.read().orElseThrow());
        assertTrue(work.isEmpty());
        assertTrue(queries.isEmpty());
    }

    @Test void upgradingAnUnknownSourceClearsTheSampleUntilTheFirstKnownQueryCompletes() {
        var connection = registry.attach(identity, null);
        var unknown = connection.read().orElseThrow();
        assertEquals(CarrierSource.UNKNOWN, unknown.source());
        assertSame(connection, registry.attach(identity, provider(CarrierSource.NETTY_QUIC)));
        assertTrue(connection.read().isEmpty());
        work.remove().run();
        assertTrue(connection.read().isEmpty());
        respond(new QuicCounters(0, 0));
        var known = connection.read().orElseThrow();
        assertEquals(CarrierSource.NETTY_QUIC, known.source());
        assertSame(unknown.carrierId(), known.carrierId());
        assertEquals(0, known.generation());
    }

    @Test void theFirstKnownProviderKeepsItsQueries() {
        var connection = registry.attach(identity, provider(CarrierSource.NETTY_QUIC));
        var rejected = new AtomicInteger();
        registry.attach(identity, new CarrierProvider.Query(CarrierSource.LINUX_TCP_INFO, completion ->
                rejected.incrementAndGet()));
        assertTrue(connection.read().isEmpty());
        work.remove().run();
        respond(new QuicCounters(1, 0));
        advance(connection, new QuicCounters(2, 0));
        var sample = connection.read().orElseThrow();
        assertEquals(CarrierSource.NETTY_QUIC, sample.source());
        assertEquals(new QuicCounters(2, 0), sample.counts().orElseThrow());
        assertEquals(0, rejected.get());
    }

    @Test void aReadTriggersAnInitialQuery() {
        var synchronous = new CarrierSampler.Registry(clock::get, Runnable::run);
        var connection = synchronous.attach(identity, new CarrierProvider.Query(CarrierSource.NETTY_QUIC,
                completion -> completion.accept(clock.get(), new QuicCounters(0, 0))));
        assertTrue(connection.read().isEmpty());
        var sample = connection.read().orElseThrow();
        assertEquals(new QuicCounters(0, 0), sample.counts().orElseThrow());
        assertEquals(0, sample.carrierId().getClass().getConstructors().length);
    }

    @Test void concurrentReadsReturnTheOldSampleWhileOneQueryIsBlocked() throws Exception {
        var tasks = new ConcurrentLinkedQueue<Runnable>();
        var cache = new CarrierSampler.Registry(clock::get, tasks::add);
        var calls = new AtomicInteger();
        var blocked = new CountDownLatch(1);
        var unblock = new CountDownLatch(1);
        var connection = cache.attach(identity, new CarrierProvider.Query(CarrierSource.NETTY_QUIC, completion -> {
            long at = clock.get();
            int count = calls.incrementAndGet();
            if (count == 2) {
                blocked.countDown();
                try {
                    if (!unblock.await(30, TimeUnit.SECONDS)) throw new AssertionError("query not released");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            completion.accept(at, new QuicCounters(count, 0));
        }));
        assertTrue(tasks.isEmpty());
        assertTrue(connection.read().isEmpty());
        tasks.remove().run();
        var old = connection.read().orElseThrow();
        clock.addAndGet(INTERVAL);
        assertSame(old, connection.read().orElseThrow());
        try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            var query = threads.submit(tasks.remove());
            try {
                assertTrue(blocked.await(30, TimeUnit.SECONDS));
                var results = new ArrayDeque<java.util.concurrent.Future<Optional<io.zmux.CarrierSample>>>();
                for (int i = 0; i < 64; i++) results.add(threads.submit(connection::read));
                for (var result : results) assertSame(old, result.get(30, TimeUnit.SECONDS).orElseThrow());
                assertTrue(tasks.isEmpty());
                assertEquals(2, calls.get());
            } finally {
                unblock.countDown();
            }
            query.get(30, TimeUnit.SECONDS);
        }
        assertEquals(new QuicCounters(2, 0), connection.read().orElseThrow().counts().orElseThrow());
    }

    @Test void idleConnectionsDoNotScheduleEvenWhenTheirSamplesExpire() {
        var identities = new Object[1000];
        var calls = new AtomicInteger();
        var known = new CarrierProvider.Query(CarrierSource.NETTY_QUIC, completion -> {
            calls.incrementAndGet();
            completion.accept(clock.get(), null);
        });
        for (int i = 0; i < identities.length; i++) {
            identities[i] = new Object();
            assertTrue(registry.attach(identities[i], known).read().isEmpty());
        }
        while (!work.isEmpty()) work.remove().run();
        assertEquals(1000, calls.get());
        clock.addAndGet(10 * INTERVAL);
        assertTrue(work.isEmpty());
        assertEquals(1000, calls.get());
    }

    private CarrierProvider.Query provider(CarrierSource source) {
        return new CarrierProvider.Query(source, queries::add);
    }

    private void respond(CarrierCounts counts) {
        queries.remove().accept(clock.get(), counts);
    }

    private void advance(CarrierSampler.Connection connection, CarrierCounts counts) {
        clock.addAndGet(INTERVAL);
        assertTrue(connection.read().isPresent());
        work.remove().run();
        respond(counts);
    }
}
