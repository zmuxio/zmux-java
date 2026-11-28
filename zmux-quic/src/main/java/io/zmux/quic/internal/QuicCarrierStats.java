package io.zmux.quic.internal;

import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicConnectionStats;
import io.netty.util.concurrent.Future;
import io.zmux.CarrierSource;
import io.zmux.QuicCounters;
import io.zmux.internal.spi.CarrierProvider;
import io.zmux.internal.spi.Loops;

/** Reads Netty's native sender statistics for a QUIC connection. */
public final class QuicCarrierStats {
    private QuicCarrierStats() {}

    public static CarrierProvider.Query query(QuicChannel connection, Object identity) {
        if (connection == null || connection != identity) return null;
        return new CarrierProvider.Query(CarrierSource.NETTY_QUIC, completion -> collect(connection, completion));
    }

    private static void collect(QuicChannel connection, CarrierProvider.SampleCompletion completion) {
        //noinspection resource: borrowed: the channel owns its event loop
        var loop = connection.eventLoop();
        Loops.dispatch(loop, loop.inEventLoop(), () -> {
            // Closed Netty channels can return their old statsAtClose.
            if (!connection.isActive()) {
                completion.accept(System.nanoTime(), null);
                return;
            }
            long sampledAt = System.nanoTime();
            Future<QuicConnectionStats> stats;
            try {
                stats = connection.collectStats();
            } catch (RuntimeException unavailable) {
                completion.accept(sampledAt, null);
                return;
            }
            // Netty's quiche implementation completes inline on the connection loop.
            if (stats.isDone()) complete(connection, stats, sampledAt, completion);
            else stats.addListener(done -> complete(connection, stats, sampledAt, completion));
        }, () -> completion.accept(System.nanoTime(), null));
    }

    private static void complete(QuicChannel connection, Future<QuicConnectionStats> stats, long sampledAt,
                                 CarrierProvider.SampleCompletion completion) {
        var pair = connection.isActive() && stats.isSuccess() ? stats.getNow() : null;
        completion.accept(sampledAt, pair == null || pair.sent() == -1 || pair.lost() == -1
                ? null : new QuicCounters(pair.sent(), pair.lost()));
    }
}
