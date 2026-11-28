package io.zmux;

import io.zmux.helper.Limiter;
import io.zmux.helper.Permit;
import io.zmux.internal.loop.SerialCallbacks;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;

/** Requests and cleanup slots belong to their direction, including after its terminal result. */
final class SpliceGrants {
    private final Limiter limiter;
    private final SerialCallbacks owner;
    private final Runnable wake;
    private final LongConsumer refunded;
    private final int reliableLimit, datagramLimit;
    private int reliable, datagrams;

    SpliceGrants(Limiter limiter, SpliceLimits limits, SerialCallbacks owner, Runnable wake, LongConsumer refunded) {
        this.limiter = limiter;
        this.owner = owner;
        this.wake = wake;
        this.refunded = refunded;
        reliableLimit = limits.reliableCleanupSlots();
        datagramLimit = limits.datagramCleanupSlots();
    }

    final class Request {
        private final boolean datagram;
        private CompletableFuture<Long> source;
        private Permit permit;
        private Throwable failure;
        private boolean revoked, done, released;

        Request(boolean datagram) { this.datagram = datagram; }

        private void returned(Long bytes, Throwable error, long min, long max) {
            done = true;
            source = null;
            if (error == null && bytes != null && bytes > 0) {
                permit = Permit.fromGrant(limiter, bytes);
                if (bytes > max || bytes < min)
                    failure = new IllegalStateException("invalid limiter grant");
            } else failure = error != null ? error : new IllegalStateException("limiter refused request");
            if (revoked) release();
            wake.run();
        }

        boolean done() { return done; }

        Permit adopt() {
            if (revoked || !done) throw new IllegalStateException("request unavailable");
            Permit result = failure == null ? permit : null;
            if (result != null) permit = null;
            release();
            return result;
        }

        Throwable failure() { return failure; }

        void revoke() {
            if (revoked) return;
            revoked = true;
            if (done) release();
            else if (source != null && !datagram) source.cancel(true);
        }

        private void release() {
            if (released) return;
            released = true;
            if (permit != null) {
                refunded.accept(permit.size());
                permit.close();
                permit = null;
            }
            if (datagram) datagrams--; else reliable--;
        }
    }

    boolean reliableAvailable() { return reliable < reliableLimit; }

    Request request(long min, long max, boolean datagram) {
        if (datagram ? datagrams >= datagramLimit : reliable >= reliableLimit) return null;
        if (datagram) datagrams++; else reliable++;
        Request request = new Request(datagram);
        Thread.ofVirtual().name("zmux-splice-grant").start(() -> {
            try {
                if (datagram) {
                    long bytes = limiter.tryAcquire(min, max);
                    owner.execute(() -> request.returned(bytes, null, min, max));
                } else {
                    var source = Objects.requireNonNull(limiter.acquire(min, max)).toCompletableFuture();
                    owner.execute(() -> {
                        request.source = source;
                        source.whenComplete((bytes, failure) ->
                                owner.execute(() -> request.returned(bytes, failure, min, max)));
                        if (request.revoked) source.cancel(true);
                    });
                }
            } catch (RuntimeException | AssertionError failure) {
                owner.execute(() -> request.returned(null, failure, min, max));
            }
        });
        return request;
    }
}
