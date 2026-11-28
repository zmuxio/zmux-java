package io.zmux;

import io.zmux.internal.loop.Bootstrap;
import io.zmux.internal.core.Time;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** Staggers carrier preparation while the caller runs one Bootstrap handshake at a time. */
final class BootstrapRace implements AutoCloseable {
    static final long STAGGER_NANOS = TimeUnit.MILLISECONDS.toNanos(250);
    private static final List<AttachmentSpec> BOOTSTRAP = List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults()));

    private final SessionController controller;
    final SessionController.Owner owner;
    private final CarrierCredential credential;
    private final ClockDomain clock;
    private final long deadline;
    private Runnable cancelDeadline = () -> {};
    private final ArrayList<SessionController.Candidate> remaining;
    private final LinkedHashSet<Opening> inFlight = new LinkedHashSet<>();
    private final ArrayDeque<Prepared> ready = new ArrayDeque<>();
    private final CompletableFuture<Void> stopped = new CompletableFuture<>();
    private CompletableFuture<Void> progress = new CompletableFuture<>();
    private long lastStart, nextAttempt;
    private DialAfterException dialAfter;
    private boolean stagger, exhausted;

    BootstrapRace(SessionController controller, SessionController.Owner owner, ZmuxConfig config, List<SessionController.Candidate> ranked) {
        this.controller = controller;
        this.owner = owner;
        credential = config.carrierCredential().orElse(null);
        clock = controller.clock();
        deadline = Time.add(clock.now(), Time.nanos(config.handshakeBound()));
        remaining = new ArrayList<>(ranked.stream().filter(SessionController.Candidate::canStart).toList());
    }

    final class Opening {
        final SessionController.Candidate candidate;
        long id;
        final Cancellation setup = new Cancellation();
        final Cancellation lifetime = new Cancellation();
        final SessionController.Lease lease;
        final long deadline = Math.min(BootstrapRace.this.deadline, Time.add(clock.now(), Time.nanos(controller.timeout())));
        private boolean published;
        private boolean selected;
        private Runnable cancelTimer = () -> {};

        private Opening(SessionController.Candidate candidate, SessionController.Lease lease) {
            this.candidate = candidate;
            this.lease = lease;
        }

        void launch() {
            cancelTimer = clock.schedule(deadline, () -> {
                publish(this, new CarrierOpener.Opened(List.of(),
                        new java.net.SocketTimeoutException("bootstrap attempt deadline expired")), false);
                cancelSetup();
            });
            Thread.ofVirtual().name("zmux-bootstrap-prepare").start(() -> {
                CarrierOpener.Opened result;
                try {
                    startOpener(this);
                    result = java.util.Objects.requireNonNull(candidate.opener().open(setup, request(this)));
                } catch (IOException | RuntimeException error) {
                    result = new CarrierOpener.Opened(List.of(), error);
                } catch (Error failure) {
                    publish(this, new CarrierOpener.Opened(List.of(), failure), true);
                    throw failure;
                } finally {
                    lease.hostReturned();
                }
                publish(this, result, true);
            });
        }

        boolean expired() { return clock.now() >= deadline; }

        private void cancelSetup() {
            Thread.ofVirtual().name("zmux-bootstrap-cancel").start(setup::cancel);
        }

        void cancel() {
            lease.finished();
            endSetup(true);
        }

        void endSetup(boolean endLifetime) {
            cancelTimer.run();
            Thread.ofVirtual().name("zmux-bootstrap-cancel").start(() -> {
                try { setup.close(); }
                finally { if (endLifetime) lifetime.close(); }
            });
        }
    }

    private synchronized void startOpener(Opening opening) throws IOException {
        if (opening.expired()) {
            opening.lease.suppress(CarrierCache.Suppression.DEADLINE);
            throw new java.net.SocketTimeoutException("bootstrap attempt deadline expired");
        }
        if (stopped.isDone() || opening.published) {
            opening.lease.suppress(CarrierCache.Suppression.STALE);
            throw new InterruptedIOException("bootstrap preparation cancelled");
        }
        if (!opening.lease.start()) {
            if (opening.lease.blockedUntil() != 0)
                throw new DialAfterException(opening.candidate.id(), opening.lease.blockedUntil());
            throw new InterruptedIOException("bootstrap preparation cancelled");
        }
        opening.id = ++nextAttempt;
    }

    private CarrierOpener.Request request(Opening opening) {
        return new CarrierOpener.Request(opening.id, CarrierOpener.Stage.BOOTSTRAP, Directions.BOTH, BOOTSTRAP,
                credential, Duration.ofNanos(Math.max(0, Time.sub(opening.deadline, clock.now()))), opening.lifetime);
    }

    record Prepared(Opening opening, CarrierOpener.Opened resources, boolean returned) {
        void discard() {
            if (!returned) return;
            var failure = resources.failure() == null ? new java.util.concurrent.CancellationException("bootstrap was not handed off")
                    : resources.failure();
            var released = resources.carriers().stream().map(io -> Bootstrap.discard(io, failure))
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(released).whenComplete((_, error) -> opening.lease.cleanup.close());
        }
    }

    void start() {
        cancelDeadline = clock.schedule(deadline, this::close);
        Thread.ofVirtual().name("zmux-bootstrap-race").start(this::schedule);
    }

    private void schedule() {
        try {
            while (!stopped.isDone()) {
                var available = controller.available();
                CompletableFuture<Void> changed;
                Opening launch = null;
                long wait;
                synchronized (this) {
                    if (remaining.isEmpty()) return;
                    changed = progress;
                    long at = clock.now();
                    wait = stagger ? Time.add(lastStart, STAGGER_NANOS) : at;
                    if (wait <= at) {
                        wait = Time.NEVER;
                        dialAfter = null;
                        boolean eligible = false;
                        for (var candidate : remaining) {
                            long after = controller.initialDialAfter().getOrDefault(candidate.id(), 0L);
                            long now = clock.now();
                            if (now < after) {
                                owner.suppressDue(new SessionController.BlockedStart(candidate, CarrierCache.Suppression.DIAL_AFTER, after));
                                if (dialAfter == null || after < dialAfter.retryAt()) dialAfter = new DialAfterException(candidate.id(), after);
                                wait = Math.min(wait, after);
                                continue;
                            }
                            long retry = controller.retryDelay(candidate);
                            if (retry != 0) {
                                var block = controller.admission(candidate, false);
                                if (block != null) owner.suppressDue(new SessionController.BlockedStart(candidate, block, Time.add(now, retry)));
                                wait = Math.min(wait, Time.add(now, retry));
                                continue;
                            }
                            eligible = true;
                            var admitted = owner.acquire(candidate, false, BOOTSTRAP.size(), true,
                                    () -> controller.initialDialAfter().getOrDefault(candidate.id(), 0L));
                            // Opening owns the lease until the winner is transferred or an attempt is discarded.
                            //noinspection resource
                            var lease = admitted.lease();
                            if (lease == null) continue;
                            launch = new Opening(candidate, lease);
                            break;
                        }
                        if (!eligible) owner.blocked();
                        if (launch != null) {
                            remaining.remove(launch.candidate);
                            inFlight.add(launch);
                            lastStart = clock.now();
                            stagger = true;
                        }
                    }
                }
                if (launch != null) launch.launch();
                else await(available, changed, wait);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            synchronized (this) { exhausted = true; notifyAll(); }
        }
    }

    private void await(CompletableFuture<Void> available, CompletableFuture<Void> changed, long at) throws InterruptedException {
        var due = new CompletableFuture<Void>();
        var cancel = clock.schedule(at, () -> due.complete(null));
        try {
            CompletableFuture.anyOf(available, changed, stopped, due).get();
        } catch (ExecutionException error) {
            throw new IllegalStateException("bootstrap race signal failed", error);
        } finally {
            cancel.run();
        }
    }

    private void publish(Opening opening, CarrierOpener.Opened resources, boolean returned) {
        var prepared = new Prepared(opening, resources, returned);
        synchronized (this) {
            if (!stopped.isDone() && !opening.published) {
                opening.published = true;
                ready.addLast(prepared);
                if (resources.failure() != null) releaseStagger();
                notifyAll();
                return;
            }
        }
        prepared.discard();
    }

    synchronized void releaseStagger() {
        stagger = false;
        var waiting = progress;
        progress = new CompletableFuture<>();
        waiting.complete(null);
    }

    synchronized Prepared take() throws IOException {
        try {
            while (!stopped.isDone()) {
                var next = ready.pollFirst();
                if (next != null) {
                    next.opening().selected = true;
                    return next;
                }
                if (exhausted && inFlight.isEmpty()) return null;
                wait();
            }
            return null;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("bootstrap interrupted");
        }
    }

    synchronized DialAfterException dialAfter() {
        if (dialAfter != null && deadline < dialAfter.retryAt()) return dialAfter;
        return remaining.stream().map(candidate -> new DialAfterException(candidate.id(),
                controller.initialDialAfter().getOrDefault(candidate.id(), 0L)))
                .filter(block -> deadline < block.retryAt())
                .min(java.util.Comparator.comparingLong(DialAfterException::retryAt)).orElse(null);
    }

    synchronized void finished(Opening opening) {
        inFlight.remove(opening);
    }

    @Override public void close() {
        List<Opening> cancel;
        List<Prepared> discard;
        synchronized (this) {
            if (!stopped.complete(null)) return;
            cancel = List.copyOf(inFlight);
            inFlight.clear();
            discard = List.copyOf(ready);
            ready.clear();
            notifyAll();
        }
        cancelDeadline.run();
        cancel.forEach(opening -> {
            if (opening.selected) opening.cancelSetup();
            else opening.cancel();
        });
        if (!discard.isEmpty()) {
            Thread.ofVirtual().name("zmux-bootstrap-discard").start(() -> discard.forEach(Prepared::discard));
        }
    }
}
