package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.Framing;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.PipeIo;
import io.zmux.carrier.PipeReader;
import io.zmux.carrier.PipeWriter;
import io.zmux.internal.spi.AdapterPorts;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.Test;

class ControllerDomainTest {
    private static SessionController.Candidate candidate(String id) {
        return new SessionController.Candidate(id, id, CarrierKind.STREAMING, "", 0, false, true,
                List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults())),
                (cancel, request) -> { throw new AssertionError("unexpected opener"); });
    }

    @Test void defaultsAndQueueBoundary() throws Exception {
        var options = new SessionController.Options("domain-queue", List.of(), SessionController.Choice.STATIC, SessionController.Limits.DEFAULT, null, null, java.util.Map.of());
        assertEquals(64, options.limits().maximumPendingOwners());
        assertEquals(4, options.limits().maximumOpeners());
        assertEquals(8, options.limits().maximumCleanupAttachments());
        assertEquals(Duration.ofSeconds(10), options.limits().attemptTimeout());
        assertEquals(250_000_000, BootstrapRace.STAGGER_NANOS);
        try (var controller = new SessionController(options)) {
            var owners = new ArrayList<SessionController.Owner>();
            for (int i = 0; i < 64; i++) {
                var owner = controller.new Owner();
                owner.enqueue();
                owners.add(owner);
            }
            owners.getFirst().enqueue();
            assertThrows(SessionController.QueueFullException.class,
                    () -> controller.open(ZmuxConfig.builder().build(), SessionTarget.empty()));
            owners.getFirst().withdraw();
            controller.new Owner().enqueue();
            assertThrows(SessionController.QueueFullException.class, owners.getFirst()::enqueue);
        }
    }

    @Test void aQueuedSessionRunsBeforeTheNextReadyPoolInThePreviousSession() throws Exception {
        try (var loops = EventLoops.create(2);
             var a = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 6));
             var b = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 6))) {
            var starts = new java.util.concurrent.LinkedBlockingQueue<String>();
            var busy = candidate("busy");
            var candidates = List.of(busy, recordingCandidate("p", a, starts), recordingCandidate("q", a, starts),
                    recordingCandidate("r", b, starts));
            var options = new SessionController.Options("domain-pool-rotation", candidates, SessionController.Choice.STATIC,
                    SessionController.Limits.DEFAULT.withMaximumOpeners(1), null, null, java.util.Map.of());
            try (var controller = new SessionController(options);
                 var occupying = controller.new Owner().acquire(busy, false, 1, false, () -> 0).lease();
                 var aControl = controller.manage(a.client, new SessionTarget(1, List.of(group("p", 2), group("q", 1)), List.of()))) {
                SessionTestSupport.observe(aControl, state -> state.blocked() == SessionControl.Blocking.EXECUTION);
                try (var bControl = controller.manage(b.client, new SessionTarget(1, List.of(group("r", 1)), List.of()))) {
                    SessionTestSupport.observe(bControl, state -> state.blocked() == SessionControl.Blocking.EXECUTION);
                    occupying.hostReturned();
                    for (String id : List.of("p", "r", "q", "p")) {
                        assertEquals(id, starts.poll(5, java.util.concurrent.TimeUnit.SECONDS));
                    }
                }
            }
        }
    }

    private static SessionController.Candidate recordingCandidate(String id, SessionTestSupport.Fixture fixture,
                                                                  java.util.concurrent.BlockingQueue<String> starts) {
        var opener = SessionTestSupport.memoryOpener(fixture.listener, (spec, io) -> io);
        return new SessionController.Candidate(id, id, CarrierKind.STREAMING, "", 0, false, false,
                List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults())), (scope, request) -> {
                    starts.add(id);
                    return opener.open(scope, request);
                });
    }

    private static SessionTarget.Group group(String id, int count) {
        return new SessionTarget.Group(id, List.of(new SessionTarget.Quota("coverage", count, Directions.BOTH, List.of(id))), SessionTarget.Rotation.NONE);
    }

    @Test void openKeepsThePendingPlaceUntilTheSessionInheritsIt() throws Exception {
        var trace = new BootstrapTrace();
        try (var loops = EventLoops.create(2)) {
            var config = SessionTestSupport.configBuilder(loops, 4).tracer(trace).build();
            try (var listener = Zmux.listener(ListenerConfig.of(config))) {
                var entered = new CompletableFuture<Void>();
                var release = new CompletableFuture<Void>();
                var bootstrap = new SessionTestSupport.MemoryBootstrap();
                var c = new SessionController.Candidate("bootstrap", "bootstrap", CarrierKind.STREAMING, "", 0, false, true,
                        List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults())), (setup, request) -> {
                            trace.mark(BootstrapStage.OPENER_ENTERED);
                            entered.complete(null);
                            release.join();
                            var io = trace.observe(bootstrap.client(), BootstrapStage.CLIENT_FIRST_RECORD_SENT,
                                    BootstrapStage.CLIENT_FIRST_RECORD_RECEIVED);
                            var opened = new CarrierOpener.Opened(List.of(io));
                            trace.mark(BootstrapStage.OPENER_RETURNED);
                            return opened;
                        });
                var limits = new SessionController.Limits(1, 1, 1, Duration.ofSeconds(10));
                var options = new SessionController.Options("bootstrap-pending-place", List.of(c),
                        SessionController.Choice.STATIC, limits, null, null, Map.of());
                try (var controller = new SessionController(options)) {
                    var opening = new FutureTask<>(() -> {
                        var opened = controller.open(config, SessionTarget.empty());
                        trace.mark(BootstrapStage.OPEN_RETURNED);
                        return opened;
                    });
                    Thread.ofVirtual().start(opening);
                    try {
                        trace.await(entered, "opener entry");
                        assertThrows(SessionController.QueueFullException.class, () -> controller.new Owner().enqueue());
                    } finally {
                        trace.mark(BootstrapStage.OPENER_RELEASED);
                        release.complete(null);
                    }
                    trace.await(bootstrap.firstRecord, "client first write");
                    assertThrows(TimeoutException.class, () -> opening.get(0, TimeUnit.NANOSECONDS),
                            () -> "the listener has not delivered its first record. " + trace);
                    assertThrows(SessionController.QueueFullException.class, () -> controller.new Owner().enqueue());
                    trace.mark(BootstrapStage.LISTENER_ADMIT_CALLED);
                    bootstrap.admit(listener, io -> trace.observe(io, BootstrapStage.SERVER_FIRST_RECORD_SENT,
                            BootstrapStage.SERVER_FIRST_RECORD_RECEIVED));
                    var result = trace.await(opening, "controller.open");
                    try (var client = result.session(); var control = result.control();
                         var peer = trace.accept(listener)) {
                        assertNotNull(control);
                        assertNotNull(client);
                        assertNotNull(peer);
                    }
                }
            }
        }
    }

    private enum BootstrapStage {
        OPENER_ENTERED, OPENER_RELEASED, OPENER_RETURNED, CLIENT_FIRST_RECORD_SENT, LISTENER_ADMIT_CALLED,
        SERVER_FIRST_RECORD_RECEIVED, SERVER_FIRST_RECORD_SENT, CLIENT_FIRST_RECORD_RECEIVED,
        INITIAL_BINDING_EVENT_OBSERVED, OPEN_RETURNED
    }

    private static final class BootstrapTrace implements Tracer {
        private final long start = System.nanoTime();
        private final AtomicReferenceArray<Long> times = new AtomicReferenceArray<>(BootstrapStage.values().length);

        void mark(BootstrapStage stage) {
            times.compareAndSet(stage.ordinal(), null, System.nanoTime() - start);
        }

        @Override public void event(SessionId session, SessionEvent event) {
            // The initial TargetChanged is emitted after installation and delivered asynchronously.
            if (event instanceof SessionEvent.TargetChanged target && target.initial())
                mark(BootstrapStage.INITIAL_BINDING_EVENT_OBSERVED);
        }

        <T> T await(Future<T> future, String stage) throws InterruptedException {
            try {
                return future.get(5, TimeUnit.SECONDS);
            } catch (TimeoutException timeout) {
                return fail("Timed out waiting for " + stage + ". " + this, timeout);
            } catch (ExecutionException failed) {
                return fail("Failed while waiting for " + stage + ". " + this, failed.getCause());
            }
        }

        ZmuxSession accept(ZmuxListener listener) throws IOException {
            try {
                return listener.accept(Duration.ofSeconds(5));
            } catch (SocketTimeoutException timeout) {
                return fail("Timed out waiting for listener.accept. " + this, timeout);
            }
        }

        PipeIo observe(PipeIo io, BootstrapStage sent, BootstrapStage received) {
            PipeReader reader = new PipeReader() {
                @Override public void start(Sink sink) {
                    io.reader().start(new Sink() {
                        @Override public void onChunk(Chunk chunk, Runnable consumed) {
                            mark(received);
                            sink.onChunk(chunk, consumed);
                        }

                        @Override public void onEnd() { sink.onEnd(); }
                        @Override public void onError(Throwable cause) { sink.onError(cause); }
                    });
                }

                @Override public void close() { io.reader().close(); }
            };
            class Writer implements PipeWriter, AdapterPorts.ReleasingHalf {
                @Override public void write(OutboundBatch batch, Completion done) {
                    mark(sent);
                    io.writer().write(batch, done);
                }

                @Override public void close() { io.writer().close(); }
                @Override public void abort(Throwable cause) { io.writer().abort(cause); }
                @Override public CompletionStage<Void> released() {
                    return ((AdapterPorts.ReleasingHalf) io.writer()).released();
                }
            }
            return io.withHalves(reader, new Writer());
        }

        @Override public String toString() {
            var stages = new StringJoiner(", ", "Bootstrap stages [", "]");
            for (BootstrapStage stage : BootstrapStage.values()) {
                Long elapsed = times.get(stage.ordinal());
                stages.add(stage + "=" + (elapsed == null ? "not recorded" : "+" + TimeUnit.NANOSECONDS.toMicros(elapsed) + " us"));
            }
            return stages.toString();
        }
    }

    @Test void bootstrapDiscardKeepsCleanupUntilTheAdapterReleases() throws Exception {
        class DeferredClose implements PipeWriter, AdapterPorts.ReleasingHalf {
            final CompletableFuture<Void> closed = new CompletableFuture<>();
            final CompletableFuture<Void> released = new CompletableFuture<>();
            @Override public void write(OutboundBatch batch, Completion done) { throw new AssertionError("unusable carrier"); }
            @Override public void close() { closed.complete(null); }
            @Override public CompletableFuture<Void> released() { return released; }
        }
        var writer = new DeferredClose();
        var c = new SessionController.Candidate("bootstrap", "bootstrap", CarrierKind.STREAMING, "", 0, false, true,
                List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults())),
                (setup, request) -> new CarrierOpener.Opened(List.of(new PipeIo(null, writer, Framing.BYTE_STREAM, null, null))));
        var limits = new SessionController.Limits(0, 0, 1, Duration.ofSeconds(10));
        var options = new SessionController.Options("bootstrap-discards-asynchronously", List.of(c),
                SessionController.Choice.STATIC, limits, null, null, Map.of());
        try (var loops = EventLoops.create(1); var controller = new SessionController(options)) {
            var config = SessionTestSupport.config(loops, 4);
            assertThrows(java.io.IOException.class, () -> controller.open(config, SessionTarget.empty()));
            writer.closed.get(5, TimeUnit.SECONDS);
            var other = controller.new Owner();
            CompletableFuture<Void> hasRoom;
            try {
                assertEquals(SessionControl.Blocking.CLEANUP, other.acquire(null, false, 1, false, () -> 0).blocked());
                hasRoom = controller.available();
            } finally {
                writer.released.complete(null);
            }
            hasRoom.get(5, TimeUnit.SECONDS);
            try (var cleanup = other.acquire(null, false, 1, false, () -> 0).lease()) {
                assertNotNull(cleanup);
            }
        }
    }

    @Test void configuredOpenerLimitCanExceedFour() {
        var candidates = java.util.stream.IntStream.range(0, 6).mapToObj(i -> candidate("opener-" + i)).toList();
        var options = new SessionController.Options("domain-configurable", candidates,
                SessionController.Choice.STATIC, SessionController.Limits.DEFAULT.withMaximumOpeners(5), null, null, java.util.Map.of());
        assertThrows(IllegalArgumentException.class, () -> SessionController.Limits.DEFAULT.withMaximumOpeners(-1));
        assertEquals(SessionController.Limits.DEFAULT, new SessionController.Limits(0, 0, 0,
                SessionController.Limits.DEFAULT.attemptTimeout()));
        try (var controller = new SessionController(options)) {
            var leases = new ArrayList<SessionController.Lease>();
            try {
                for (int i = 0; i < 5; i++) {
                    var admission = controller.new Owner().acquire(candidates.get(i), false, 1, false, () -> 0);
                    assertNotNull(admission.lease());
                    leases.add(admission.lease());
                }
                var last = controller.new Owner();
                assertEquals(SessionControl.Blocking.EXECUTION, last.acquire(candidates.getLast(), false, 1, false, () -> 0).blocked());
                leases.getFirst().hostReturned();
                var next = last.acquire(candidates.getLast(), false, 1, false, () -> 0);
                assertNotNull(next.lease());
                leases.add(next.lease());
            } finally { leases.forEach(SessionController.Lease::close); }
        }
    }

    @Test void splitDemandAccumulatesCleanupAndKeepsExecutionIndependent() {
        var split = candidate("split");
        var single = candidate("single");
        var options = new SessionController.Options("domain-cleanup", List.of(split, single),
                SessionController.Choice.STATIC, SessionController.Limits.DEFAULT, null, null, java.util.Map.of());
        try (var controller = new SessionController(options);
             var retiring = controller.new Owner().acquire(null, false, 8, false, () -> 0).lease()) {
            var first = controller.new Owner();
            var second = controller.new Owner();
            first.enqueue();
            second.enqueue();
            assertEquals(SessionControl.Blocking.CLEANUP, first.acquire(split, false, 2, false, () -> 0).blocked());
            retiring.cleanup.releaseOne();
            assertEquals(SessionControl.Blocking.CLEANUP, second.acquire(single, false, 1, false, () -> 0).blocked());
            retiring.cleanup.releaseOne();
            try (var active = first.acquire(split, false, 2, false, () -> 0).lease()) {
                assertNotNull(active);
                active.hostReturned();
                active.finished();
                assertEquals(SessionControl.Blocking.CLEANUP, second.acquire(single, false, 1, false, () -> 0).blocked());
                active.cleanup.releaseOne();
                try (var admitted = second.acquire(single, false, 1, false, () -> 0).lease()) {
                    assertNotNull(admitted);
                }
            }
        }
    }

    @Test void waitingOpenerKeepsItsCleanupClass() {
        var occupied = candidate("occupied");
        var waiting = candidate("waiting");
        var defaults = SessionController.Limits.DEFAULT.withMaximumOpeners(1);
        var options = new SessionController.Options("domain-waiting-opener", List.of(occupied, waiting),
                SessionController.Choice.STATIC, defaults, null, null, java.util.Map.of());
        try (var controller = new SessionController(options);
             var host = controller.new Owner().acquire(occupied, false, 1, false, () -> 0).lease()) {
            var first = controller.new Owner();
            var later = controller.new Owner();
            assertEquals(SessionControl.Blocking.EXECUTION, first.acquire(waiting, false, 1, false, () -> 0).blocked());
            assertEquals(SessionControl.Blocking.CLEANUP, later.acquire(null, false, 1, false, () -> 0).blocked());
            host.hostReturned();
            try (var granted = first.acquire(waiting, false, 1, false, () -> 0).lease();
                 var cleanup = later.acquire(null, false, 1, false, () -> 0).lease()) {
                assertNotNull(granted);
                assertNotNull(cleanup);
            }
        }
    }
}
