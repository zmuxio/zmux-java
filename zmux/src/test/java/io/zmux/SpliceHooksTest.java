package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class SpliceHooksTest {
    private static final Duration GUARD = Duration.ofSeconds(45);
    private static final Duration RESIDENCE = Duration.ofSeconds(30);

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(GUARD.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static SpliceHooks bytes(SpliceHook... hooks) {
        return new SpliceHooks(List.of(hooks), List.of(), List.of());
    }

    @Test void eachLinkSeesTheWholeGroupAndCannotRetainBorrowedStorage() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new SpliceTest.Relay(SpliceTest.config(loops))) {
            var resume = new SpliceWait.Resume();
            var seen = new CompletableFuture<SpliceUnit.Metadata>();
            var escaped = new AtomicReference<SpliceUnit.Item>();
            var writer = new AtomicReference<SpliceUnit.Output>();
            var opts = SpliceOptions.builder().tags("inbound", "outbound").hooksAToB(bytes(unit -> {
                SpliceUnit.Item item = unit.item(0);
                escaped.set(item);
                SpliceUnit.Output first = item.slice(0, item.length());
                SpliceUnit.Output second = unit.allocate(2);
                second.copyFrom(0, item, 0, 1);
                second.put(1, (byte) 9);
                writer.set(second);
                return new SpliceHook.Replace(List.of(first, second), SpliceWait.until(resume, RESIDENCE));
            }, unit -> {
                assertEquals(2, unit.size());
                assertEquals(7, unit.item(0).get(0));
                assertEquals(9, unit.item(1).get(1));
                seen.complete(unit.metadata());
                return new SpliceHook.Pass();
            })).build();
            var splice = Zmux.splice(relay.a, relay.b, opts);
            var meta = await(seen);
            assertEquals("inbound", meta.sourceTag());
            assertEquals(SpliceUnit.Origin.FORWARDED, meta.origin());
            assertEquals(SpliceUnit.Direction.A_TO_B, meta.direction());
            assertTrue(meta.targetId().isEmpty());
            assertThrows(IllegalStateException.class, () -> escaped.get().get(0));
            assertThrows(IllegalStateException.class, () -> writer.get().put(0, (byte) 0));
            resume.signal();
            try (var peer = relay.destination()) {
                assertArrayEquals(new byte[] {7, 7, 9}, peer.inputStream().readNBytes(3));
                relay.endpoint.finish();
                peer.finish();
                var result = await(splice);
                assertEquals(3, result.aToB().bytes());
                assertEquals(2, result.aToB().hookCalls());
                assertEquals(1, result.aToB().replacements());
            }
        }
    }

    @Test void injectionRunsTheInstalledGenerationAndFinClosesAdmissionEvenWhilePaused() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new SpliceTest.Relay(SpliceTest.config(loops))) {
            var control = Zmux.spliceControlled(relay.a, relay.b, SpliceOptions.DEFAULT);
            var direction = control.aToB();
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                long paused = await(direction.pause());
                var rejected = direction.injectStream(ByteBuffer.wrap(new byte[] {1}));
                assertEquals(new SpliceController.Rejected(SpliceController.Rejection.PAUSED), await(rejected.admission()));
                assertInstanceOf(SpliceController.Rejected.class, await(rejected.disposition()));

                var seen = new CompletableFuture<SpliceUnit.Metadata>();
                long generation = await(direction.replaceHooks(bytes(unit -> {
                    seen.complete(unit.metadata());
                    var item = unit.allocate(1);
                    item.put(0, (byte) (unit.item(0).get(0) + 1));
                    return new SpliceHook.Replace(List.of(item));
                })));
                assertTrue(await(direction.resume(paused)));
                var sent = direction.injectStream(ByteBuffer.wrap(new byte[] {9}));
                assertInstanceOf(SpliceController.Accepted.class, await(sent.admission()));
                assertInstanceOf(SpliceController.Submitted.class, await(sent.disposition()));
                assertEquals(SpliceUnit.Origin.INJECTED, await(seen).origin());
                assertEquals(generation, await(seen).generation());
                assertEquals(10, peer.inputStream().read());
                assertTrue(await(direction.replaceHooks(SpliceHooks.NONE)) > generation);
                var direct = direction.injectStream(ByteBuffer.wrap(new byte[] {9}));
                assertInstanceOf(SpliceController.Submitted.class, await(direct.disposition()));
                assertEquals(9, peer.inputStream().read());

                long nextPause = await(direction.pause());
                assertNotEquals(paused, nextPause);
                assertFalse(await(direction.resume(paused)));
                relay.endpoint.finish();
                assertEquals(-1, peer.inputStream().read());
                assertEquals(new SpliceController.Rejected(SpliceController.Rejection.CLOSED), await(direction.injectStream(ByteBuffer.allocate(0)).admission()));
                peer.write(new byte[] {3});
                peer.finish();
                assertArrayEquals(new byte[] {3}, SessionTest.readAll(relay.endpoint));
                assertEquals(new SpliceEnd.Finished(), await(control.result()).aToB().end());
            }
        }
    }

    @Test void hookedCapsulesKeepNonMinimalHeadersAndRejectAnInvalidContextBeforeWritingIt() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new SpliceTest.Relay(SpliceTest.config(loops))) {
            var seen = new CompletableFuture<SpliceUnit.Metadata>();
            var hooks = new SpliceHooks(List.of(), List.of(unit -> {
                assertEquals(3, unit.item(0).length());
                assertEquals(3, unit.item(0).get(2));
                seen.complete(unit.metadata());
                return new SpliceHook.Pass();
            }), List.of());
            var splice = Zmux.splicePackets(relay.a, relay.b, SpliceOptions.builder().chunk(1).hooksAToB(hooks).build());
            relay.endpoint.write(new byte[] {0x40, 3, 1, 2, 3});
            try (var peer = relay.destination()) {
                assertEquals(7, await(seen).capsuleType().orElseThrow());
                assertArrayEquals(new byte[] {7, 0x40, 3, 1, 2, 3}, peer.inputStream().readNBytes(6));
                relay.endpoint.write(new byte[] {0, 1, 0x40});
                relay.endpoint.finish();
                var error = assertThrows(StreamResetException.class, () -> peer.inputStream().read());
                assertEquals(io.zmux.internal.wire.TransportError.PACKET_ERROR, error.code());
                peer.finish();
                assertEquals(new SpliceEnd.PacketError(), await(splice).aToB().end());
            }
        }
    }

    @Test void datagramReorderingUsesTheClockAndLeavesReliableSubmissionIndependent() throws Exception {
        var clock = new ManualClock();
        var hooks = new SpliceHooks(List.of(), List.of(), List.of(unit -> unit.item(0).get(0) == 1
                ? new SpliceHook.Pass(SpliceWait.after(Duration.ofSeconds(5), RESIDENCE))
                : new SpliceHook.Pass()));
        var limits = new SpliceLimits(4, 4, 16, 65536, 4 << 20, 4, 65536, RESIDENCE, 1, 4);
        try (EventLoops loops = EventLoops.create(2);
             var relay = new SpliceTest.Relay(ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).build())) {
            var options = SpliceOptions.builder().limits(limits).hooksAToB(hooks).clock(clock).build();
            var control = Zmux.spliceControlled(relay.a, relay.b, options);
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                var first = control.aToB().injectDatagram(4, ByteBuffer.wrap(new byte[] {1}));
                var second = control.aToB().injectDatagram(4, ByteBuffer.wrap(new byte[] {2}));
                assertInstanceOf(SpliceController.Accepted.class, await(first.admission()));
                assertInstanceOf(SpliceController.Submitted.class, await(second.disposition()));
                assertEquals(2, peer.receiveDatagram(GUARD).payload()[0]);
                relay.endpoint.write(new byte[] {8});
                assertEquals(8, peer.inputStream().read());
                clock.advance(Duration.ofSeconds(5));
                assertInstanceOf(SpliceController.Submitted.class, await(first.disposition()));
                assertEquals(1, peer.receiveDatagram(GUARD).payload()[0]);
                relay.endpoint.finish();
                peer.finish();
                assertEquals(2, await(control.result()).aToB().datagramBytes());
            }
        }
    }

    @Test void targetStopWinsOverAWaitAndDisposesTheOldGeneration() throws Exception {
        try (EventLoops loops = EventLoops.create(2); var relay = new SpliceTest.Relay(SpliceTest.config(loops))) {
            var token = new SpliceWait.Resume();
            var entered = new CompletableFuture<Void>();
            relay.b.flush();
            var opts = SpliceOptions.builder().hooksAToB(bytes(unit -> {
                entered.complete(null);
                return new SpliceHook.Pass(SpliceWait.until(token, RESIDENCE));
            })).build();
            var control = Zmux.spliceControlled(relay.a, relay.b, opts);
            try (var peer = relay.destination()) {
                await(entered);
                assertEquals(2, await(control.aToB().replaceHooks(SpliceHooks.NONE)));
                peer.stop(83);
                peer.finish();
                var result = await(control.result());
                assertEquals(new SpliceEnd.Stopped(83), result.aToB().end());
                assertEquals(0, result.aToB().bytes());
                token.signal();
            }
        }
    }

    @Test void aPendingReliableUnitExpiresWithoutAcquiringAnOutputGrant() throws Exception {
        var clock = new ManualClock();
        var entered = new CompletableFuture<Void>();
        SpliceWait.Resume resume = new SpliceWait.Resume();
        var cost = new CompletableFuture<Long>();
        var limiter = new io.zmux.helper.Limiter() {
            @Override public long maxBurstBytes() { return Long.MAX_VALUE; }
            @Override public CompletionStage<Long> acquire(long min, long max) {
                cost.complete(max);
                return CompletableFuture.completedFuture(max);
            }
            @Override public long tryAcquire(long min, long max) { return 0; }
            @Override public void refund(long n) {}
        };
        var opts = SpliceOptions.builder().clock(clock).aToB(limiter).hooksAToB(bytes(unit -> {
            entered.complete(null);
            return new SpliceHook.Pass(SpliceWait.until(resume, RESIDENCE));
        })).build();
        try (EventLoops loops = EventLoops.create(2); var relay = new SpliceTest.Relay(SpliceTest.config(loops))) {
            relay.b.flush();
            var splice = Zmux.splice(relay.a, relay.b, opts);
            try (var peer = relay.destination()) {
                await(entered);
                clock.advance(RESIDENCE);
                var reset = assertThrows(StreamResetException.class, () -> peer.inputStream().read());
                assertEquals(io.zmux.internal.wire.TransportError.CANCELLED, reset.code());
                peer.finish();
                assertEquals(new SpliceEnd.WaitTimeout(), await(splice).aToB().end());
                assertFalse(cost.isDone());
            }
        }
    }

    @Test void aFullMirrorOnlyDropsSamples() throws Exception {
        try (EventLoops loops = EventLoops.create(2);
             var relay = new SpliceTest.Relay(SpliceTest.config(loops));
             var mirror = new SpliceMirror(1, 1, 1)) {
            var options = SpliceOptions.builder().mirrorAToB(mirror).build();
            var control = Zmux.spliceControlled(relay.a, relay.b, options);
            try (var peer = relay.destination()) {
                assertEquals(7, peer.inputStream().read());
                var second = control.aToB().injectStream(ByteBuffer.wrap(new byte[] {8}));
                assertInstanceOf(SpliceController.Submitted.class, await(second.disposition()));
                assertEquals(8, peer.inputStream().read());
                var sample = mirror.poll();
                assertNotNull(sample);
                assertEquals(7, sample.payload().getFirst().get(0));
                assertEquals(3, mirror.misses());
                relay.endpoint.finish();
                peer.finish();
                assertEquals(2, await(control.result()).aToB().bytes());
            }
        }
    }

    static final class ManualClock implements SpliceClock {
        private record Wake(long at, Runnable runnable) {}
        private final ArrayList<Wake> wakes = new ArrayList<>();
        private final java.util.Map<Long, CompletableFuture<Void>> registrations = new java.util.HashMap<>();
        private long now;

        synchronized CompletableFuture<Void> whenScheduled(long at) {
            if (wakes.stream().anyMatch(wake -> wake.at == at)) return CompletableFuture.completedFuture(null);
            return registrations.computeIfAbsent(at, ignored -> new CompletableFuture<>());
        }

        ClockDomain domain() {
            return new ClockDomain(new ClockDomain.Source() {
                @Override public long nanoTime() { return now(); }
                @Override public java.time.Instant wallTime() { return java.time.Instant.EPOCH.plusNanos(now()); }
                @Override public Runnable schedule(long delay, Runnable task) {
                    return ManualClock.this.schedule(now() + delay, task);
                }
            });
        }

        @Override public synchronized long now() { return now; }

        @Override public Runnable schedule(long at, Runnable runnable) {
            var wake = new Wake(at, runnable);
            CompletableFuture<Void> registered;
            boolean pending;
            synchronized (this) {
                pending = at > now;
                if (pending) wakes.add(wake);
                registered = registrations.remove(at);
            }
            if (registered != null) registered.complete(null);
            if (pending) return () -> cancel(wake);
            runnable.run();
            return () -> {};
        }

        private synchronized void cancel(Wake wake) { wakes.remove(wake); }

        void advance(Duration duration) {
            List<Wake> ready;
            synchronized (this) {
                now += duration.toNanos();
                long advanced = now;
                ready = wakes.stream().filter(wake -> wake.at <= advanced).toList();
                wakes.removeAll(ready);
            }
            ready.forEach(wake -> wake.runnable.run());
        }
    }
}
