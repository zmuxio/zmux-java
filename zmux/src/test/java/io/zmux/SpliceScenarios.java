package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import io.zmux.helper.Limiter;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.buffer.ReceiveLoans;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.StreamRecv;
import io.zmux.internal.loop.Bootstrap;
import io.zmux.internal.wire.TransportParams;
import io.zmux.testutil.SpecFixtures;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Fixture operations on real application stream halves, callback gates and the manual clock. */
final class SpliceScenarios {
    static final Duration GUARD = Duration.ofSeconds(5);
    final Map<?, ?> setup;
    final List<Map<?, ?>> steps = new ArrayList<>();
    final SpliceHooksTest.ManualClock clock = new SpliceHooksTest.ManualClock();

    SpliceScenarios(Map<?, ?> vector) {
        setup = (Map<?, ?>) vector.get("setup");
        for (Object value : (List<?>) vector.get("steps")) steps.add((Map<?, ?>) value);
    }

    List<Map<?, ?>> actions(String name) {
        return steps.stream().filter(s -> name.equals(s.get("action"))).map(s -> (Map<?, ?>) s.get("args"))
                .collect(java.util.stream.Collectors.toList());
    }

    Map<?, ?> args(String action) { return actions(action).getFirst(); }

    Object expected(String key) {
        for (var step : steps) if (step.get("values") instanceof Map<?, ?> values && values.containsKey(key)) return values.get(key);
        throw new AssertionError(key);
    }

    static long num(Map<?, ?> values, String key) { return ((Number) values.get(key)).longValue(); }

    static <T> T await(CompletionStage<T> future) throws Exception {
        return future.toCompletableFuture().get(GUARD.toMillis(), TimeUnit.MILLISECONDS);
    }

    @FunctionalInterface interface Cut { boolean test(SpliceDirection state) throws IOException; }

    static SpliceDirection until(SpliceController.Direction direction, Cut condition) throws Exception {
        long limit = System.nanoTime() + GUARD.toNanos();
        SpliceDirection state;
        do {
            state = await(direction.snapshot());
            if (condition.test(state)) return state;
        } while (System.nanoTime() < limit);
        throw new AssertionError("direction did not reach fixture cut: " + state);
    }

    static class Credit implements Limiter {
        final AtomicLong balance;
        final AtomicInteger calls = new AtomicInteger();
        final long burst;

        Credit(long balance, long burst) { this.balance = new AtomicLong(balance); this.burst = burst; }

        @Override public long maxBurstBytes() { return burst; }
        @Override public CompletionStage<Long> acquire(long min, long max) { return CompletableFuture.completedFuture(tryAcquire(min, max)); }
        @Override public long tryAcquire(long min, long max) {
            calls.incrementAndGet();
            long before = balance.getAndUpdate(n -> n - Math.min(n, max));
            return Math.min(before, max);
        }
        @Override public void refund(long n) { balance.addAndGet(n); }
    }

    final class Legs implements AutoCloseable {
        final EventLoops loops = EventLoops.create(2);
        final Pair left, right;
        final ZmuxStream source, input, target, peer;
        SpliceBillingVectors.NativeGate channel;

        Legs() throws Exception {
            var builder = ZmuxConfig.builder().eventLoops(loops).enableDatagrams(true).noDatagramQueueDelay()
                    .datagramStreamQueue((int) numberOr(setup, "target_datagram_queue_items", 64))
                    .initialStreamReceiveWindow(4096).maxStreamReceiveWindow(4096);
            if (setup.containsKey("hop_ttl_us") || !actions("pause").isEmpty()) builder.clockDomain(clock.domain());
            var cfg = builder.build();
            left = Pair.memory(cfg);
            PipeIo[] pipes = MemoryPipe.pair(Framing.BYTE_STREAM);
            boolean nativeTarget = "native".equals(setup.get("target_representation"));
            if (nativeTarget) {
                channel = new SpliceBillingVectors.NativeGate(!Boolean.TRUE.equals(setup.get("output_carrier_hold")));
                pipes[0] = pipes[0].withDatagramChannel(channel.outbound());
                pipes[1] = pipes[1].withDatagramChannel(channel.inbound());
            }
            var server = CompletableFuture.supplyAsync(() -> {
                try {
                    var core = cfg.toCore(CoreConfig.Role.RESPONDER, CoreConfig.Role.RESPONDER);
                    if (Boolean.FALSE.equals(setup.get("target_reset_at")))
                        core.local().present &= ~(TransportParams.ANY_ZMUX | TransportParams.RESET_AT);
                    if (setup.containsKey("target_max_datagram_payload"))
                        core.local().maxDatagramFrameSize = num(setup, "target_max_datagram_payload") + 4;
                    var opened = Zmux.started(Bootstrap.external(() -> loops.group().next(), core, pipes[1], cfg.hooks), null);
                    opened.awaitEstablished(Long.MAX_VALUE);
                    return opened;
                } catch (IOException error) { throw new CompletionException(error); }
            }, task -> Thread.ofVirtual().start(task));
            right = new Pair(Zmux.open(pipes[0], Role.CLIENT, cfg), await(server));
            source = left.client().openStream();
            source.flush();
            input = left.server().acceptStream(GUARD);
            if (setup.containsKey("target_stream_credit_bytes")) {
                // Write in the reverse half of an established flow. Its credit contains no OPEN preamble.
                peer = right.server().openStream();
                peer.flush();
                target = right.client().acceptStream(GUARD);
            } else {
                target = right.client().openStream();
                target.flush();
                peer = right.server().acceptStream(GUARD);
            }
            peer.setReadTimeout(GUARD);
        }

        SpliceController controlled(SpliceOptions.Builder builder) {
            var options = builder.clock(clock).build();
            return "packet".equals(setup.get("mode")) ? Zmux.splicePacketsControlled(input, target, options)
                    : Zmux.spliceControlled(input, target, options);
        }

        void receive(Map<?, ?> args) throws IOException {
            source.sendDatagram(num(args, "context"), ByteBuffer.wrap(SpecFixtures.hex(args.get("hex"))));
        }

        void expectBytes(String hex) throws IOException {
            byte[] bytes = SpecFixtures.hex(hex);
            assertArrayEquals(bytes, peer.inputStream().readNBytes(bytes.length));
        }

        @Override public void close() throws IOException {
            left.client().abort(0, "fixture finished");
            right.client().abort(0, "fixture finished");
            source.close();
            peer.close();
            left.close();
            right.close();
            loops.close();
        }
    }

    static long numberOr(Map<?, ?> map, String name, long fallback) {
        return map.get(name) instanceof Number n ? n.longValue() : fallback;
    }

    static SpliceHook.Decision replace(SpliceUnit unit, List<?> children, SpliceWait wait) {
        var members = new ArrayList<SpliceUnit.Output>();
        for (Object hex : children) {
            byte[] bytes = SpecFixtures.hex(hex);
            var output = unit.allocate(bytes.length);
            output.put(0, ByteBuffer.wrap(bytes));
            members.add(output);
        }
        return new SpliceHook.Replace(members, wait);
    }

    static SpliceLimits limits(Map<?, ?> options) {
        var d = SpliceLimits.DEFAULT;
        Map<?, ?> group = options.get("group_limits") instanceof Map<?, ?> m ? m : Map.of();
        Map<?, ?> cleanup = options.get("cleanup_slots") instanceof Map<?, ?> m ? m : Map.of();
        return new SpliceLimits(d.maxHookSteps(), d.maxOutputs(), d.maxSegments(),
                (int) numberOr(group, "output_bytes", d.maxGroupBytes()), numberOr(group, "backing_bytes", d.maxBackingBytes()),
                (int) numberOr(options, "queue_items", d.maxQueuedUnits()), numberOr(options, "queue_bytes", d.maxQueuedBytes()),
                Duration.ofNanos(numberOr(options, "total_wait_cap_us", 0) * 1000),
                (int) numberOr(cleanup, "reliable", 1), (int) numberOr(cleanup, "datagram_groups", 4));
    }

    void install() throws Exception {
        var args = args("install");
        var limiter = new Credit(0, args.get("limiter") instanceof Map<?, ?> l ? num(l, "max_burst_bytes") : Long.MAX_VALUE);
        var options = SpliceOptions.builder().limits(limits(args)).chunk((int) num(args, "chunk_bytes"))
                .packetLimits((int) num(args, "max_packet_payload"), (int) num(args, "opaque_value_cap")).aToB(limiter);
        try (var legs = new Legs()) {
            assertThrows(IllegalArgumentException.class, () -> legs.controlled(options));
            assertEquals(0, legs.input.in.readOffset());
            assertEquals(0, limiter.calls.get());
        }
    }

    void waitCap() throws Exception {
        Map<?, ?> hook = (Map<?, ?>) setup.get("hook");
        var options = SpliceOptions.builder().limits(limits(setup)).hooksAToB(new SpliceHooks(List.of(unit ->
                new SpliceHook.Pass(SpliceWait.after(Duration.ofNanos(num(hook, "wait_until_us") * 1000),
                        Duration.ofDays(1)))), List.of(), List.of()));
        try (var legs = new Legs()) {
            var relay = legs.controlled(options);
            legs.source.write(SpecFixtures.hex(args("receive_reliable_bytes").get("hex")));
            until(relay.aToB(), s -> s.hookCalls() == 1);
            for (var step : steps) {
                if ("advance".equals(step.get("op"))) clock.advance(Duration.ofNanos(num(step, "at_us") * 1000 - clock.now()));
                if (!(step.get("values") instanceof Map<?, ?> values)) continue;
                if (values.containsKey("end")) assertEquals(new SpliceEnd.WaitTimeout(), await(relay.aToB().finished()));
                if (values.containsKey("running")) assertFalse(relay.aToB().finished().toCompletableFuture().isDone());
                if (values.containsKey("first_carrier_bytes")) until(relay.aToB(), s ->
                        s.settlement().snapshot().firstCarrierBytes() == num(values, "first_carrier_bytes"));
                if (values.containsKey("received_reliable_hex")) {
                    String hex = (String) values.get("received_reliable_hex");
                    if (hex.isEmpty()) assertEquals(0, legs.target.stats().bytesWritten());
                    else legs.expectBytes(hex);
                }
            }
            relay.abort(0);
            await(relay.result());
        }
    }

    void capsuleTail() throws Exception {
        try (var legs = new Legs()) {
            var relay = Zmux.splicePackets(legs.input, legs.target);
            for (var write : actions("receive_reliable_bytes")) legs.source.write(SpecFixtures.hex(write.get("hex")));
            String prefix = (String) expected("handed_off_reliable_hex");
            legs.expectBytes(prefix);
            if (!actions("receive_fin").isEmpty()) legs.source.finish();
            var reset = assertThrows(StreamResetException.class, () -> legs.peer.inputStream().read());
            assertEquals(expected("code"), reset.code());
            if (Boolean.TRUE.equals(setup.get("target_reset_at"))) assertEquals(expected("reliable_size"), legs.peer.in.applicationOffset(reset.reliableSize()));
            assertEquals(prefix.length() / 2, legs.target.stats().bytesWritten());
            legs.peer.finish();
            assertInstanceOf(SpliceEnd.PacketError.class, await(relay).aToB().end());
        }
    }

    final java.util.concurrent.ConcurrentMap<SpliceOptions.Work, Integer> work = new java.util.concurrent.ConcurrentHashMap<>();
    void observe(SpliceUnit.Direction direction, SpliceOptions.Work operation) {
        if (direction == SpliceUnit.Direction.A_TO_B) work.merge(operation, 1, Integer::sum);
    }

    void staticBytes() throws Exception {
        try (var legs = new Legs()) {
            var relay = Zmux.splice(legs.input, legs.target, SpliceOptions.builder().probe(this::observe).build());
            legs.source.write(SpecFixtures.hex(args("receive_reliable_bytes").get("hex")));
            legs.source.finish();
            legs.expectBytes((String) expected("received_reliable_hex"));
            assertEquals(-1, legs.peer.inputStream().read());
            legs.peer.finish();
            var done = await(relay).aToB();
            assertEquals(0, done.hookCalls());
            assertEquals(Map.of(), work);
            assertEquals(0, done.inputUnits());
            assertEquals(0, done.payloadCopyBytes());
        }
    }

    void injectionFin(boolean accepted) throws Exception {
        try (var legs = new Legs()) {
            var options = SpliceOptions.builder();
            if (accepted) options.hooksAToB(new SpliceHooks(List.of(unit -> new SpliceHook.Pass(
                    SpliceWait.after(Duration.ofNanos(num((Map<?, ?>) setup.get("hook"), "ready_at_us") * 1000),
                            Duration.ofSeconds(1)))), List.of(), List.of()));
            var relay = legs.controlled(options);
            if (!accepted) {
                legs.source.finish();
                assertEquals(-1, legs.peer.inputStream().read());
            }
            var ticket = relay.aToB().injectStream(ByteBuffer.wrap(SpecFixtures.hex(args("inject").get("hex"))));
            if (accepted) {
                var identity = assertInstanceOf(SpliceController.Accepted.class, await(ticket.admission()));
                assertEquals(expected("generation"), identity.generation());
                legs.source.finish();
                assertEquals(0, legs.target.stats().bytesWritten());
                long at = steps.stream().filter(s -> "advance".equals(s.get("op"))).mapToLong(s -> num(s, "at_us")).findFirst().orElseThrow();
                clock.advance(Duration.ofNanos(at * 1000));
                var submitted = assertInstanceOf(SpliceController.Submitted.class, await(ticket.disposition()));
                assertEquals(identity, submitted.identity());
                assertEquals(expected("accepted_bytes"), submitted.acceptedBytes());
                assertEquals(expected("enqueued_items"), (long) submitted.enqueuedItems());
                assertEquals('a', legs.peer.inputStream().read());
                assertEquals(-1, legs.peer.inputStream().read());
            } else assertEquals(new SpliceController.Rejected(SpliceController.Rejection.CLOSED), await(ticket.disposition()));
            legs.peer.finish();
            assertInstanceOf(SpliceEnd.Finished.class, await(relay.result()).aToB().end());
        }
    }

    void groupAdmission(String id) throws Exception {
        Map<?, ?> limiter = setup.get("limiter") instanceof Map<?, ?> l ? l : Map.of();
        long balance = numberOr(limiter, "available_bytes", numberOr(setup, "exact_credit", Long.MAX_VALUE));
        var credit = new Credit(balance, numberOr(limiter, "max_burst_bytes", Long.MAX_VALUE));
        var opts = SpliceOptions.builder().aToB(credit).limits(limits(setup));
        if (setup.containsKey("hop_ttl_us")) opts.packetsAToB(new DatagramSendOptions(
                new DatagramSendOptions.Deadline.After(Duration.ofNanos(num(setup, "hop_ttl_us") * 1000)), null, null, null, null));
        opts.hooksAToB(new SpliceHooks(List.of(), List.of(), List.of(unit -> {
            if (setup.containsKey("output_items")) {
                var items = new ArrayList<SpliceUnit.Output>();
                for (int i = 0; i < num(setup, "output_items"); i++) items.add(unit.allocate(0));
                return new SpliceHook.Replace(items);
            }
            return replace(unit, (List<?>) ((Map<?, ?>) setup.get("hook")).get("replace_payloads_hex"), null);
        })));
        try (var legs = new Legs()) {
            int filler = 0;
            if (setup.containsKey("target_stream_credit_bytes")) {
                long available = legs.target.sendCapacity();
                filler = Math.toIntExact(available - num(setup, "target_stream_credit_bytes"));
                legs.target.write(new byte[filler]);
                legs.target.flush();
            }
            var relay = legs.controlled(opts);
            legs.receive(args("receive_datagram"));
            var direction = relay.aToB();
            switch (id) {
                case "empty-replacements-still-consume-group-slots" -> {
                    var failure = assertInstanceOf(SpliceEnd.HookFailed.class, await(direction.finished()));
                    assertEquals(expected("cause"), failure.cause().orElseThrow().getMessage());
                    assertEquals(0, credit.calls.get());
                }
                case "short-target-queue-rejects-group-before-publication", "short-group-reservation-publishes-nothing" -> {
                    var reason = setup.containsKey("target_datagram_queue_items")
                            ? SpliceDirection.Drop.DROPPED_QUEUE_FULL : SpliceDirection.Drop.DROPPED_LIMITER;
                    var done = until(direction, d -> d.dropCounts().getOrDefault(reason, 0L) == 1L);
                    assertEquals(0, done.enqueuedItems());
                    assertEquals(0, done.settlement().snapshot().acceptedBytes());
                    assertEquals(1, done.hookCalls());
                    until(direction, d -> d.preAdmissionRefundedBytes() == balance);
                    assertEquals(balance, credit.balance.get());
                    assertFalse(direction.finished().toCompletableFuture().isDone());
                }
                case "structural-fallback-commits-whole-group" -> {
                    legs.expectBytes((String) expected("received_reliable_hex"));
                    var done = until(direction, d -> d.settlement().snapshot().firstCarrierBytes() == (long) expected("first_carrier_bytes"));
                    assertEquals(expected("capsule_count"), done.datagramsAsCapsules());
                    assertEquals(expected("enqueued_items"), done.enqueuedItems());
                }
                case "fallback-expires-while-waiting-for-credit", "started-fallback-finishes-group-after-expiry" -> {
                    boolean started = num(setup, "target_stream_credit_bytes") != 0;
                    until(direction, d -> credit.balance.get() == 0 && (!started || d.settlement().snapshot().acceptedBytes() == 1));
                    clock.advance(Duration.ofNanos(num(setup, "hop_ttl_us") * 1000));
                    assertEquals(filler, legs.peer.inputStream().readNBytes(filler).length);
                    if (started) {
                        String expected = (String) steps.stream().filter(step -> step.get("values") instanceof Map<?, ?> v
                                && v.containsKey("received_reliable_hex")).reduce((a, b) -> b)
                                .map(step -> ((Map<?, ?>) step.get("values")).get("received_reliable_hex")).orElseThrow();
                        legs.expectBytes(expected);
                        until(direction, d -> d.settlement().snapshot().firstCarrierBytes() == 11);
                    } else {
                        var done = until(direction, d -> d.dropCounts().containsKey(SpliceDirection.Drop.DROPPED_EXPIRED));
                        assertEquals(0, done.settlement().snapshot().acceptedBytes());
                        until(direction, d -> d.preAdmissionRefundedBytes() == balance);
                        legs.source.write(SpecFixtures.hex(args("receive_reliable_bytes").get("hex")));
                        legs.expectBytes("210161");
                    }
                }
                default -> throw new AssertionError(id);
            }
            relay.abort(0);
            await(relay.result());
        }
    }

    void lateGrant() throws Exception {
        var entered = new CompletableFuture<Long>();
        var raw = new CompletableFuture<Long>() {
            @Override public boolean cancel(boolean interrupt) { return false; }
        };
        var returned = new CompletableFuture<Long>();
        var count = new AtomicInteger();
        Credit credit = new Credit(3, Long.MAX_VALUE) {
            @Override public CompletionStage<Long> acquire(long min, long max) {
                entered.complete(max);
                return raw;
            }
            @Override public void refund(long n) { count.incrementAndGet(); returned.complete(n); }
        };
        var events = new java.util.concurrent.CopyOnWriteArrayList<SpliceController.Event>();
        try (var legs = new Legs()) {
            var relay = legs.controlled(SpliceOptions.builder().aToB(credit)
                    .events((dir, e) -> { if (dir == SpliceUnit.Direction.A_TO_B) events.add(e); })
                    .hooksAToB(new SpliceHooks(List.of(), List.of(unit -> new SpliceHook.Pass()), List.of())));
            legs.source.write(SpecFixtures.hex(args("receive_reliable_bytes").get("hex")));
            assertEquals(((List<?>) expected("pending_requests")).getFirst(), await(entered));
            long code = num(args("abort"), "code");
            relay.abort(code);
            assertEquals(new SpliceEnd.Cancelled(code), await(relay.aToB().finished()));
            raw.complete(num(args("complete_acquire"), "granted_bytes"));
            assertEquals(((List<?>) expected("refund_calls")).getFirst(), await(returned));
            assertEquals(1, count.get());
            assertEquals(List.of(new SpliceController.DirectionFinished(new SpliceEnd.Cancelled(code))), events);
            assertEquals(0, legs.target.stats().bytesWritten());
            assertEquals(expected("pre_admission_refunded_bytes"), await(relay.aToB().snapshot()).preAdmissionRefundedBytes());
            await(relay.result());
        }
    }

    void borrowedCallback() throws Exception {
        var entered = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        var seen = new CompletableFuture<Byte>();
        try (var legs = new Legs()) {
            var relay = legs.controlled(SpliceOptions.builder().hooksAToB(new SpliceHooks(List.of(unit -> {
                entered.complete(null);
                await(release);
                seen.complete(unit.item(0).get(0));
                return new SpliceHook.Pass();
            }), List.of(), List.of())));
            legs.source.write(SpecFixtures.hex(args("receive_owned_reliable_bytes").get("hex")));
            await(entered);
            long code = num(args("abort"), "code");
            relay.abort(code);
            release.complete(null);
            assertEquals(SpecFixtures.hex(expected("observed_hex"))[0], await(seen));
            var result = await(relay.result()).aToB();
            assertEquals(new SpliceEnd.Cancelled(code), result.end());
            assertEquals(0, result.enqueuedItems());
            assertEquals(0, legs.target.stats().bytesWritten());
        }
    }

    void generation(String id) throws Exception {
        var seen = new java.util.concurrent.CopyOnWriteArrayList<Long>();
        try (var legs = new Legs()) {
            if (id.equals("injections-straddle-hook-replacement")) {
                var oldPolicy = (Map<?, ?>) setup.get("hook");
                var relay = legs.controlled(SpliceOptions.builder().hooksAToB(new SpliceHooks(List.of(unit ->
                        replace(unit, (List<?>) oldPolicy.get("replace_payloads_hex"), SpliceWait.after(
                                Duration.ofNanos(num(oldPolicy, "wait_until_us") * 1000), GUARD))), List.of(), List.of())));
                var a = actions("inject");
                var old = relay.aToB().injectStream(ByteBuffer.wrap(SpecFixtures.hex(a.getFirst().get("hex"))));
                var held = ByteBuffer.wrap(SpecFixtures.hex(a.getLast().get("hex")));
                assertEquals(1, assertInstanceOf(SpliceController.Accepted.class, await(old.admission())).generation());
                Map<?, ?> next = (Map<?, ?>) ((List<?>) args("replace_hooks").get("hooks")).getFirst();
                assertEquals(2, await(relay.aToB().replaceHooks(new SpliceHooks(List.of(unit ->
                        replace(unit, (List<?>) next.get("replace_payloads_hex"), null)), List.of(), List.of()))));
                var newer = relay.aToB().injectStream(held);
                assertEquals(2, assertInstanceOf(SpliceController.Accepted.class, await(newer.admission())).generation());
                assertEquals(0, legs.target.stats().bytesWritten());
                clock.advance(Duration.ofNanos(num(oldPolicy, "wait_until_us") * 1000));
                legs.expectBytes("6263");
                assertEquals(1, assertInstanceOf(SpliceController.Submitted.class, await(old.disposition())).identity().generation());
                assertEquals(2, assertInstanceOf(SpliceController.Submitted.class, await(newer.disposition())).identity().generation());
                relay.abort(0);
                await(relay.result());
                return;
            }
            var relay = legs.controlled(SpliceOptions.builder().probe(this::observe).hooksAToB(new SpliceHooks(List.of(), List.of(unit -> {
                seen.add(unit.metadata().generation());
                return new SpliceHook.Pass();
            }), List.of())));
            if (id.equals("controlled-detached")) {
                assertEquals(2, await(relay.aToB().replaceHooks(SpliceHooks.NONE)));
                int batches = 0;
                for (var batch : actions("receive_read_batch")) {
                    var joined = new java.io.ByteArrayOutputStream();
                    for (Object hex : (List<?>) batch.get("segments_hex")) joined.writeBytes(SpecFixtures.hex(hex));
                    legs.source.write(joined.toByteArray());
                    legs.expectBytes(HexFormat.of().formatHex(joined.toByteArray()));
                    assertEquals(Map.of(SpliceOptions.Work.BOUNDARY_READ, ++batches), work);
                }
                assertEquals(0, await(relay.aToB().snapshot()).hookCalls());
            } else {
                var writes = actions("receive_reliable_bytes");
                legs.source.write(SpecFixtures.hex(writes.getFirst().get("hex")));
                until(relay.aToB(), d -> legs.input.in.applicationOffset(legs.input.in.readOffset()) == 1);
                assertEquals(2, await(relay.aToB().replaceHooks(new SpliceHooks(List.of(), List.of(unit -> {
                    seen.add(unit.metadata().generation());
                    return new SpliceHook.Drop();
                }), List.of()))));
                legs.source.write(SpecFixtures.hex(writes.getLast().get("hex")));
                legs.expectBytes((String) expected("received_reliable_hex"));
                until(relay.aToB(), d -> d.hookCalls() == 2);
                assertEquals(expected("observed_generations"), seen);
            }
            relay.abort(0);
            await(relay.result());
        }
    }

    void ownership(String id) throws Exception {
        var args = args("receive_owned_datagram");
        byte[] input = SpecFixtures.hex(args.get("hex"));
        int capacity = (int) num(args, "backing_bytes");
        var released = new AtomicInteger();
        var leases = new ReceiveLoans(numberOr(setup, "borrow_budget_bytes", capacity), 1);
        var limits = limits(setup);
        var budget = new SplicePayload.Budget(limits);
        var metadata = new SpliceUnit.Metadata(SpliceUnit.Direction.A_TO_B, SpliceUnit.Shape.DATAGRAM,
                SpliceUnit.Origin.FORWARDED, 1, 1, java.util.OptionalLong.empty(), java.util.OptionalLong.empty(),
                java.util.OptionalLong.empty(), java.util.OptionalLong.empty(), java.util.OptionalLong.of(0),
                false, Preamble.EMPTY, "", "");
        var mirror = new SpliceMirror(Set.of(SpliceMirror.Phase.INPUT), SpliceMirror.Ownership.LEASE, 2, 1024, 32);
        int rounds = actions("receive_owned_datagram").size();
        for (int j = 0; j < rounds; j++) {
            var data = ByteBuffer.allocate(capacity).put(input).flip();
            var root = new ReadBuf(data, b -> released.incrementAndGet());
            assertNull(leases.transfer(root, true, ignored -> false));
            assertEquals(1, root.refs());
            assertEquals(0, leases.bytes());
            var moved = leases.transfer(root, true, b -> budget.fits(b.root(), b.capacity()));
            assertNotNull(moved);
            assertEquals(0, leases.bytes());
            var payload = new SplicePayload(budget);
            assertTrue(payload.append(new StreamRecv.Chunk(data, moved, false)));
            moved.unref();
            assertEquals(capacity, budget.bytes());
            try (var group = new SpliceGroup(metadata, payload, 0, -1, Long.MAX_VALUE, null, null)) {
                switch (id) {
                    case "peak-backing-charge-includes-input-and-outputs" -> {
                        var request = args("build_replacement_range");
                        var decision = group.transform(List.of(unit -> {
                            unit.allocate((int) num(request, "length"), (int) num(request, "backing_bytes"));
                            return new SpliceHook.Pass();
                        }), limits, capacity, () -> {});
                        var failed = assertInstanceOf(SpliceEnd.HookFailed.class, decision);
                        assertEquals(expected("cause"), failed.cause().orElseThrow().getMessage());
                    }
                    case "shared-root-two-children" -> {
                        var ranges = (List<?>) ((Map<?, ?>) setup.get("hook")).get("replace_with_input_ranges");
                        assertNull(group.transform(List.of(unit -> {
                            var outputs = new ArrayList<SpliceUnit.Output>();
                            for (Object range : ranges) {
                                var r = (List<?>) range;
                                outputs.add(unit.item(0).slice(((Number) r.get(0)).intValue(), ((Number) r.get(1)).intValue()));
                            }
                            return new SpliceHook.Replace(outputs);
                        }), limits, 32, () -> {}));
                        group.prepare(false, limits);
                        assertEquals(expected("group_peak_root_bytes"), budget.bytes());
                        assertEquals(expected("output_payload_descriptors"), (long) group.output.stream().mapToInt(SplicePayload::segments).sum());
                        try (var legs = new Legs()) {
                            var chunks = new StreamRecv.Chunk[group.output.size()];
                            var charges = new io.zmux.internal.buffer.CarrierCharge[chunks.length];
                            var confirmations = new java.util.concurrent.LinkedBlockingQueue<Long>();
                            for (int i = 0; i < chunks.length; i++) {
                                var item = group.output.get(i);
                                assertEquals(0, item.copyForDatagram());
                                chunks[i] = item.datagram();
                                assertSame(data.array(), chunks[i].data().array());
                                charges[i] = new io.zmux.internal.buffer.CarrierCharge(item.length(), true, confirmations::add);
                            }
                            var target = legs.target.session;
                            long submitted = target.driver.now();
                            var resolved = (io.zmux.internal.core.Session.DatagramOptions) target.run(op ->
                                    target.core.resolveDatagram(legs.target.st, DatagramSendOptions.INHERIT, submitted));
                            int largest = java.util.Arrays.stream(chunks).mapToInt(chunk -> chunk.data().remaining()).max().orElseThrow();
                            long groupBytes = java.util.Arrays.stream(chunks).mapToLong(chunk -> chunk.data().remaining()).sum();
                            var status = await(target.sendDatagramGroupAsync(legs.target.st, 0, chunks, largest, groupBytes,
                                    resolved, submitted, io.zmux.internal.core.Session.DatagramMode.DATAGRAM_ONLY, charges, () -> true));
                            assertEquals(io.zmux.internal.core.Session.DatagramAdmission.QUEUED, status);
                            for (var charge : charges) charge.release();
                            long handedOff = 0;
                            for (Object hex : (List<?>) expected("accepted_payloads_hex")) {
                                var send = legs.channel.pending.poll(GUARD.toMillis(), TimeUnit.MILLISECONDS);
                                assertNotNull(send);
                                var bytes = SpecFixtures.hex(hex);
                                legs.channel.accept(send, bytes.length);
                                assertArrayEquals(bytes, legs.peer.receiveDatagram(GUARD).payload());
                                handedOff += confirmations.poll(GUARD.toMillis(), TimeUnit.MILLISECONDS);
                            }
                            assertEquals(expected("first_carrier_bytes"), handedOff);
                        }
                    }
                    case "mirror-lease-would-pin-receive-loan" ->
                            assertEquals(-1, mirror.publish(metadata, SpliceMirror.Phase.INPUT, 0, payload));
                    case "receive-to-splice-at-both-capacity-limits" -> {
                        var opts = SpliceOptions.builder().limits(limits).packetLimits(32, 32).chunk(32).build();
                        var policy = new SplicePolicy(true, opts, SpliceHooks.NONE, true, () -> {},
                                new SpliceSettlement(), metadata.direction(), null);
                        assertTrue(policy.queue(group));
                        assertEquals(0, payload.copyForDatagram());
                        assertSame(root, payload.datagram().buf().root());
                        policy.releaseQueue(group.queueCharge);
                    }
                    default -> throw new AssertionError(id);
                }
            }
            assertEquals(j + 1, released.get());
            assertEquals(0, budget.bytes());
        }
        assertNull(mirror.poll());
        if (id.startsWith("mirror")) assertEquals(rounds, mirror.misses());
        mirror.close();
    }

    void resetAfterInjection() throws Exception {
        try (var legs = new Legs()) {
            var relay = legs.controlled(SpliceOptions.builder());
            var inject = args("inject");
            var ticket = relay.aToB().injectCapsule(num(inject, "type"), ByteBuffer.wrap(SpecFixtures.hex(inject.get("hex"))));
            assertInstanceOf(SpliceController.Submitted.class, await(ticket.disposition()));
            var reset = args("receive_reset_at");
            byte[] source = SpecFixtures.hex(args("receive_reliable_bytes").get("hex"));
            legs.source.write(source);
            legs.source.markReliableBoundary();
            legs.source.session.run(op -> {
                var sender = legs.source.session.core.sending();
                sender.write(legs.source.st, ByteBuffer.allocate((int) num(reset, "final_size") - source.length), false);
                sender.reset(legs.source.st, num(reset, "code"));
                return true;
            });
            legs.expectBytes("21027879220162");
            var end = assertThrows(StreamResetException.class, () -> {
                while (legs.peer.inputStream().read() >= 0) { }
            });
            assertEquals(expected("reliable_size"), legs.peer.in.applicationOffset(end.reliableSize()));
            assertEquals(expected("code"), end.code());
            assertEquals(new SpliceEnd.Reset(0), await(relay.aToB().finished()));
            legs.peer.finish();
            await(relay.result());
        }
    }

    void datagramDelayOrMirror(boolean delayed) throws Exception {
        try (var legs = new Legs()) {
            var opts = SpliceOptions.builder().limits(limits(setup));
            SpliceMirror mirror = null;
            if (delayed) {
                Map<?, ?> times = (Map<?, ?>) ((Map<?, ?>) setup.get("hook")).get("ready_at_us_by_payload");
                opts.packetsAToB(new DatagramSendOptions(
                        new DatagramSendOptions.Deadline.After(Duration.ofNanos(num(setup, "hop_ttl_us") * 1000)),
                        DatagramSendOptions.Carriage.REQUIRE_NATIVE, null, null, null)).hooksAToB(new SpliceHooks(
                        List.of(), List.of(), List.of(unit -> new SpliceHook.Pass(SpliceWait.after(Duration.ofNanos(
                                num(times, HexFormat.of().toHexDigits(unit.item(0).get(0))) * 1000), GUARD)))));
            } else {
                Map<?, ?> config = (Map<?, ?>) setup.get("mirror");
                int bytes = (int) num(config, "backing_bytes");
                mirror = new SpliceMirror(Set.of(SpliceMirror.Phase.INPUT), SpliceMirror.Ownership.COPY,
                        (int) num(config, "items"), bytes, bytes);
                opts.mirrorAToB(mirror);
            }
            var relay = legs.controlled(opts);
            for (var input : actions("receive_datagram")) legs.receive(input);
            if (delayed) {
                until(relay.aToB(), d -> d.dropCounts().containsKey(SpliceDirection.Drop.DROPPED_QUEUE_FULL));
                clock.advance(Duration.ofNanos(5000));
            }
            for (Object hex : (List<?>) expected("accepted_payloads_hex"))
                assertArrayEquals(SpecFixtures.hex(hex), legs.peer.receiveDatagram(GUARD).payload());
            if (delayed) {
                clock.advance(Duration.ofNanos(5000));
                var state = until(relay.aToB(), d -> d.dropCounts().containsKey(SpliceDirection.Drop.DROPPED_EXPIRED));
                assertEquals(expected("hook_calls"), state.hookCalls());
            } else {
                var state = until(relay.aToB(), d -> d.enqueuedItems() == 2);
                assertEquals(expected("mirror_misses"), state.mirrorMisses());
                assertEquals(expected("mirror_copy_bytes"), state.mirrorCopyBytes());
                assertEquals(SpecFixtures.hex(((List<?>) expected("payloads_hex")).getFirst())[0], mirror.poll().payload().getFirst().get(0));
                assertNull(mirror.poll());
                mirror.close();
            }
            until(relay.aToB(), d -> d.settlement().snapshot().firstCarrierBytes() == 2);
            relay.abort(0);
            await(relay.result());
        }
    }

    void pausedDatagrams() throws Exception {
        var observed = new java.util.concurrent.LinkedBlockingQueue<SpliceController.Event>();
        try (var legs = new Legs()) {
            var relay = legs.controlled(SpliceOptions.builder()
                    .events((dir, event) -> { if (dir == SpliceUnit.Direction.A_TO_B) observed.add(event); })
                    .hooksAToB(new SpliceHooks(List.of(), List.of(), List.of(unit -> new SpliceHook.Pass()))));
            var direction = relay.aToB();
            long token = await(direction.pause());
            assertEquals(new SpliceController.PauseAcknowledged(token), observed.poll(5, TimeUnit.SECONDS));
            long count = 0;
            for (var arrival : actions("receive_datagram")) {
                legs.input.datagramReceiveLifetime(Duration.ofNanos(num(arrival, "receive_expiry_at_us") * 1000));
                legs.receive(arrival);
                long queued = ++count;
                until(direction, state -> legs.input.stats().datagramsReceived() == queued);
            }
            assertEquals(0, await(direction.snapshot()).hookCalls());
            clock.advance(Duration.ofNanos(5000));
            until(direction, state -> legs.input.stats().datagramsDroppedRecv() == 1);
            assertTrue(await(direction.resume(token)));
            assertArrayEquals(SpecFixtures.hex(((List<?>) expected("accepted_payloads_hex")).getFirst()),
                    legs.peer.receiveDatagram(GUARD).payload());
            var result = await(direction.snapshot());
            assertEquals(1, result.hookCalls());
            assertEquals(0, result.datagramsDropped());
            relay.abort(0);
            await(relay.result());
        }
    }

    void cleanupSlots() throws Exception {
        var gate = new java.util.concurrent.LinkedBlockingQueue<CompletableFuture<Long>>();
        var refunds = new java.util.concurrent.LinkedBlockingQueue<Long>();
        Credit limiter = new Credit(0, Long.MAX_VALUE) {
            @Override public long tryAcquire(long min, long max) {
                calls.incrementAndGet();
                var result = new CompletableFuture<Long>();
                gate.add(result);
                return result.orTimeout(5, TimeUnit.SECONDS).join();
            }
            @Override public void refund(long n) { refunds.add(n); }
        };
        try (var legs = new Legs()) {
            var relay = legs.controlled(SpliceOptions.builder().limits(limits(setup)).aToB(limiter)
                    .packetsAToB(new DatagramSendOptions(new DatagramSendOptions.Deadline.After(
                            Duration.ofNanos(num(setup, "hop_ttl_us") * 1000)), null, null, null, null)));
            var arrivals = actions("receive_datagram");
            legs.receive(arrivals.getFirst());
            var late = gate.poll(5, TimeUnit.SECONDS);
            assertNotNull(late);
            clock.advance(Duration.ofNanos(num(setup, "hop_ttl_us") * 1000));
            until(relay.aToB(), d -> d.dropCounts().containsKey(SpliceDirection.Drop.DROPPED_EXPIRED));
            legs.receive(arrivals.get(1));
            until(relay.aToB(), d -> d.dropCounts().containsKey(SpliceDirection.Drop.DROPPED_BLOCKED));
            assertEquals(1, limiter.calls.get());
            late.complete(1L);
            assertEquals(1L, refunds.poll(5, TimeUnit.SECONDS));
            legs.receive(arrivals.getLast());
            var grant = gate.poll(5, TimeUnit.SECONDS);
            assertNotNull(grant);
            grant.complete(1L);
            assertArrayEquals(SpecFixtures.hex(arrivals.getLast().get("hex")), legs.peer.receiveDatagram(GUARD).payload());
            assertEquals(2, limiter.calls.get());
            relay.abort(0);
            await(relay.result());
        }
    }
}
