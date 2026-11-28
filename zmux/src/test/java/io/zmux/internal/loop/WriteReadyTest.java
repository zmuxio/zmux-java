package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.PipeState;
import io.zmux.carrier.OutboundBatch;
import io.zmux.carrier.CarrierChannel;
import io.zmux.internal.buffer.CarrierCharge;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.Action;
import io.zmux.internal.core.Batch;
import io.zmux.internal.core.BatchFixture;
import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.DatagramChannel;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.SessionState;
import io.zmux.internal.core.Time;
import io.zmux.internal.core.TimerHeap;
import io.zmux.internal.memory.MemoryBudget;
import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.FrameDecoder;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.FrameKind;
import io.zmux.internal.wire.TransportParams;
import io.zmux.internal.wire.VarInt;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WriteReadyTest {
    private static final class Host extends SessionHost {
        private long wake = Time.NEVER;
        private int wakeChanges;
        private boolean rejectPosts;
        private long now;

        Host() { super(0); }
        @Override public boolean inLoop() { return !rejectPosts; }
        @Override public boolean tryExecute(Runnable task) {
            if (rejectPosts) return false;
            task.run();
            return true;
        }
        @Override protected void requestRoundEnd() {}
        @Override protected void wakeAt(long deadline) { wake = deadline; wakeChanges++; }
        @Override protected long timerNow() { return now; }
    }

    private static final class Out extends CarrierActionsTest.Carrier {
        private final NativeChannel channel;
        private int writableChecks;
        private int samples;
        private int writes;
        private boolean hold;
        private boolean retainNext;
        private OutboundBatch.Retained retained;
        private ByteBuffer retainedView;
        private List<ByteBuffer> wire;


        Out(Host host) { this(host, null); }

        Out(Host host, NativeChannel channel) {
            super(host, Action.DIR_BIDI, null, 0);
            this.channel = channel;
        }

        @Override protected void begin() { openReader(); }
        @Override protected boolean accepting() { return !closed; }
        @Override public CarrierChannel channel() { return channel; }
        @Override public TransportInfo info() {
            return channel == null ? super.info() : new TransportInfo(info.carrier().withChannel(channel), "", null, 0, 0);
        }
        @Override public boolean writable() { writableChecks++; return !writing() && !closed; }
        @Override public long unsent() { samples++; return 0; }
        @Override protected void send(OutboundBatch b) {
            writes++;
            if (wire != null) {
                for (int i = 0; i < b.segmentCount(); i++) {
                    var data = b.segment(i);
                    wire.add(ByteBuffer.allocate(data.remaining()).put(data).flip());
                }
            }
            if (retainNext) {
                retainNext = false;
                retained = b.retain();
                retainedView = b.segment(0);
            }
            if (!hold) writeDone(null);
        }

        void releaseWrite() {
            hold = false;
            writeDone(null);
        }
    }

    private static final class NativeChannel implements CarrierChannel, DatagramChannel {
        int room;
        int submitted;
        Runnable query, stop = () -> {}, unwatch;
        boolean hold;
        java.util.function.Consumer<Outcome> receipt;

        @Override public long channelStreamId() { return -1; }
        @Override public int maxChannelDatagram() {
            if (query != null) query.run();
            return room;
        }
        @Override public int channelDatagramLimit(int currentRoom) { return 1200; }
        @Override public Runnable watch(Runnable changed) { return unwatch; }
        @Override public Runnable receive(Sink sink) { return stop; }
        @Override public void send(byte[] datagram, long expiry, java.util.function.Consumer<Outcome> completed) {
            submitted++;
            if (hold) receipt = completed;
            else completed.accept(Outcome.ACCEPTED);
        }
    }


    private static DriverHooks fixedClock(Host host) {
        return DriverHooks.NONE.withClock(new io.zmux.ClockDomain(new io.zmux.ClockDomain.Source() {
            @Override public long nanoTime() { return host.timerNow(); }
            @Override public java.time.Instant wallTime() { return java.time.Instant.EPOCH; }
            @Override public Runnable schedule(long delay, Runnable task) {
                var timer = new TimerHeap.Timer() {
                    @Override public void fire(long now) { task.run(); }
                };
                host.schedule(timer, Time.add(host.timerNow(), delay));
                return () -> host.schedule(timer, Time.NEVER);
            }
        }));
    }

    private static void establishNative(Host host, SessionRuntime driver, Out out) {
        driver.attachBootstrap(out);
        host.endRound();
        var peer = datagrams(CoreConfig.Role.RESPONDER);
        var info = out.info().carrier();
        peer.attachBootstrap(info, info);
        var batch = new Batch();
        assertTrue(peer.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, 0));
        deliver(out, ByteBuffer.wrap(BatchFixture.flat(batch)));
        batch.discard();
        host.endRound();
        assertTrue(driver.core.features().carrierDatagrams());
    }

    private static void queuePaddedNative(Session core, java.util.List<Long> settled) throws Exception {
        core.setPipeShaper(0, new io.zmux.shaping.Shaper() {
            @Override public java.util.Set<io.zmux.shaping.Capability> capabilities() {
                return java.util.Set.of(io.zmux.shaping.Capability.DATAGRAM_PLAN);
            }
            @Override public void onEvent(io.zmux.shaping.Event event, io.zmux.shaping.Effects effects) {}
            @Override public void plan(io.zmux.shaping.RecordView view, io.zmux.shaping.RecordPlan plan) {}
            @Override public void planDatagram(io.zmux.shaping.DatagramView view, io.zmux.shaping.UnitPlan plan,
                                               io.zmux.shaping.Effects effects) { plan.pad(4); }
        });
        byte[] payload = {42};
        var charge = new CarrierCharge(payload.length, true, settled::add);
        try {
            var options = core.resolveDatagram(null, io.zmux.DatagramSendOptions.INHERIT, 0);
            assertEquals(Session.DatagramAdmission.QUEUED, core.admitDatagram(null, 0, payload, payload.length,
                    options, 0, Session.DatagramMode.DATAGRAM_ONLY, false, false, charge));
        } finally {
            charge.release();
        }
    }

    @Test void adapterQueryFailureAfterTicketCreationReleasesTheUnsentUnit() throws Exception {
        Host host = new Host();
        var channel = new NativeChannel();
        channel.room = 1200;
        var out = new CarrierSetup().own(() -> new Out(host, channel));
        var core = datagrams(CoreConfig.Role.INITIATOR);
        var driver = new SessionRuntime(host, core, fixedClock(host));
        try {
            establishNative(host, driver, out);
            var settled = new java.util.ArrayList<Long>();
            queuePaddedNative(core, settled);
            long sequence = core.datagrams().sendSnapshot().nextSequence();
            var failure = new IllegalStateException("capacity query after creating native ticket");
            channel.query = () -> {
                if (core.datagrams().sendSnapshot().nextSequence() == sequence) return;
                channel.query = null;
                throw failure;
            };
            var writeChannel = SessionRuntime.class.getDeclaredMethod("writeChannel", CarrierSet.Attached.class, CarrierChannel.class);
            writeChannel.setAccessible(true);
            var thrown = assertThrows(java.lang.reflect.InvocationTargetException.class, () ->
                    writeChannel.invoke(driver, driver.carriers().get(0), channel));
            assertSame(failure, thrown.getCause());
            assertEquals(sequence + 1, core.datagrams().sendSnapshot().nextSequence());
            assertEquals(0, channel.submitted);
            assertEquals(java.util.List.of(0L), settled);
            var overhead = core.shapingMeters().overhead();
            assertEquals(0, overhead.policy().reservedBytes());
            assertEquals(io.zmux.shaping.OverheadMeters.Precision.EXACT, overhead.precision());
        } finally {
            host.runShutdownHooks();
        }
    }

    @Test void ownerShutdownSettlesARejectedReceiptPostAndReleasesItsSpliceCharge() throws Exception {
        for (boolean reported : new boolean[] {true, false}) {
            Host host = new Host();
            var channel = new NativeChannel();
            channel.room = 1200;
            channel.hold = true;
            var out = new CarrierSetup().own(() -> new Out(host, channel));
            var core = datagrams(CoreConfig.Role.INITIATOR);
            var driver = new SessionRuntime(host, core, fixedClock(host));
            var settled = new java.util.ArrayList<Long>();
            try {
                establishNative(host, driver, out);
                queuePaddedNative(core, settled);
                host.endRound();
                assertEquals(1, channel.submitted);
                assertEquals(4, core.shapingMeters().overhead().policy().reservedBytes());
                assertTrue(settled.isEmpty());
                if (reported) {
                    host.rejectPosts = true;
                    channel.receipt.accept(CarrierChannel.Outcome.ACCEPTED);
                    host.rejectPosts = false;
                    assertTrue(settled.isEmpty());
                }
            } finally {
                host.rejectPosts = false;
                host.runShutdownHooks();
            }
            channel.receipt.accept(CarrierChannel.Outcome.ACCEPTED);
            assertEquals(java.util.List.of(reported ? 1L : 0L), settled);
            var overhead = core.shapingMeters().overhead();
            assertEquals(reported ? 0 : 4, overhead.policy().reservedBytes());
            assertEquals(reported ? 4 : 0, overhead.measuredBytes());
            assertEquals(reported ? io.zmux.shaping.OverheadMeters.Precision.EXACT
                    : io.zmux.shaping.OverheadMeters.Precision.UNAVAILABLE, overhead.precision());
        }
    }

    @Test void bothAdapterUnregistrationsAndCarrierReleaseRunAfterCallbackFailure() {
        for (boolean failReceive : new boolean[] {true, false}) {
            Host host = new Host();
            var channel = new NativeChannel();
            int[] unregistered = {0};
            channel.stop = () -> {
                unregistered[0]++;
                if (failReceive) throw new IllegalStateException("receive unregister");
            };
            channel.unwatch = () -> {
                unregistered[0]++;
                if (!failReceive) throw new IllegalStateException("watch unregister");
            };
            var out = new Out(host, channel);
            var core = datagrams(CoreConfig.Role.INITIATOR);
            var driver = new SessionRuntime(host, core, fixedClock(host));
            try {
                driver.attachBootstrap(out);
                var attached = driver.carriers().get(0);
                driver.carriers().remove(attached, null);
                assertEquals(2, unregistered[0]);
                assertTrue(out.closed);
                assertTrue(attached.attachment.completion().toCompletableFuture().isDone());
                assertEquals(0, driver.carriers().size());
                driver.carriers().remove(attached, null);
                assertEquals(2, unregistered[0]);
            } finally {
                host.runShutdownHooks();
            }
        }
    }

    @Test void spliceWaitsBeforeConsumingLeaseAndCancellationLeavesItWritable() throws Exception {
        var host = new Host();
        var budget = new MemoryBudget(4L << 20, null);
        var config = CoreConfig.builder().budget(budget);
        config.local().maxStreamsBidi = config.local().maxStreamsUni = 0;
        var local = new Session(config.build(), 0);
        var driver = new SessionRuntime(host, local, DriverHooks.NONE);
        try {
            var stream = local.streams().openStream(false);
            var send = driver.lanes.of(stream).send();
            send.drain(); // Publish a payload lease before exercising receipt admission.
            long committed = budget.used();
            budget.limit(committed);
            var denied = new CarrierCharge(1, false, acknowledged -> assertEquals(0, acknowledged));
            var first = new ReadBuf(ByteBuffer.wrap(new byte[] {1}), null);
            var blocked = send.writeAsync(null, first.data(), first, Long.MAX_VALUE, denied, 0);
            host.endRound();
            assertFalse(blocked.isDone());
            assertEquals(0, SendLane.written(blocked));
            assertEquals(committed, budget.used());
            assertTrue(blocked.cancel());
            denied.release();
            assertEquals(1, send.tryWrite(ByteBuffer.wrap(new byte[] {2})));

            var paid = new CarrierCharge(1, false, acknowledged -> assertEquals(0, acknowledged));
            budget.limit(committed + paid.backingBytes());
            var second = new ReadBuf(ByteBuffer.wrap(new byte[] {3}), null);
            var sent = send.writeAsync(null, second.data(), second, Long.MAX_VALUE, paid, 0);
            assertTrue(sent.isDone());
            assertEquals(1, SendLane.written(sent));
            assertEquals(committed + paid.backingBytes(), budget.used());
            paid.release();
            send.drain();
        } finally {
            local.close(0, null);
            host.endRound();
            host.runShutdownHooks();
        }
        assertEquals(0, budget.used());
    }

    @Test void nativeCapacityWithoutASignalRechecksOnlyPendingPackets() throws Exception {
        Host host = new Host();
        var channel = new NativeChannel();
        var out = new CarrierSetup().own(() -> new Out(host, channel));
        var local = datagrams(CoreConfig.Role.INITIATOR);
        var driver = new SessionRuntime(host, local, DriverHooks.NONE);
        try {
            driver.attachBootstrap(out);
            host.endRound();
            var peer = datagrams(CoreConfig.Role.RESPONDER);
            var info = out.info().carrier();
            peer.attachBootstrap(info, info);
            var batch = new Batch();
            assertTrue(peer.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, 0));
            deliver(out, ByteBuffer.wrap(BatchFixture.flat(batch)));
            batch.discard();
            host.endRound();
            assertTrue(local.features().carrierDatagrams());
            int before = host.timerCount();
            var nativeOnly = new io.zmux.DatagramSendOptions(null, io.zmux.DatagramSendOptions.Carriage.REQUIRE_NATIVE, null, null, null);
            local.sendDatagram(null, 0, new byte[] {42}, nativeOnly, local.now());
            host.endRound();
            assertEquals(before + 1, host.timerCount());
            assertEquals(0, channel.submitted);

            int samples = out.samples;
            long at = host.earliestTimer();
            channel.room = 1200;
            host.runTimers(at - 1);
            host.endRound();
            assertEquals(0, channel.submitted);
            host.runTimers(at);
            host.endRound();
            assertEquals(1, channel.submitted);
            assertEquals(samples, out.samples);
            assertEquals(before, host.timerCount());
        } finally {
            host.runShutdownHooks();
        }
    }

    private static Session datagrams(CoreConfig.Role role) {
        var config = CoreConfig.builder().role(role).transportRole(role).keepAlive(CoreConfig.OFF).datagrams(true);
        config.local().present |= TransportParams.DATAGRAM;
        config.local().maxDatagramFrameSize = 1200;
        return new Session(config.build(), 0);
    }

    @Test void completionRestoresWritabilityAfterTheLastPartialWrite() {
        var host = new Host();
        var core = datagrams(CoreConfig.Role.INITIATOR);
        var driver = new SessionRuntime(host, core, fixedClock(host));
        var out = new CarrierSetup().own(() -> new Out(host));
        try {
            out.hold = true;
            driver.attachBootstrap(out);
            host.endRound();
            assertTrue(out.writing());
            out.writableChanged(false);
            // TCP and TLS report socket readiness before returning the encoded batch.
            out.writableChanged(true);
            assertFalse(core.pipeStats(0, 0, 0).writable());
            out.releaseWrite();
            assertTrue(core.pipeStats(0, 0, 0).writable());
        } finally {
            host.runShutdownHooks();
        }
    }

    @ParameterizedTest(name = "abrupt primary end: {0}")
    @ValueSource(booleans = {false, true})
    void backupRemainsUsableAfterPartialReinjection(boolean abruptPrimaryEnd) throws Exception {
        var fixture = BatchFixture.beforeReinjection();
        var host = new Host();
        host.now = fixture.now();
        var core = fixture.session();
        var primary = new CarrierSetup().own(() -> new Out(host));
        var backup = new CarrierSetup().own(() -> new Out(host));
        var driver = new SessionRuntime(host, core, fixedClock(host));
        backup.wire = new ArrayList<>();
        try {
            driver.carriers().attach(primary, 0, 0, null);
            driver.carriers().attach(backup, 2, 0, null);
            core.setPipeLiveness(0, 0, Time.SECOND, -1, Double.NaN);
            core.setPipeLiveness(2, 0, Time.SECOND, -1, Double.NaN);
            round(host, driver);
            backup.wire.clear();
            backup.hold = true;
            if (abruptPrimaryEnd) {
                // A record-boundary EOF without PIPE_ABANDON still fails the path.
                primary.inputEnded(true, false);
            } else {
                tick(host, driver, 250 * Time.MILLISECOND);
                assertEquals(PipeState.Outbound.SUSPECT, core.pipeStats(0, host.now, host.now).outbound());
            }
            round(host, driver);
            assertTrue(backup.writing(), "the original stream bytes must be reinjected on the backup");
            int originalBytes = 14; // 11 payload bytes and the trace's three-byte stream preamble.
            assertTrue(streamFrame(backup.wire, fixture.stream().id(), 0, originalBytes, false));

            backup.wire.clear();
            assertEquals(1, core.sending().write(fixture.stream(), ByteBuffer.wrap(new byte[] {42}), true));
            backup.writeProgress(1);
            backup.writableChanged(false);
            backup.writableChanged(true);
            backup.releaseWrite();
            acknowledgeBackup(host, core, backup);
            assertEquals(0, core.pipeStats(2, host.now, host.now).bytesInFlight().orElseThrow());
            round(host, driver);
            acknowledgeBackup(host, core, backup);
            tick(host, driver, 250 * Time.MILLISECOND);
            acknowledgeBackup(host, core, backup);
            tick(host, driver, 2 * Time.SECOND);
            assertEquals(SessionState.ACTIVE, core.state(), () -> String.valueOf(core.error()));
            assertTrue(streamFrame(backup.wire, fixture.stream().id(), originalBytes, 1, true),
                    "the backup must send more data and FIN after the partial write completes");
        } finally {
            driver.failInternalNow(new java.io.IOException("test owner stopped"));
            host.runShutdownHooks();
        }
    }

    private static void round(Host host, SessionRuntime driver) {
        driver.markDirty();
        host.endRound();
    }

    private static void tick(Host host, SessionRuntime driver, long elapsed) {
        host.now += elapsed;
        driver.core.handleTimeout(host.now);
        round(host, driver);
    }

    private static void acknowledgeBackup(Host host, Session core, Out backup) {
        long position = core.pipeStats(2, host.now, host.now).bytesSent();
        var frames = ByteBuffer.allocate(64);
        FrameEncoder.pipeAckHeader(frames, 1);
        FrameEncoder.ackEntry(frames, 2, 0, position, 0);
        var record = ByteBuffer.allocate(64);
        VarInt.put(record, frames.position());
        record.put(frames.flip());
        deliver(backup, record.flip());
    }

    private static boolean streamFrame(List<ByteBuffer> wire, long streamId, long offset, int bytes, boolean fin)
            throws Exception {
        int size = wire.stream().mapToInt(ByteBuffer::remaining).sum();
        var data = ByteBuffer.allocate(size);
        for (var part : wire) data.put(part.duplicate());
        data.flip();
        var decoder = new FrameDecoder();
        var frame = new Frame();
        while (data.hasRemaining()) {
            int start = data.position();
            long length = VarInt.get(data, start, data.limit());
            int at = start + VarInt.lengthOf(data.get(start));
            int end = at + Math.toIntExact(length);
            while (at < end) {
                at += decoder.decode(data, at, end, frame);
                if (frame.kind == FrameKind.STREAM && frame.streamId == streamId && frame.offset == offset
                        && frame.dataLen == bytes && frame.fin == fin) return true;
            }
            data.position(end);
        }
        return false;
    }

    @Test void byteStreamPublishesOnlyCompleteRecordsAndLendsEveryPayloadRange() throws Exception {
        Host host = new Host();
        var local = CoreConfig.builder().role(CoreConfig.Role.RESPONDER)
                .transportRole(CoreConfig.Role.RESPONDER).keepAlive(CoreConfig.OFF);
        var peer = CoreConfig.builder().role(CoreConfig.Role.INITIATOR)
                .transportRole(CoreConfig.Role.INITIATOR).keepAlive(CoreConfig.OFF);
        local.local().present &= ~TransportParams.PREAMBLE;
        peer.local().present &= ~TransportParams.PREAMBLE;
        var core = new Session(local.build(), 0);
        var driver = new SessionRuntime(host, core, DriverHooks.NONE);
        var carrier = new CarrierSetup().own(() -> new Out(host));
        try {
            driver.attachBootstrap(carrier);
            host.endRound();

            var remote = new Session(peer.build(), 0);
            remote.attachBootstrap(carrier.info().carrier(), carrier.info().carrier());
            var first = new Batch();
            assertTrue(remote.pollTransmit(0, 0, first, Session.DEFAULT_BATCH, 0));
            ByteBuffer hello = ByteBuffer.wrap(BatchFixture.flat(first));
            first.discard();
            int[] returned = {0};
            // Splitting inside both the record prefix and the transport parameters exercises
            // the same byte ingress used for bootstrap replay and subsequent stream records.
            while (hello.hasRemaining()) {
                giveOwned(carrier, hello.slice(hello.position(), 1), returned);
                hello.position(hello.position() + 1);
                if (hello.hasRemaining()) assertFalse(core.established());
            }
            host.endRound();
            assertTrue(core.established(), () -> String.valueOf(core.error()));
            assertEquals(hello.limit(), returned[0]);

            int bytes = 300;
            ByteBuffer wire = ByteBuffer.allocate(512);
            VarInt.put(wire, FrameEncoder.streamHeaderLength(0, 0, bytes, true) + bytes);
            FrameEncoder.streamHeader(wire, 0, 0, bytes, false, true);
            int start = wire.position();
            for (int i = 0; i < bytes; i++) wire.put((byte) i);
            wire.flip();

            giveOwned(carrier, wire.slice(0, 1), returned);
            giveOwned(carrier, wire.slice(1, start + 100 - 1), returned);
            assertNull(core.streams().acceptStream(true));
            giveOwned(carrier, wire.slice(start + 100, 200), returned);
            assertEquals(hello.limit() + 1, returned[0], () -> String.valueOf(core.error()));
            var stream = core.streams().acceptStream(true);
            assertNotNull(stream);
            var chunk = core.receiving().readChunk(stream, bytes);
            try {
                assertEquals(100, chunk.data().remaining());
                assertEquals((byte) 99, chunk.data().get(99));
                ByteBuffer dest = ByteBuffer.allocate(200);
                assertEquals(200, core.receiving().read(stream, dest).n());
                assertEquals((byte) 100, dest.get(0));
                assertEquals((byte) 299, dest.get(199));
                assertEquals(hello.limit() + 2, returned[0]);
                assertEquals(0, host.copiedInputBytes());
            } finally {
                chunk.buf().unref();
            }
            assertEquals(hello.limit() + 3, returned[0]);
        } finally {
            host.runShutdownHooks();
        }
    }

    private static void giveOwned(Out carrier, ByteBuffer input, int[] returned) {
        ReadBuf owner = new ReadBuf(input, ignored -> returned[0]++);
        try {
            carrier.input(input, owner);
            assertFalse(input.hasRemaining());
        } finally {
            owner.unref();
        }
    }

    @Test void unrelatedRoundsNeverVisitTheCarrierAndPingWritesOnce() throws Exception {
        Host host = new Host();
        var setup = new CarrierSetup();
        var out = setup.own(() -> new Out(host));
        var core = new Session(CoreConfig.builder().role(CoreConfig.Role.INITIATOR).keepAlive(CoreConfig.OFF).build(), 0);
        var driver = new SessionRuntime(host, core, DriverHooks.NONE);
        driver.attachBootstrap(out);
        host.endRound();

        var peer = new Session(CoreConfig.builder().role(CoreConfig.Role.RESPONDER).keepAlive(CoreConfig.OFF).build(), 0);
        peer.attachBootstrap(out.info().carrier(), out.info().carrier());
        var params = new Batch();
        assertTrue(peer.pollTransmit(0, 0, params, Session.DEFAULT_BATCH, 0));
        deliver(out, ByteBuffer.wrap(BatchFixture.flat(params)));
        params.discard();
        host.endRound();
        assertTrue(core.established());

        ByteBuffer response = ByteBuffer.allocate(10);
        VarInt.put(response, 9);
        FrameEncoder.ping(response, true, 0);
        deliver(out, response.flip());
        host.endRound();

        int checks = out.writableChecks;
        int samples = out.samples;
        int writes = out.writes;
        for (int i = 0; i < 64; i++) {
            driver.markDirty();
            host.endRound();
        }
        assertEquals(checks, out.writableChecks);
        assertEquals(samples, out.samples);
        assertEquals(writes, out.writes);

        out.hold = true;
        core.ping(null);
        host.endRound();
        assertEquals(samples + 1, out.samples);
        assertEquals(writes + 1, out.writes);

        core.pipes().wakeWriters(0);
        host.endRound();
        assertTrue(out.writing());
        int blockedChecks = out.writableChecks;
        out.releaseWrite();
        host.endRound();
        assertEquals(blockedChecks, out.writableChecks, "completion with no remaining work must not probe the carrier");
        assertEquals(samples + 1, out.samples);
        assertEquals(writes + 1, out.writes);
        host.runShutdownHooks();
    }

    @Test void anAcceptedBatchKeepsItsEncoderUntilItsLastProviderHold() throws Exception {
        Host host = new Host();
        var setup = new CarrierSetup();
        var out = setup.own(() -> new Out(host));
        var config = CoreConfig.builder().role(CoreConfig.Role.INITIATOR).keepAlive(CoreConfig.OFF);
        var core = new Session(config.build(), 0);
        var driver = new SessionRuntime(host, core, DriverHooks.NONE);
        try {
            driver.attachBootstrap(out);
            host.endRound();
            var peer = new Session(CoreConfig.builder().role(CoreConfig.Role.RESPONDER).keepAlive(CoreConfig.OFF).build(), 0);
            peer.attachBootstrap(out.info().carrier(), out.info().carrier());
            var hello = new Batch();
            assertTrue(peer.pollTransmit(0, 0, hello, Session.DEFAULT_BATCH, 0));
            deliver(out, ByteBuffer.wrap(BatchFixture.flat(hello)));
            hello.discard();
            host.endRound();

            ByteBuffer reply = ByteBuffer.allocate(10);
            VarInt.put(reply, 9);
            FrameEncoder.ping(reply, true, 0);
            deliver(out, reply.flip());
            host.endRound();

            out.retainNext = true;
            core.ping(null);
            host.endRound();
            assertNotNull(out.retained);
            int writes = out.writes, samples = out.samples;
            ByteBuffer old = ByteBuffer.allocate(out.retainedView.remaining()).put(out.retainedView.duplicate()).flip();

            ByteBuffer ping = ByteBuffer.allocate(10);
            VarInt.put(ping, 9);
            FrameEncoder.ping(ping, false, 0);
            deliver(out, ping.flip());
            host.endRound();
            assertEquals(writes, out.writes);
            assertEquals(samples, out.samples);
            assertEquals(old, out.retainedView);

            out.retained.close();
            host.endRound();
            assertEquals(writes + 1, out.writes);
            assertEquals(samples + 1, out.samples);
        } finally {
            if (out.retained != null) out.retained.close();
            host.runShutdownHooks();
        }
    }

    @Test void replacementDiscardsAnAlreadyQueuedPermission() {
        Host host = new Host();
        var core = new Session(CoreConfig.builder().keepAlive(CoreConfig.OFF).build(), 0);
        var driver = new SessionRuntime(host, core, DriverHooks.NONE);
        var old = new CarrierSetup().own(() -> new Out(host));
        var replacement = new CarrierSetup().own(() -> new Out(host));
        try {
            driver.attachBootstrap(old);
            driver.carriers().attach(replacement, 0, 0, null);
            host.endRound();
            assertEquals(0, old.writes);
            assertEquals(0, old.samples);
            assertEquals(1, replacement.writes);
            assertEquals(1, replacement.samples);
        } finally {
            host.runShutdownHooks();
        }
    }

    @Test void peerCloseReleasesAWaitingConnectionCloseWithoutAnotherTimer() {
        Host host = new Host();
        var core = multipath(CoreConfig.Role.INITIATOR);
        var peer = multipath(CoreConfig.Role.RESPONDER);
        var driver = new SessionRuntime(host, core, DriverHooks.NONE);
        var out = new CarrierSetup().own(() -> new Out(host));
        try {
            driver.attachBootstrap(out);
            host.endRound();
            peer.attachBootstrap(out.info().carrier(), out.info().carrier());
            Batch params = new Batch();
            assertTrue(peer.pollTransmit(0, 0, params, Session.DEFAULT_BATCH, 0));
            deliver(out, ByteBuffer.wrap(BatchFixture.flat(params)));
            params.discard();
            host.endRound();
            assertTrue(core.features().multipath());

            core.close(0x10, new byte[0]);
            host.endRound();
            assertFalse(out.stoppedOutput);
            int before = out.writes;
            ByteBuffer response = ByteBuffer.allocate(32);
            VarInt.put(response, FrameEncoder.closeLength(true, 0x10, 0, 0));
            FrameEncoder.close(response, true, 0x10, 0, new byte[0]);
            deliver(out, response.flip());
            host.endRound();
            assertEquals(before + 1, out.writes);
            assertTrue(out.stoppedOutput);
        } finally {
            host.runShutdownHooks();
        }
    }

    private static Session multipath(CoreConfig.Role role) {
        var config = CoreConfig.builder().role(role).transportRole(role).sessionPolicy(new io.zmux.internal.core.CoreSessionPolicy(new io.zmux.CapacityLimits(2))).keepAlive(CoreConfig.OFF)
                .psk(new byte[32]).declaredEndToEnd(true);
        var params = config.local();
        params.present |= TransportParams.MULTIPATH | TransportParams.SESSION_CRYPTO;
        params.secretFlags = TransportParams.SECRET_PSK;
        params.sessionCrypto = io.zmux.internal.join.SessionCrypto.supported();
        params.sessionNonce[0] = role == CoreConfig.Role.INITIATOR ? (byte) 1 : (byte) 2;
        return new Session(config.build(), 0);
    }

    private static void deliver(Out out, ByteBuffer data) {
        ReadBuf input = new ReadBuf(data, null);
        try {
            assertEquals(data.limit(), out.inbound().bytes(out, input, data, 0, data.limit()));
        } finally {
            input.unref();
        }
    }

    @Test void onlyTheCurrentHeapTopProgramsTheHostWake() {
        Host host = new Host();
        int[] calls = {0};
        TimerHeap.Timer first = new TimerHeap.Timer() {
            @Override public void fire(long now) { calls[0]++; }
        };
        TimerHeap.Timer second = new TimerHeap.Timer() {
            @Override public void fire(long now) { calls[0]++; }
        };
        host.schedule(first, 10);
        host.schedule(second, 20);
        host.schedule(first, 10);
        assertEquals(1, host.wakeChanges);
        host.schedule(first, 30);
        assertEquals(20, host.wake);
        host.runTimers(19);
        assertEquals(0, calls[0]);
        host.runTimers(20);
        assertEquals(1, calls[0]);
        assertEquals(30, host.wake);
        host.schedule(first, Time.NEVER);
        assertEquals(Time.NEVER, host.wake);
        assertEquals(4, host.wakeChanges);
        host.runTimers(40);
        assertEquals(1, calls[0]);
    }
}
