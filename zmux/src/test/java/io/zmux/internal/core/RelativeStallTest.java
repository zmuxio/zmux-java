package io.zmux.internal.core;

import static io.zmux.internal.core.TraceFrames.getU;
import static io.zmux.internal.core.TraceFrames.list;
import static io.zmux.internal.core.TraceFrames.map;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.zmux.DatagramCopyPolicy;
import io.zmux.DirectionHealth;
import io.zmux.MemberId;
import io.zmux.internal.wire.FrameEncoder;
import io.zmux.internal.wire.TransportParams;
import io.zmux.testutil.Json;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** Virtual owner cuts, real cumulative ACKs and real scheduler handoff of the retained STREAM range. */
class RelativeStallTest {
    @TestFactory List<DynamicTest> contractVectors() throws Exception {
        String vectorFile = System.getProperty("zmux.pipe.health.vectors");
        String json;
        if (vectorFile != null) {
            json = Files.readString(Path.of(vectorFile));
        } else {
            try (var input = getClass().getResourceAsStream("/host-contracts/pipe-health.json")) {
                assumeTrue(input != null, "development fixture pipe-health.json is unavailable");
                json = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        var suite = Json.object(json);
        assertEquals("zmux-host-contract-1", suite.get("schema"));
        List<DynamicTest> tests = new ArrayList<>();
        for (Object item : list(suite.get("cases"))) {
            var vector = map(item);
            var setup = map(vector.get("setup"));
            if (!"relative-stall-replay".equals(setup.get("profile"))) continue;
            tests.add(DynamicTest.dynamicTest((String) vector.get("id"), () -> {
                var replay = new Replay(setup);
                for (Object step : list(vector.get("steps"))) replay.step(map(step));
            }));
        }
        assertFalse(tests.isEmpty());
        return tests;
    }

    @Test void aCreditBlockedPrefixUsesTheSameDeadlineAndKeepsOneOutstandingCopy() throws Exception {
        Replay r = new Replay(true, 30 * Time.SECOND, false);
        r.add("p", 0, 6_000_000, 118_750, 3_100_000);
        r.add("q", 1, 80_000, 3750, 55_000);
        r.prepare();
        r.open("s", "p", 16, 0);
        var stream = r.streams.get("s");
        Poke.set(stream.snd, "maxData", stream.snd.sent());
        r.session.sending().write(stream, ByteBuffer.allocate(64), false);
        assertTrue(Retention.streamBlocked(stream));
        r.record("q", "q1");
        r.at(80_000);
        r.ack("q", "q1");
        r.barrier();
        r.at(120_000);
        r.barrier();
        Pipe p = r.pipes.get("p"), q = r.pipes.get("q");
        assertEquals(16, r.session.stats().bytesOpportunistic());
        assertEquals(16, q.mp().ret.oppBytes());
        assertEquals(1, r.session.statsSnapshot().penalties());
        assertEquals(Attach.OUT_ACTIVE, p.mp().out());
        var health = p.mp().projection.snapshot().send();
        assertTrue(health.settled());
        assertFalse(health.canCarry());
        assertEquals(DirectionHealth.State.USABLE, health.state());
        assertEquals(0, stream.snd.mp().ret.history.resendBytes());
        r.path.health.makeSuspect(p);
        r.barrier();
        assertEquals(1, r.events.stream().filter(e -> "PrefixDispatched".equals(e.get("name"))).count());
        assertEquals(0, stream.snd.mp().ret.history.resendBytes());
        r.session.pipeEnded(q.id, 0, true, false, null, r.session.now());
        assertEquals(16, stream.snd.mp().ret.history.resendBytes());
    }

    @Test void aPenalizedBootstrapGivesNewDataToTheFastPipeWithoutRecopyingItsPrefix() throws Exception {
        Replay r = new Replay(true, 30 * Time.SECOND, false);
        r.add("bootstrap", 0, 6_000_000, 118_750, 3_100_000);
        r.add("fast", 1, 80_000, 3750, 55_000);
        r.prepare();
        r.open("s", "bootstrap", 16, 0);
        var stream = r.streams.get("s");
        long credit = stream.snd.maxData();
        Poke.set(stream.snd, "maxData", stream.snd.sent());
        assertEquals(64, r.session.sending().write(stream, ByteBuffer.allocate(64), false));
        r.record("fast", "proof");
        r.at(80_000);
        r.ack("fast", "proof");
        r.barrier();
        r.at(119_999);
        r.barrier();
        assertEquals(0, r.session.stats().bytesOpportunistic());
        r.at(120_000);
        r.barrier();
        Pipe bootstrap = r.pipes.get("bootstrap"), fast = r.pipes.get("fast");
        assertTrue(bootstrap.mp().ret.penalized());
        assertEquals(16, fast.mp().ret.oppBytes());
        r.at(175_000);
        r.ackPosition(fast, r.copies.get("s"));
        Poke.set(stream.snd, "maxData", credit);
        r.barrier();
        assertEquals(80, stream.snd.sent());
        for (Retained entry : bootstrap.out().lanes.getFirst().log)
            if (entry.d.kind() == FrameDesc.Kind.STREAM) assertEquals(0, entry.d.off());
        assertTrue(r.events.stream().anyMatch(event -> "PrefixDispatched".equals(event.get("name"))
                && "fast".equals(event.get("pipe")) && Long.valueOf(64).equals(event.get("length"))));
        assertEquals(16, r.session.stats().bytesOpportunistic());
    }

    @Test void deepBufferRateUsesTheBytePositionAndTheFullAckInterval() throws Exception {
        for (long lag : List.of(499 * Time.MILLISECOND, 500 * Time.MILLISECOND)) {
            Replay r = new Replay(true, 30 * Time.SECOND, false);
            r.add("bootstrap", 0, 100_000, 10_000, 60_000);
            r.prepare();
            Pipe pipe = r.pipes.get("bootstrap");
            var send = pipe.out().snd;
            assertEquals(100 * Time.MILLISECOND, send.rtt().min());
            assertFalse(send.re().measured());
            long before = send.delivered();
            long sent = r.session.now();
            r.open("s", "bootstrap", 3000, 0);
            r.session.setNow(sent + lag);
            r.ackPosition(pipe, pipe.out().lanes.getFirst().tx.pos());
            assertTrue(send.delivered() - before >= 3000, "the cumulative advance includes the complete stream record");
            if (lag == 500 * Time.MILLISECOND) {
                assertTrue(send.re().measured(), "adjusted RTT of five times min RTT proves backlog without a blocked writer");
                assertEquals((send.delivered() - before) * 1e9 / lag, send.re().raw());
            } else {
                assertFalse(send.re().measured(), "one step before the proof threshold is not a bottleneck sample");
            }
        }
    }

    @Test void aWriterStallUsesTheBaseDeadlineEvenWithAFastDeliveringPipe() throws Exception {
        Replay r = new Replay(true, 30 * Time.SECOND, false);
        r.add("p", 0, 1_000_000, 118_750, 600_000);
        r.add("q", 1, 80_000, 3750, 55_000);
        r.prepare();
        Pipe p = r.pipes.get("p");
        r.path.health.requestPing(p, r.session.now());
        var stalledWrite = new Batch();
        assertTrue(r.session.pollTransmit(p.id, 0, stalledWrite, Session.DEFAULT_BATCH, r.session.now()));
        r.ackPosition(p, p.out().lanes.getFirst().tx.pos());
        r.record("q", "q1");
        r.at(80_000);
        r.ack("q", "q1");
        r.session.handleTimeout(r.session.now());
        r.at(360_000);
        r.session.handleTimeout(r.session.now());
        assertEquals(Attach.OUT_ACTIVE, p.mp().out());
        r.at(3_000_000);
        r.session.handleTimeout(r.session.now());
        assertEquals(Attach.OUT_SUSPECT, p.mp().out());
        assertEquals(DirectionHealth.Reason.WRITE_STALL, p.mp().projection.snapshot().send().reason());
        r.session.writeDone(stalledWrite, null, r.session.now());
    }

    @Test void theCheapestStallReferenceAndFastestPrefixDestinationCanDiffer() throws Exception {
        Replay r = new Replay(true, 30 * Time.SECOND, false);
        r.add("p", 0, 6_000_000, 118_750, 3_100_000);
        r.add("q", 1, 80_000, 3750, 55_000);
        r.add("r", 2, 50_000, 5000, 40_000);
        r.prepare();
        r.open("s", "p", 16, 0);
        r.record("q", "q1");
        r.record("r", "r1");
        r.at(80_000);
        r.ack("q", "q1");
        r.ack("r", "r1");
        Pipe lowerEta = r.pipes.get("r");
        r.session.unsentReported(lowerEta.id, 0, (long) (r.path.metrics.rateOf(lowerEta) / 10));
        r.record("r", "r2");
        r.session.unsentReported(lowerEta.id, 0, -1);
        r.barrier();
        r.at(90_000);
        r.barrier();
        assertEquals(16, lowerEta.mp().ret.oppBytes());
        assertEquals(0, r.pipes.get("q").mp().ret.oppBytes());

        r.at(200_000);
        r.record("q", "q2");
        r.barrier();
        r.at(280_000);
        r.ack("q", "q2");
        r.ackPosition(lowerEta, r.copies.get("s"));
        r.session.unsentReported(lowerEta.id, 0, (long) (r.path.metrics.rateOf(lowerEta) / 10));
        r.record("r", "r3");
        r.session.unsentReported(lowerEta.id, 0, -1);
        r.barrier();
        r.at(285_000);
        r.barrier();
        assertEquals(Attach.OUT_ACTIVE, r.pipes.get("p").mp().out());
        r.at(360_000);
        r.barrier();
        assertEquals(Attach.OUT_SUSPECT, r.pipes.get("p").mp().out());
    }

    @Test void duePrefixWaitsUntilTheCopyBudgetRefillsWithoutPolling() throws Exception {
        Replay r = new Replay(true, 60 * Time.SECOND, false);
        r.add("p", 0, 20_000_000, 118_750, 10_100_000);
        r.add("q", 1, 4_800_000, 43_750, 1_000_000);
        r.prepare();
        r.open("s", "p", 16, 0);
        var budget = r.session.tx.copyBudget();
        budget.copy(r.session.now(), budget.remaining(r.session.now()));
        r.record("q", "q1");
        r.at(6_000_000);
        r.ack("q", "q1");
        r.barrier();
        assertEquals(0, r.session.stats().bytesOpportunistic());
        r.at(10_999_999);
        r.barrier();
        assertEquals(0, r.session.stats().bytesOpportunistic());
        r.at(11_000_000);
        r.barrier();
        assertEquals(16, r.session.stats().bytesOpportunistic());
    }

    @Test void anOutboundOnlyDirectionWithoutALiveReturnDoesNotRunTheAckClock() throws Exception {
        Replay r = new Replay(false, 30 * Time.SECOND, false);
        r.add("p", 0, 50_000, 5_000, 40_000);
        r.prepare();
        r.open("s", "p", 16, 0);
        Pipe p = r.pipes.get("p");
        p.mp().topologyExcluded(Action.DIR_IN);
        r.path.emitPipeState(p);
        r.barrier();
        r.at(3_000_000);
        r.barrier();
        assertTrue(p.mp().link.unacked());
        assertEquals(Attach.OUT_ACTIVE, p.mp().out());
    }

    @Test void aLaneAckAndItsDuplicateCannotRefreshThePipeWideProof() throws Exception {
        Replay r = new Replay(true, 30 * Time.SECOND, false);
        r.add("p", 0, 6_000_000, 118_750, 3_100_000);
        r.add("q", 1, 80_000, 3750, 55_000);
        r.prepare();
        Pipe q = r.pipes.get("q");
        Pipe.OutLane lane1 = new Pipe.OutLane(1, Replay.CARRIER, true);
        q.out().lanes.add(lane1);
        r.record("q", "first");
        r.record(q, lane1, "later");
        r.at(80_000);
        long later = r.records.get("later");
        r.path.ackEntry(q, q.id, 1, later, 0);
        assertFalse(r.path.health.deliveringAfter(q, Replay.ORIGIN, r.session.now(), true));
        r.ack("q", "first");
        assertTrue(r.path.health.deliveringAfter(q, Replay.ORIGIN, r.session.now(), true));
        r.at(320_000);
        r.path.ackEntry(q, q.id, 1, later, 0);
        assertFalse(r.path.health.deliveringAfter(q, Replay.ORIGIN, r.session.now(), true));
    }

    @Test void oldProgressCanRecoverButCannotProveTheNewGeneration() throws Exception {
        Replay r = new Replay(true, 30 * Time.SECOND, false);
        r.add("p", 0, 50_000, 5000, 40_000);
        r.prepare();
        r.record("p", "old");
        Pipe p = r.pipes.get("p");
        long prior = p.mp().projection.snapshot().send().evidenceGeneration();
        r.path.health.makeSuspect(p);
        r.at(80_000);
        r.ack("p", "old");
        var send = p.mp().projection.snapshot().send();
        assertEquals(Attach.OUT_ACTIVE, p.mp().out());
        assertTrue(send.evidenceGeneration() > prior);
        assertEquals(DirectionHealth.ProofKind.NONE, send.proofKind());
        assertFalse(r.path.health.deliveringAfter(p, Replay.ORIGIN, r.session.now(), true));
    }

    @Test void acknowledgingOneElicitingRecordDoesNotWaitForTheRestOfTheBatch() throws Exception {
        var r = new Replay(true, 30 * Time.SECOND, false);
        r.add("p", 0, 50_000, 5000, 40_000);
        r.prepare();
        Pipe p = r.pipes.get("p");
        var lane = p.out().lanes.getFirst();
        long start = lane.tx.pos();
        long first, padding;
        Batch batch = new Batch();
        try {
            RecordWriter writer = new RecordWriter();
            writer.init(batch, Framing.BYTES, 2048, 2048);
            writer.retainFor(p, lane.log.room());
            int pingLength = io.zmux.internal.wire.VarInt.length(io.zmux.internal.wire.FrameType.PIPE_PING);
            assertTrue(writer.frame(pingLength, true, FrameEncoder::pipePing));
            writer.end();
            first = start + Batch.positionBytes(batch.recordSize(0));
            assertTrue(writer.frame(1, false, b -> b.put((byte) 0)));
            writer.end();
            padding = first + Batch.positionBytes(batch.recordSize(1));
            assertTrue(writer.frame(pingLength, true, FrameEncoder::pipePing));
            writer.end();
            r.session.tx.afterBatch(writer, p, lane);
        } finally {
            batch.release();
        }
        assertTrue(lane.tx.pos() > padding);
        r.at(50_000);
        long originalRttAt = p.out().snd.rttAt();
        r.ackPosition(p, first);
        assertTrue(r.path.health.deliveringAfter(p, Replay.ORIGIN, r.session.now(), true));
        assertEquals(originalRttAt, p.out().snd.rttAt(), "one batch still contributes one RTT sample");
        long deliveredAt = r.session.now();
        r.at(70_000);
        r.ackPosition(p, padding);
        assertFalse(r.path.health.deliveringAfter(p, deliveredAt, r.session.now(), true));
    }

    @Test void acknowledgingARepairAdvancesToTheNextBlockingOriginal() throws Exception {
        var r = new Replay(true, 30 * Time.SECOND, false);
        r.add("p", 0, 6_000_000, 118_750, 3_100_000);
        r.add("q", 1, 80_000, 3750, 55_000);
        r.prepare();
        r.open("s", "p", 16, 0);
        Pipe p = r.pipes.get("p"), q = r.pipes.get("q");
        r.record("q", "q1");
        r.at(80_000);
        r.ack("q", "q1");
        r.barrier();
        r.at(120_000);
        r.barrier();
        assertEquals(16, q.mp().ret.oppBytes());

        r.at(200_000);
        r.ackPosition(q, r.copies.get("s"));
        r.session.setWritable(q.id, 0, false, r.session.now());
        Stream s = r.streams.get("s");
        assertEquals(16, r.session.sending().write(s, ByteBuffer.allocate(16), false));
        r.write(p, null);
        r.session.setWritable(q.id, 0, true, r.session.now());
        assertEquals(32, s.snd.sent());
        assertEquals(16, s.snd.mp().ret.history.prefix());
        assertTrue(p.mp().link.unacked());
        assertFalse(Retention.streamBlocked(s));
        r.record("q", "q2");
        r.at(280_000);
        r.ack("q", "q2");
        r.barrier();
        r.at(319_999);
        r.barrier();
        assertEquals(16, q.mp().ret.oppBytes());
        r.at(320_000);
        r.barrier();
        assertEquals(32, q.mp().ret.oppBytes());
        assertEquals(0, r.session.statsSnapshot().penalties());
    }

    @Test void aPrefixUsesOnlyTheUnreservedPartOfTheSessionCopyBound() throws Exception {
        var r = new Replay(true, 30 * Time.SECOND, false);
        r.add("p", 0, 6_000_000, 118_750, 3_100_000);
        r.add("q", 1, 80_000, 3750, 55_000);
        r.prepare();
        r.open("s", "p", 16, 0);
        long bound = r.path.retention.copyBound();
        Stream disjoint = r.session.streams().openStream(true);
        assertTrue(r.path.retention.queueCopy(disjoint, 0, bound - 8, false, new OwnerQueue<>()));
        r.record("q", "q1");
        r.at(80_000);
        r.ack("q", "q1");
        r.barrier();
        r.at(120_000);
        r.barrier();
        assertEquals(8, r.session.stats().bytesOpportunistic());
        assertEquals(bound, r.path.retention.copiesOutstanding());
        r.at(200_000);
        r.ackPosition(r.pipes.get("q"), r.copies.get("s"));
        r.barrier();
        assertEquals(16, r.session.stats().bytesOpportunistic());
    }

    @Test void aReferenceWithADueWriteStallCannotWithdrawAnotherPipeAtTheSameCut() throws Exception {
        var r = new Replay(true, 30 * Time.SECOND, false);
        r.add("p", 0, 6_000_000, 118_750, 3_100_000);
        r.add("q", 1, 80_000, 3750, 55_000);
        r.prepare();
        r.open("s", "p", 16, 0);
        r.record("q", "q1");
        r.at(110_000);
        Pipe q = r.pipes.get("q");
        var tx = q.out().lanes.getFirst().tx;
        Poke.set(tx, "writing", true);
        Poke.set(tx, "stallAt", r.session.now());
        r.at(200_000);
        r.ack("q", "q1");
        r.barrier();
        assertTrue(r.path.health.deliveringAfter(q, Replay.ORIGIN, r.session.now(), true));
        r.at(360_000);
        r.barrier();
        assertEquals(Attach.OUT_SUSPECT, q.mp().out());
        assertEquals(DirectionHealth.Reason.WRITE_STALL, q.mp().projection.snapshot().send().reason());
        assertEquals(Attach.OUT_ACTIVE, r.pipes.get("p").mp().out());
    }

    private static final class Replay {
        private static final long ORIGIN = 2 * Time.SECOND;
        private static final long SETTLE = 100 * Time.MILLISECOND;
        private static final CarrierInfo CARRIER = new CarrierInfo(Framing.BYTES, false, true, false, false);

        final Session session;
        final PathSession path;
        final Map<String, Pipe> pipes = new LinkedHashMap<>();
        final Map<Long, PathMetrics.Measurement> measurements = new HashMap<>();
        final Map<String, Stream> streams = new LinkedHashMap<>();
        final Map<String, Long> records = new HashMap<>();
        final Map<MemberId, Long> faults = new HashMap<>();
        final Map<Pipe, Long> probes = new HashMap<>();
        final Map<String, Long> copies = new HashMap<>();
        final List<Map<String, Object>> events = new ArrayList<>();

        Replay(Map<String, Object> setup) throws Exception {
            this(!Boolean.FALSE.equals(setup.get("resume")),
                    setup.containsKey("idle_timeout_us") ? micros(setup, "idle_timeout_us") : 30 * Time.SECOND,
                    setup.containsKey("controller"));
            for (Object item : list(setup.get("pipes"))) {
                var input = map(item);
                String name = (String) input.get("id");
                add(name, getU(input, "pipe_id"), getU(input, "srtt_us"), getU(input, "rttvar_us"), getU(input, "eta_us"));
                Pipe p = pipes.get(name);
                var m = measurements.get(p.id);
                measurements.put(p.id, new PathMetrics.Measurement(m.srtt(), m.variation(), m.eta(),
                        input.containsKey("deviation_us") ? micros(input, "deviation_us") : m.variation()));
            }
            prepare();
        }

        Replay(boolean resume, long idle, boolean managed) throws Exception {
            session = CounterSemanticsTest.established(config -> {
                config.random(new Random(0)).keepAlive(CoreConfig.OFF).probeInterval(0).standbyProbe(0)
                        .localIdleTimeout(idle).settleFor(SETTLE).redundancy(false)
                        .policy(CoreConfig.Policy.SPREAD_ETA)
                        .test(new CoreConfig.TestHooks(true, null, 0, 0, Map.of(), 0, null))
                        .resume(resume ? 30 * Time.SECOND : 0)
                        .datagramCopyPolicy(new DatagramCopyPolicy(1, 100, false));
                var params = config.local();
                params.maxIdleTimeoutMillis = idle / Time.MILLISECOND;
                params.present |= TransportParams.PREAMBLE | TransportParams.ACK_DELAY;
                params.maxAckDelayMillis = 25;
                if (resume) {
                    params.present |= TransportParams.RESUME;
                    params.resumeGraceMillis = 30_000;
                    params.locator = new byte[] {1};
                } else {
                    params.present &= ~TransportParams.RESUME;
                }
            });
            path = session.mp();
            assertEquals(resume, session.features().resume());
            Poke.set(path.metrics, "measured", (java.util.function.LongFunction<PathMetrics.Measurement>) measurements::get);
            if (managed) session.attend(1);
            session.memberFeedback(change -> {
                if (change.failed()) faults.merge(change.id(), 1L, Long::sum);
            });
        }

        void add(String name, long id, long srtt, long variation, long eta) {
            Pipe p = session.pipes.get(id);
            if (p == null) {
                p = new Pipe(id, new Pipe.InDir(new Pipe.InLane(0, false, false)), new Pipe.OutDir(CARRIER),
                        new Attach(false, 2, id, 0, 0, -1, 0, Attach.OUT_ACTIVE, Attach.IN_OPEN, false, session.now()), null);
                session.pipes.add(p);
            }
            var member = session.pipes.member(name, Action.DIR_BIDI, 1);
            member.joined(io.zmux.PipeId.of(p.id));
            p.member(member);
            pipes.put(name, p);
            measures(p, srtt, variation, eta);
            path.emitPipeState(p);
        }

        void prepare() throws Exception {
            session.setNow(Time.SECOND);
            for (Pipe p : pipes.values()) {
                path.health.requestPing(p, session.now());
                write(p, null);
            }
            session.setNow(Time.SECOND + 100 * Time.MILLISECOND);
            for (Pipe p : pipes.values()) ackPosition(p, p.out().lanes.getFirst().tx.pos());
            session.handleTimeout(ORIGIN);
            for (Pipe p : pipes.values()) {
                assertTrue(p.mp().projection.snapshot().send().settled(), "setup needs settled " + p.id);
                assertFalse(p.mp().link.unacked(), "setup needs empty flight " + p.id);
                probes.put(p, p.mp().health.pingsSent());
            }
            while (session.outbox().pollEvent() != null) { }
        }

        void step(Map<String, Object> step) throws Exception {
            String target = (String) step.get("target");
            String action = (String) step.get("action");
            switch ((String) step.get("op")) {
                case "advance" -> at(getU(step, "at_us"));
                case "barrier" -> barrier();
                case "expect" -> expect(target, map(step.get("values")));
                case "call" -> {
                    assertEquals("open_stream_with_prefix", action);
                    var args = map(step.get("args"));
                    open((String) args.get("stream"), (String) args.get("source"),
                            getU(args, "prefix_bytes"), micros(args, "queue_delay_us"));
                }
                case "carrier" -> carrier(target, action, map(step.get("args")));
                default -> fail("unsupported step " + step);
            }
        }

        private void carrier(String target, String action, Map<String, Object> args) throws Exception {
            Pipe p = pipes.get(target);
            String record = (String) args.get("record");
            switch (action) {
                case "path_estimator_inputs" -> measures(p, getU(args, "srtt_us"), getU(args, "rttvar_us"), getU(args, "eta_us"));
                case "handoff_ack_eliciting" -> record(target, record);
                case "ack_record" -> ack(target, record);
                case "ack_record_and_recovery_probe" -> {
                    assertTrue(p.out().lanes.getFirst().tx.pos() > records.get(record));
                    ackPosition(p, p.out().lanes.getFirst().tx.pos());
                }
                case "ack_prefix_copy" -> ackPosition(p, copies.get((String) args.get("stream")));
                case "set_writer_ready" -> session.setWritable(p.id, 0, Boolean.TRUE.equals(args.get("ready")), session.now());
                case "unexpected_eof" -> session.pipeEnded(p.id, 0, true, false, null, session.now());
                default -> fail("unsupported carrier action " + action);
            }
        }

        void at(long atUs) {
            long goal = ORIGIN + atUs * Time.MICROSECOND;
            session.refreshDeadlines();
            for (long next; (next = session.nextTimeout()) < goal; ) {
                assertTrue(next > session.now(), "deadline did not settle at " + next);
                session.setNow(next);
                barrier();
                session.refreshDeadlines();
            }
            session.setNow(goal);
        }

        void barrier() {
            session.settle();
            session.handleTimeout(session.now());
            for (var item : pipes.entrySet()) {
                Pipe p = item.getValue();
                if (p.out().failed()) continue;
                write(p, item.getKey());
            }
            collect();
            session.refreshDeadlines();
        }

        void open(String label, String source, long prefix, long queue) throws Exception {
            Pipe p = pipes.get(source);
            for (Pipe q : pipes.values()) if (p != q) session.setWritable(q.id, 0, false, session.now());
            long ahead = Math.round(path.metrics.rateOf(p) * queue / Time.SECOND);
            Stream s = session.streams().openStream(true);
            int app = Math.toIntExact(prefix) - s.preSize();
            assertEquals(app, session.sending().write(s, ByteBuffer.allocate(app), false));
            Batch batch = new Batch();
            // The normal handoff snapshots the reported bytes ahead after selecting the writer.
            assertTrue(session.pollReady(p.id, 0, batch, Session.DEFAULT_BATCH, 0, session.now(), () -> ahead));
            session.writeDone(batch, null, session.now());
            assertEquals(prefix, s.snd.sent());
            Retained retained = null;
            for (Retained e : p.out().lanes.getFirst().log) {
                if (e.d.kind() == FrameDesc.Kind.STREAM && e.d.id() == s.tab.id()) retained = e;
            }
            assertNotNull(retained);
            assertEquals(prefix, retained.d.n());
            assertEquals(s.tab.id(), retained.d.id());
            assertTrue(Math.abs(queue - p.mp().link.oldestQueue()) <= 1000, "reported oldest queue");
            records.put(label, retained.end);
            streams.put(label, s);
            for (Pipe q : pipes.values()) if (p != q) session.setWritable(q.id, 0, true, session.now());
            session.unsentReported(p.id, 0, -1);
        }

        void record(String route, String label) {
            Pipe p = pipes.get(route);
            record(p, p.out().lanes.getFirst(), label);
        }

        private void record(Pipe p, Pipe.OutLane lane, String label) {
            Batch batch = new Batch();
            RecordWriter writer = new RecordWriter();
            try {
                writer.init(batch, Framing.BYTES, 2048, 2048);
                writer.retainFor(p, lane.log.room());
                assertTrue(writer.frame(io.zmux.internal.wire.VarInt.length(io.zmux.internal.wire.FrameType.PIPE_PING),
                        true, FrameEncoder::pipePing));
                writer.end();
                session.tx.afterBatch(writer, p, lane);
                records.put(label, lane.tx.pos());
            } finally {
                batch.release();
            }
        }

        void ack(String route, String label) throws Exception {
            ackPosition(pipes.get(route), records.get(label));
        }

        void ackPosition(Pipe p, long position) throws Exception {
            if (position != 0) path.ackEntry(p, p.id, 0, position, 0);
        }

        private void write(Pipe p, String target) {
            Batch b = new Batch();
            var lane = p.out().lanes.getFirst();
            long start = lane.tx.pos();
            if (!session.pollTransmit(p.id, 0, b, Session.DEFAULT_BATCH, session.now())) return;
            session.writeDone(b, null, session.now());
            if (target == null) return;
            collect();
            for (Retained entry : lane.log) {
                if (entry.end <= start || entry.d.kind() != FrameDesc.Kind.STREAM) continue;
                for (var stream : streams.entrySet()) {
                    if (entry.d.id() != stream.getValue().tab.id()) continue;
                    copies.put(stream.getKey(), entry.end);
                    boolean failed = events.stream().anyMatch(e -> "PipeFailed".equals(e.get("name")));
                    String mode = entry.d.prefixRepair() ? "opportunistic" : failed ? "failure_reinjection" : "suspect_reinjection";
                    events.add(Map.of("name", "PrefixDispatched", "stream", stream.getKey(), "pipe", target,
                            "at_us", timeUs(), "length", entry.d.n(), "mode", mode));
                }
            }
        }

        private void collect() {
            for (Event e; (e = session.outbox().pollEvent()) != null; ) {
                String kind = switch (e.kind()) {
                    case PIPE_SUSPECT -> "PipeSuspect";
                    case PIPE_FAILED -> "PipeFailed";
                    case PIPE_RECOVERED -> "PipeRecovered";
                    case DEADLINE_STALLED -> fail("stalled deadline " + e.reason());
                    default -> null;
                };
                if (kind == null) continue;
                Pipe p = session.pipes.get(e.pipe());
                long id = e.pipe();
                String label = pipes.entrySet().stream().filter(row -> row.getValue().id == id)
                        .map(Map.Entry::getKey).findFirst().orElseThrow();
                if (e.kind() == Event.Kind.PIPE_RECOVERED) {
                    events.add(Map.of("name", kind, "pipe", label, "at_us", timeUs()));
                } else {
                    var reason = p.mp().projection.snapshot().send().reason();
                    String value = switch (reason) {
                        case ACK_STALL -> "ack_stalled";
                        case WRITE_STALL -> "write_stalled";
                        case UNEXPECTED_EOF -> "unexpected_eof";
                        default -> reason.name();
                    };
                    events.add(Map.of("name", kind, "pipe", label, "at_us", timeUs(), "reason", value));
                }
            }
        }

        private void expect(String target, Map<String, Object> values) {
            if (values.containsKey("events")) {
                assertEquals(list(values.get("events")), events, "events at " + timeUs());
                return;
            }
            if ("controller".equals(target)) {
                // These cuts drive the owner feedback, with opener execution held outside the replay.
                if (values.containsKey("missing_required_send")) {
                    assertEquals(getU(values, "missing_required_send") != 0, path.joins.lacks(false, true));
                }
                return;
            }
            Pipe p = pipes.get(target);
            for (var entry : values.entrySet()) {
                Object actual = switch (entry.getKey()) {
                    case "settled" -> p.mp().projection.snapshot().send().settled();
                    case "eligible_for_new_data" -> p.mp().projection.snapshot().send().canCarry();
                    case "member_faults" -> faults.getOrDefault(p.member().id(), 0L);
                    case "suspect_probe_requests" -> p.mp().health.pingsSent() - probes.get(p);
                    case "delivery_evidence_fresh" -> path.health.deliveringAfter(p, ORIGIN, session.now(), path.health.liveReturn());
                    default -> fail("unsupported expectation " + entry);
                };
                assertEquals(entry.getValue(), actual, entry.getKey() + " at " + timeUs());
            }
        }

        private void measures(Pipe p, long srtt, long variation, long eta) {
            var previous = measurements.get(p.id);
            long deviation = previous == null ? variation * Time.MICROSECOND : previous.deviation();
            measurements.put(p.id, new PathMetrics.Measurement(srtt * Time.MICROSECOND,
                    variation * Time.MICROSECOND, eta * Time.MICROSECOND, deviation));
        }

        private long timeUs() { return (session.now() - ORIGIN) / Time.MICROSECOND; }
    }

    private static long micros(Map<String, Object> values, String key) {
        return getU(values, key) * Time.MICROSECOND;
    }
}
