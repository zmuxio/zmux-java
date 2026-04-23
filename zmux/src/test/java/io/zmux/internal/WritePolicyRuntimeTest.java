package io.zmux.internal;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.zmux.TestLists.listOf;
import static org.junit.jupiter.api.Assertions.*;

final class WritePolicyRuntimeTest {
    private static List<Integer> queuedDataSizes(SessionRuntime runtime) throws Exception {
        List<Integer> sizes = new ArrayList<>();
        for (Object outbound : SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue")) {
            sizes.add(SessionRuntimeTestSupport.outboundDataBytes(outbound));
        }
        return sizes;
    }

    private static int payloadPrefixLength(Object outbound) throws Exception {
        byte[] prefix = (byte[]) SessionRuntimeTestSupport.invokePrivate(outbound, "payloadPrefix", new Class<?>[0]);
        return prefix == null ? 0 : prefix.length;
    }

    private static void makePeerVisible(SessionRuntime runtime, StreamRuntime stream) throws Exception {
        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "beginLocalOpenLocked",
                new Class<?>[]{StreamRuntime.class},
                stream
        );
        runtime.markLocalStreamOpeningCommittedLocked(stream);
        SessionRuntimeTestSupport.invokePrivate(
                runtime,
                "markPeerVisibleLocked",
                new Class<?>[]{StreamRuntime.class},
                stream
        );
    }

    @Test
    void priorityBiasesBurstLimitAndFragmentCap() throws Exception {
        Settings peerSettings = Settings.builder()
                .maxFramePayload(16_384L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, peerSettings);
        StreamRuntime mild = (StreamRuntime) runtime.openStream(new OpenOptions(2L, null, new byte[0]));
        StreamRuntime strong = (StreamRuntime) runtime.openStream(new OpenOptions(6L, null, new byte[0]));
        StreamRuntime saturated = (StreamRuntime) runtime.openStream(new OpenOptions(20L, null, new byte[0]));

        synchronized (runtime.lock()) {
            assertEquals(WritePolicy.MILD_WRITE_BURST_FRAMES, mild.writeBurstLimitLocked(), "mild priority should reduce burst length");
            assertEquals(WritePolicy.STRONG_WRITE_BURST_FRAMES, strong.writeBurstLimitLocked(), "strong priority should reduce burst length further");
            assertEquals(WritePolicy.SATURATED_WRITE_BURST_FRAMES, saturated.writeBurstLimitLocked(), "saturated priority should use the smallest burst");

            assertEquals(12_288L, mild.txFragmentCapLocked(0L), "mild priority should keep three quarters of max_frame_payload");
            assertEquals(8_192L, strong.txFragmentCapLocked(0L), "strong priority should halve max_frame_payload");
            assertEquals(4_096L, saturated.txFragmentCapLocked(0L), "saturated priority should quarter max_frame_payload");
        }
    }

    @Test
    void latencyHintShrinksDefaultWriteChunks() throws Exception {
        Settings peerSettings = Settings.builder()
                .maxFramePayload(16_384L)
                .schedulerHints(SchedulerHint.LATENCY)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        stream.write(new byte[9_000]);

        synchronized (runtime.lock()) {
            assertEquals(WritePolicy.MILD_WRITE_BURST_FRAMES, stream.writeBurstLimitLocked(), "latency hint should bias default streams toward shorter bursts");
            assertEquals(8_192L, stream.txFragmentCapLocked(0L), "latency hint should halve the default fragment cap");
            assertEquals(listOf(8_192, 808), queuedDataSizes(runtime), "latency hint should reduce queued DATA fragment size");
        }
    }

    @Test
    void saturatedPriorityRespectsOpeningMetadataPrefixWhenChunking() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA | Protocol.CAPABILITY_PRIORITY_HINTS;
        Settings peerSettings = Settings.builder()
                .maxFramePayload(16_384L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream(new OpenOptions(20L, null, "ssh".getBytes(StandardCharsets.UTF_8)));

        stream.write(new byte[5_000]);

        synchronized (runtime.lock()) {
            List<Object> queued = new ArrayList<>(SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue"));
            assertEquals(2, queued.size(), "priority-biased opening write should fragment into two DATA frames");

            Object first = queued.get(0);
            int prefixLength = payloadPrefixLength(first);
            assertTrue(prefixLength > 0, "opening DATA should retain the metadata prefix");

            int firstDataBytes = SessionRuntimeTestSupport.outboundDataBytes(first);
            long expectedFirstDataBytes = WritePolicy.scaledFragmentCap(peerSettings.maxFramePayload() - prefixLength, 1L, 4L);
            assertEquals(expectedFirstDataBytes, firstDataBytes, "opening fragment cap should apply after subtracting the metadata prefix");
            assertEquals(prefixLength + firstDataBytes, SessionRuntimeTestSupport.outboundPayload(first).length, "opening frame payload should include prefix plus app bytes");

            Object second = queued.get(1);
            assertEquals(5_000 - firstDataBytes, SessionRuntimeTestSupport.outboundDataBytes(second), "remaining app bytes should stay queued behind the opening fragment");
        }
    }

    @Test
    void slowSendRateEstimateShrinksFragmentCapBeyondPriorityBias() throws Exception {
        Settings peerSettings = Settings.builder()
                .maxFramePayload(16_384L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.setLongField(runtime, "sendRateEstimateBytesPerSecond", 1_000L);
            assertEquals(200L, stream.txFragmentCapLocked(0L), "slow send-rate estimate should clamp fragment cap to the serialization-time budget");
        }
    }

    @Test
    void highSendRateEstimateDoesNotOverflowIntoArtificialFragmentClamp() {
        long baseCap = 1_000_000_000_000L;
        long highRate = 50_000_000_000_000L;

        long cap = WritePolicy.rateLimitedFragmentCap(
                baseCap,
                highRate,
                0L,
                SchedulerHint.UNSPECIFIED_OR_BALANCED
        );

        assertEquals(baseCap, cap, "high send-rate estimates should keep the static cap instead of overflowing into a tiny rate cap");
    }

    @Test
    void scaledFragmentCapUsesWideMultiplyDivideNearVarint62Limit() {
        long max = Protocol.MAX_VARINT62;

        long cap = WritePolicy.scaledFragmentCap(max, 3L, 4L);

        assertEquals(3_458_764_513_820_540_927L, cap);
    }

    @Test
    void sendRateEstimateUsesMeaningfulFlushSamples() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "noteSendRateEstimateLocked",
                    new Class<?>[]{long.class, long.class},
                    8_192L,
                    TimeUnit.SECONDS.toNanos(2L)
            );
            assertEquals(4_096L, SessionRuntimeTestSupport.getLongField(runtime, "sendRateEstimateBytesPerSecond"), "first meaningful flush sample should seed the send-rate estimate");

            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "noteSendRateEstimateLocked",
                    new Class<?>[]{long.class, long.class},
                    8_192L,
                    TimeUnit.SECONDS.toNanos(1L)
            );
            assertEquals(6_144L, SessionRuntimeTestSupport.getLongField(runtime, "sendRateEstimateBytesPerSecond"), "later samples should update the estimate with a simple EWMA");
        }
    }

    @Test
    void sendRateEstimateUsesExactWideMultiplyDivideWhenSampleWouldOverflowLongProduct() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "noteSendRateEstimateLocked",
                    new Class<?>[]{long.class, long.class},
                    50_000_000_000_000L,
                    TimeUnit.MILLISECONDS.toNanos(200L)
            );
            assertEquals(
                    250_000_000_000_000L,
                    SessionRuntimeTestSupport.getLongField(runtime, "sendRateEstimateBytesPerSecond"),
                    "large throughput samples should not be under-estimated by intermediate multiplication overflow"
            );
        }
    }

    @Test
    void sendRateEstimateSaturatesAndAveragesWithoutOverflow() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "noteSendRateEstimateLocked",
                    new Class<?>[]{long.class, long.class},
                    Long.MAX_VALUE,
                    1L
            );
            assertEquals(
                    Long.MAX_VALUE,
                    SessionRuntimeTestSupport.getLongField(runtime, "sendRateEstimateBytesPerSecond"),
                    "overflowing throughput samples should saturate at the Java signed counter maximum"
            );

            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "noteSendRateEstimateLocked",
                    new Class<?>[]{long.class, long.class},
                    Long.MAX_VALUE,
                    1L
            );
            assertEquals(
                    Long.MAX_VALUE,
                    SessionRuntimeTestSupport.getLongField(runtime, "sendRateEstimateBytesPerSecond"),
                    "EWMA averaging must not overflow when both operands are saturated"
            );
        }
    }

    @Test
    void sendRateEstimateIgnoresTinyFastSamples() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(
                    runtime,
                    "noteSendRateEstimateLocked",
                    new Class<?>[]{long.class, long.class},
                    128L,
                    TimeUnit.MILLISECONDS.toNanos(2L)
            );
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(runtime, "sendRateEstimateBytesPerSecond"), "tiny fast writes should not poison the slow-link estimate");
        }
    }

    @Test
    void finalWriteTimeoutBeforeQueueAdmissionDoesNotLatchFinQueued() throws Exception {
        Settings peerSettings = Settings.defaults().toBuilder()
                .initialMaxData(1L << 20)
                .initialMaxStreamDataBidiPeerOpened(0L)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, peerSettings);
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        synchronized (runtime.lock()) {
            makePeerVisible(runtime, stream);
            SessionRuntimeTestSupport.setLongField(stream, "peerSendLimit", 0L);
        }

        stream.setWriteDeadline(Instant.now().minusMillis(1L));

        assertThrows(WriteTimeoutException.class, () -> stream.writeFinal(new byte[]{1}));
        synchronized (runtime.lock()) {
            assertFalse(stream.finQueuedLocked(), "timed-out final write must not commit FIN before queue admission");
            assertEquals(0L, SessionRuntimeTestSupport.getLongField(stream, "reservedSendBytes"));
            assertEquals(0, SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue").size());
        }
    }
}
