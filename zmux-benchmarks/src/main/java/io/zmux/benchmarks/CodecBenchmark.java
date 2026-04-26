package io.zmux.benchmarks;

import io.zmux.DecodedVarint;
import io.zmux.Frame;
import io.zmux.FrameType;
import io.zmux.Limits;
import io.zmux.MetadataUpdate;
import io.zmux.Preface;
import io.zmux.Protocol;
import io.zmux.Role;
import io.zmux.Settings;
import io.zmux.ZmuxCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class CodecBenchmark {
    private static Limits baselineNormalize(Limits limits) {
        Settings defaults = Settings.defaults();
        return new Limits(
                limits.maxFramePayload() == 0L ? defaults.maxFramePayload() : limits.maxFramePayload(),
                limits.maxControlPayloadBytes() == 0L ? defaults.maxControlPayloadBytes() : limits.maxControlPayloadBytes(),
                limits.maxExtensionPayloadBytes() == 0L ? defaults.maxExtensionPayloadBytes() : limits.maxExtensionPayloadBytes()
        );
    }

    @Benchmark
    public byte[] encodeVarint(CodecState state) throws Exception {
        return ZmuxCodec.encodeVarint(state.varintValue);
    }

    @Benchmark
    public DecodedVarint parseVarint(CodecState state) throws Exception {
        return ZmuxCodec.parseVarint(state.varintBytes);
    }

    @Benchmark
    public byte[] appendTlv(CodecState state) throws Exception {
        return ZmuxCodec.appendTlv(state.prefix, Protocol.METADATA_OPEN_INFO, state.tlvValue);
    }

    @Benchmark
    public MetadataUpdate encodePriorityUpdate() throws Exception {
        return new MetadataUpdate(23L, 29L);
    }

    @Benchmark
    public Frame parseFrame(CodecState state) throws Exception {
        return ZmuxCodec.parseFrame(state.encodedFrame, state.limits).frame();
    }

    @Benchmark
    public Frame parseFrameZeroSentinelLimits(CodecState state) throws Exception {
        return ZmuxCodec.parseFrame(state.encodedFrame, state.zeroSentinelLimits).frame();
    }

    @Benchmark
    public Frame parseFrameZeroSentinelLimitsBaseline(CodecState state) throws Exception {
        return ZmuxCodec.parseFrame(state.encodedFrame, baselineNormalize(state.zeroSentinelLimits)).frame();
    }

    @Benchmark
    public Limits normalizeZeroSentinelLimits(CodecState state) {
        return state.zeroSentinelLimits.normalize();
    }

    @Benchmark
    public Limits normalizeZeroSentinelLimitsBaseline(CodecState state) {
        return baselineNormalize(state.zeroSentinelLimits);
    }

    @Benchmark
    public Limits settingsLimits(CodecState state) {
        return state.settings.limits();
    }

    @Benchmark
    public Limits settingsLimitsBaseline(CodecState state) {
        return new Limits(
                state.settings.maxFramePayload(),
                state.settings.maxControlPayloadBytes(),
                state.settings.maxExtensionPayloadBytes()
        );
    }

    @Benchmark
    public void writeFrame(CodecState state, Blackhole blackhole) throws Exception {
        state.output.reset();
        ZmuxCodec.writeFrame(state.output, state.dataFrame, state.limits);
        blackhole.consume(state.output.size());
    }

    @Benchmark
    public void writeFrameZeroSentinelLimits(CodecState state, Blackhole blackhole) throws Exception {
        state.output.reset();
        ZmuxCodec.writeFrame(state.output, state.dataFrame, state.zeroSentinelLimits);
        blackhole.consume(state.output.size());
    }

    @Benchmark
    public void writeFrameZeroSentinelLimitsBaseline(CodecState state, Blackhole blackhole) throws Exception {
        state.output.reset();
        ZmuxCodec.writeFrame(state.output, state.dataFrame, baselineNormalize(state.zeroSentinelLimits));
        blackhole.consume(state.output.size());
    }

    @Benchmark
    public void writePreface(CodecState state, Blackhole blackhole) throws Exception {
        state.output.reset();
        ZmuxCodec.writePreface(state.output, state.preface);
        blackhole.consume(state.output.size());
    }

    @State(Scope.Thread)
    public static class CodecState {
        private static final long CAPABILITIES = Protocol.CAPABILITY_OPEN_METADATA
                | Protocol.CAPABILITY_PRIORITY_UPDATE
                | Protocol.CAPABILITY_PRIORITY_HINTS
                | Protocol.CAPABILITY_STREAM_GROUPS;

        long varintValue;
        byte[] varintBytes;
        byte[] prefix;
        byte[] tlvValue;
        Frame dataFrame;
        byte[] encodedFrame;
        Settings settings;
        Limits limits;
        Limits zeroSentinelLimits;
        Preface preface;
        ByteArrayOutputStream output;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            varintValue = Protocol.MAX_VARINT62 - 123_456L;
            varintBytes = ZmuxCodec.encodeVarint(varintValue);
            prefix = "prefix".getBytes(StandardCharsets.UTF_8);
            tlvValue = "benchmark-metadata".getBytes(StandardCharsets.UTF_8);
            dataFrame = new Frame(FrameType.DATA, 0, 4L, "benchmark-payload".getBytes(StandardCharsets.UTF_8));
            output = new ByteArrayOutputStream(256);
            settings = Settings.defaults();
            limits = settings.limits();
            zeroSentinelLimits = new Limits(0L, 0L, 0L);
            ZmuxCodec.writeFrame(output, dataFrame, limits);
            encodedFrame = output.toByteArray();
            preface = new Preface(
                    Protocol.PREFACE_VERSION,
                    Role.INITIATOR,
                    0L,
                    Protocol.PROTO_VERSION,
                    Protocol.PROTO_VERSION,
                    CAPABILITIES,
                    settings
            );
        }
    }
}
