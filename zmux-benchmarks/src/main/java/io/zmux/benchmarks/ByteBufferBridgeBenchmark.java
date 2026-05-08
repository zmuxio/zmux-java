package io.zmux.benchmarks;

import io.zmux.MetadataUpdate;
import io.zmux.StreamMetadata;
import io.zmux.ZmuxSendStream;
import io.zmux.transport.ReadHalf;
import io.zmux.transport.WriteHalf;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ByteBufferBridgeBenchmark {
    private static int baselineReadFullyIntoByteBuffer(ByteBuffer dst, SourceReadHalf reader) throws IOException {
        int total = 0;
        while (dst.hasRemaining()) {
            int read = baselineReadIntoByteBuffer(dst, reader);
            if (read <= 0) {
                return total == 0 ? read : total;
            }
            total += read;
        }
        return total;
    }

    private static int readFullyIntoByteBuffer(ByteBuffer dst, SourceReadHalf reader) throws IOException {
        int total = 0;
        while (dst.hasRemaining()) {
            int read = reader.read(dst);
            if (read <= 0) {
                return total == 0 ? read : total;
            }
            total += read;
        }
        return total;
    }

    private static int baselineReadIntoByteBuffer(ByteBuffer dst, ReadHalf reader) throws IOException {
        if (!dst.hasRemaining()) {
            return 0;
        }
        if (dst.hasArray()) {
            int position = dst.position();
            int read = reader.read(dst.array(), dst.arrayOffset() + position, dst.remaining());
            if (read > 0) {
                dst.position(position + read);
            }
            return read;
        }
        byte[] buffer = new byte[Math.min(dst.remaining(), 8192)];
        int read = reader.read(buffer, 0, buffer.length);
        if (read > 0) {
            dst.put(buffer, 0, read);
        }
        return read;
    }

    private static int baselineWriteFromByteBuffer(ByteBuffer src, WriteHalf writer) throws IOException {
        if (!src.hasRemaining()) {
            return 0;
        }
        if (src.hasArray()) {
            int position = src.position();
            int length = src.remaining();
            writer.write(src.array(), src.arrayOffset() + position, length);
            src.position(position + length);
            return length;
        }
        int initialPosition = src.position();
        int total = 0;
        ByteBuffer duplicate = src.duplicate();
        byte[] buffer = new byte[Math.min(duplicate.remaining(), 8192)];
        while (duplicate.hasRemaining()) {
            int length = Math.min(duplicate.remaining(), buffer.length);
            duplicate.get(buffer, 0, length);
            writer.write(buffer, 0, length);
            total += length;
            src.position(initialPosition + total);
        }
        return total;
    }

    private static int baselineWriteFinalFromByteBuffer(ByteBuffer src, ZmuxSendStream writer) throws IOException {
        if (!src.hasRemaining()) {
            return writer.writeFinal(new byte[0]);
        }
        if (src.hasArray()) {
            int position = src.position();
            int length = src.remaining();
            int written = writer.writeFinal(src.array(), src.arrayOffset() + position, length);
            if (written > 0) {
                src.position(position + written);
            }
            return written;
        }
        int initialPosition = src.position();
        int total = 0;
        ByteBuffer duplicate = src.duplicate();
        byte[] buffer = new byte[Math.min(duplicate.remaining(), 8192)];
        while (duplicate.remaining() > buffer.length) {
            duplicate.get(buffer, 0, buffer.length);
            writer.write(buffer, 0, buffer.length);
            total += buffer.length;
            src.position(initialPosition + total);
        }
        int finalLength = duplicate.remaining();
        duplicate.get(buffer, 0, finalLength);
        int written = writer.writeFinal(buffer, 0, finalLength);
        if (written > 0) {
            total += written;
            src.position(initialPosition + total);
        }
        return total;
    }

    @Benchmark
    public int writeDirectByteBufferBaseline(BridgeState state) throws Exception {
        state.resetWrite();
        return baselineWriteFromByteBuffer(state.directWriteSource, state.writeHalf) + state.writeHalf.checksum;
    }

    @Benchmark
    public int writeDirectByteBuffer(BridgeState state) throws Exception {
        state.resetWrite();
        return state.writeHalf.write(state.directWriteSource) + state.writeHalf.checksum;
    }

    @Benchmark
    public int writeFinalDirectByteBufferBaseline(BridgeState state) throws Exception {
        state.resetWriteFinal();
        return baselineWriteFinalFromByteBuffer(state.directWriteSource, state.sendStream) + state.sendStream.checksum;
    }

    @Benchmark
    public int writeFinalDirectByteBuffer(BridgeState state) throws Exception {
        state.resetWriteFinal();
        return state.sendStream.writeFinal(state.directWriteSource) + state.sendStream.checksum;
    }

    @Benchmark
    public int readDirectByteBufferBaseline(BridgeState state) throws Exception {
        state.resetRead();
        return baselineReadFullyIntoByteBuffer(state.directReadTarget, state.readHalf) + state.readHalf.checksum;
    }

    @Benchmark
    public int readDirectByteBuffer(BridgeState state) throws Exception {
        state.resetRead();
        return readFullyIntoByteBuffer(state.directReadTarget, state.readHalf) + state.readHalf.checksum;
    }

    @State(Scope.Thread)
    public static class BridgeState {
        @Param({"256", "65536"})
        int payloadBytes;

        byte[] sourceBytes;
        ByteBuffer directWriteSource;
        ByteBuffer directReadTarget;
        SinkWriteHalf writeHalf;
        SinkSendStream sendStream;
        SourceReadHalf readHalf;

        @Setup(Level.Trial)
        public void setup() {
            sourceBytes = new byte[payloadBytes];
            for (int i = 0; i < sourceBytes.length; i++) {
                sourceBytes[i] = (byte) (i * 31 + 7);
            }
            directWriteSource = ByteBuffer.allocateDirect(payloadBytes);
            directWriteSource.put(sourceBytes);
            directWriteSource.flip();
            directReadTarget = ByteBuffer.allocateDirect(payloadBytes);
            writeHalf = new SinkWriteHalf();
            sendStream = new SinkSendStream();
            readHalf = new SourceReadHalf(sourceBytes);
        }

        void resetWrite() {
            directWriteSource.position(0);
            directWriteSource.limit(sourceBytes.length);
            writeHalf.reset();
        }

        void resetWriteFinal() {
            directWriteSource.position(0);
            directWriteSource.limit(sourceBytes.length);
            sendStream.reset();
        }

        void resetRead() {
            directReadTarget.clear();
            readHalf.reset();
        }
    }

    private static final class SinkWriteHalf implements WriteHalf {
        private int checksum;

        void reset() {
            checksum = 0;
        }

        @Override
        public void write(byte[] src, int offset, int length) {
            if (length > 0) {
                checksum += (src[offset] & 0xff) + length;
            }
        }

        @Override
        public void closeWrite() {
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
        }
    }

    private static final class SinkSendStream implements ZmuxSendStream {
        private int checksum;

        void reset() {
            checksum = 0;
        }

        @Override
        public void write(byte[] src, int offset, int length) {
            if (length > 0) {
                checksum += (src[offset] & 0xff) + length;
            }
        }

        @Override
        public int writeFinal(byte[] src, int offset, int length) {
            if (length > 0) {
                checksum += (src[offset] & 0xff) + length;
            }
            return length;
        }

        @Override
        public void updateMetadata(MetadataUpdate update) {
        }

        @Override
        public void closeWrite() {
        }

        @Override
        public void cancelWrite(long code) {
        }

        @Override
        public void closeWithError(long code, String reason) {
        }

        @Override
        public void setWriteDeadline(Instant deadline) {
        }

        @Override
        public long streamId() {
            return 0L;
        }

        @Override
        public byte[] openInfo() {
            return new byte[0];
        }

        @Override
        public StreamMetadata metadata() {
            return StreamMetadata.empty();
        }

        @Override
        public SocketAddress localAddress() {
            return null;
        }

        @Override
        public SocketAddress remoteAddress() {
            return null;
        }
    }

    private static final class SourceReadHalf implements ReadHalf {
        private final byte[] source;
        private int offset;
        private int checksum;

        private SourceReadHalf(byte[] source) {
            this.source = source;
        }

        void reset() {
            offset = 0;
            checksum = 0;
        }

        @Override
        public int read(byte[] dst, int dstOffset, int length) {
            if (offset >= source.length) {
                return -1;
            }
            int read = Math.min(length, source.length - offset);
            System.arraycopy(source, offset, dst, dstOffset, read);
            if (read > 0) {
                checksum += (dst[dstOffset] & 0xff) + read;
                offset += read;
            }
            return read;
        }

        @Override
        public void closeRead() {
        }

        @Override
        public void setReadDeadline(Instant deadline) {
        }
    }
}
