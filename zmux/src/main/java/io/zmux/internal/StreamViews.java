package io.zmux.internal;

import io.zmux.MetadataUpdate;
import io.zmux.StreamMetadata;
import io.zmux.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.time.Instant;
import java.util.concurrent.CompletionStage;

abstract class AbstractNativeStreamView {
    protected final StreamRuntime runtime;

    AbstractNativeStreamView(StreamRuntime runtime) {
        this.runtime = runtime;
    }

    public long streamId() {
        return runtime.streamId();
    }

    public byte[] openInfo() {
        return runtime.openInfo();
    }

    public StreamMetadata metadata() {
        return runtime.metadata();
    }

    public void closeWithError(long code, String reason) throws IOException {
        runtime.closeWithError(code, reason);
    }

    public CompletionStage<Void> closeWithErrorAsync(long code, String reason) {
        return runtime.closeWithErrorAsync(code, reason);
    }

    public boolean openedLocally() {
        return runtime.openedLocally();
    }

    public boolean bidirectional() {
        return runtime.bidirectional();
    }

    public SocketAddress localAddress() {
        return runtime.localAddress();
    }

    public SocketAddress remoteAddress() {
        return runtime.remoteAddress();
    }

    public void close() throws IOException {
        runtime.close();
    }
}

final class NativeSendStreamView extends AbstractNativeStreamView implements ZmuxNativeSendStream, ZmuxAsyncSendStream {
    NativeSendStreamView(StreamRuntime runtime) {
        super(runtime);
    }

    @Override
    public void write(byte[] src, int offset, int length) throws IOException {
        runtime.write(src, offset, length);
    }

    @Override
    public int writeFinal(byte[] src, int offset, int length) throws IOException {
        return runtime.writeFinal(src, offset, length);
    }

    @Override
    public CompletionStage<Void> writeAsync(byte[] src, int offset, int length) {
        return runtime.writeAsync(src, offset, length);
    }

    @Override
    public CompletionStage<Void> writeFinalAsync(byte[] src, int offset, int length) {
        return runtime.writeFinalAsync(src, offset, length);
    }

    @Override
    public int writevFinal(byte[]... parts) throws IOException {
        return runtime.writevFinal(parts);
    }

    @Override
    public void setWriteDeadline(Instant deadline) throws IOException {
        runtime.setWriteDeadline(deadline);
    }

    @Override
    public void updateMetadata(MetadataUpdate update) throws IOException {
        runtime.updateMetadata(update);
    }

    @Override
    public void closeWrite() throws IOException {
        runtime.closeWrite();
    }

    @Override
    public CompletionStage<Void> closeWriteAsync() {
        return runtime.closeWriteAsync();
    }

    @Override
    public void cancelWrite(long code) throws IOException {
        runtime.cancelWrite(code);
    }

    @Override
    public CompletionStage<Void> cancelWriteAsync(long code) {
        return runtime.cancelWriteAsync(code);
    }

    @Override
    public boolean writeClosed() {
        return runtime.writeClosed();
    }
}

final class NativeRecvStreamView extends AbstractNativeStreamView implements ZmuxNativeRecvStream, ZmuxAsyncRecvStream {
    NativeRecvStreamView(StreamRuntime runtime) {
        super(runtime);
    }

    @Override
    public int read(byte[] dst, int offset, int length) throws IOException {
        return runtime.read(dst, offset, length);
    }

    @Override
    public void setReadDeadline(Instant deadline) throws IOException {
        runtime.setReadDeadline(deadline);
    }

    @Override
    public void closeRead() throws IOException {
        runtime.closeRead();
    }

    @Override
    public CompletionStage<Void> closeReadAsync() {
        return runtime.closeReadAsync();
    }

    @Override
    public void cancelRead(long code) throws IOException {
        runtime.cancelRead(code);
    }

    @Override
    public CompletionStage<Void> cancelReadAsync(long code) {
        return runtime.cancelReadAsync(code);
    }

    @Override
    public boolean readClosed() {
        return runtime.readClosed();
    }
}
