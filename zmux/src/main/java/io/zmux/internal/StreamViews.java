package io.zmux.internal;

import io.zmux.MetadataUpdate;
import io.zmux.StreamMetadata;
import io.zmux.ZmuxNativeRecvStream;
import io.zmux.ZmuxNativeSendStream;

import java.io.IOException;
import java.net.SocketAddress;
import java.time.Instant;

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

final class NativeSendStreamView extends AbstractNativeStreamView implements ZmuxNativeSendStream {
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
    public void cancelWrite(long code) throws IOException {
        runtime.cancelWrite(code);
    }

    @Override
    public boolean writeClosed() {
        return runtime.writeClosed();
    }
}

final class NativeRecvStreamView extends AbstractNativeStreamView implements ZmuxNativeRecvStream {
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
    public void cancelRead(long code) throws IOException {
        runtime.cancelRead(code);
    }

    @Override
    public boolean readClosed() {
        return runtime.readClosed();
    }
}
