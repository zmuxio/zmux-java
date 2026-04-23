package io.zmux.adapter.quic.netty;

import io.zmux.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.time.Instant;

abstract class AbstractNettyQuicStream {
    final NettyQuicStreamState state;

    AbstractNettyQuicStream(NettyQuicStreamState state) {
        this.state = state;
    }

    public long streamId() {
        return state.streamId();
    }

    public byte[] openInfo() {
        return state.openInfo();
    }

    public StreamMetadata metadata() {
        return state.metadata();
    }

    public SocketAddress localAddress() {
        return state.localAddress();
    }

    public SocketAddress remoteAddress() {
        return state.remoteAddress();
    }

    final void applyDeadline(Instant deadline) throws IOException {
        state.setDeadline(deadline);
    }

    final void applyReadDeadline(Instant deadline) throws IOException {
        state.setReadDeadline(deadline);
    }

    final void applyWriteDeadline(Instant deadline) throws IOException {
        state.setWriteDeadline(deadline);
    }

    final boolean isBenignCloseError(IOException error) {
        return NettyQuicSupport.isBenignCloseError(error);
    }
}

final class NettyQuicBidiStream extends AbstractNettyQuicStream implements ZmuxStream {
    NettyQuicBidiStream(NettyQuicStreamState state) {
        super(state);
    }

    @Override
    public int read(byte[] dst, int offset, int length) throws IOException {
        return state.read(dst, offset, length);
    }

    @Override
    public void write(byte[] src, int offset, int length) throws IOException {
        state.write(src, offset, length);
    }

    @Override
    public int writeFinal(byte[] src, int offset, int length) throws IOException {
        return state.writeFinal(src, offset, length);
    }

    @Override
    public int writevFinal(byte[]... parts) throws IOException {
        return state.writevFinal(parts);
    }

    @Override
    public void setDeadline(Instant deadline) throws IOException {
        applyDeadline(deadline);
    }

    @Override
    public void setReadDeadline(Instant deadline) throws IOException {
        applyReadDeadline(deadline);
    }

    @Override
    public void setWriteDeadline(Instant deadline) throws IOException {
        applyWriteDeadline(deadline);
    }

    @Override
    public void updateMetadata(MetadataUpdate update) throws IOException {
        state.updateMetadata(update);
    }

    @Override
    public void closeRead() throws IOException {
        state.closeRead();
    }

    @Override
    public void cancelRead(long code) throws IOException {
        state.cancelRead(code);
    }

    @Override
    public void closeWrite() throws IOException {
        state.closeWrite();
    }

    @Override
    public void cancelWrite(long code) throws IOException {
        state.cancelWrite(code);
    }

    @Override
    public void closeWithError(long code, String reason) throws IOException {
        state.closeWithError(code, reason);
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        try {
            closeWrite();
        } catch (IOException error) {
            if (!isBenignCloseError(error)) {
                first = error;
            }
        }
        try {
            closeRead();
        } catch (IOException error) {
            if (first == null && !isBenignCloseError(error)) {
                first = error;
            }
        }
        if (first != null) {
            throw first;
        }
    }
}

final class NettyQuicSendStream extends AbstractNettyQuicStream implements ZmuxSendStream {
    NettyQuicSendStream(NettyQuicStreamState state) {
        super(state);
    }

    @Override
    public void write(byte[] src, int offset, int length) throws IOException {
        state.write(src, offset, length);
    }

    @Override
    public int writeFinal(byte[] src, int offset, int length) throws IOException {
        return state.writeFinal(src, offset, length);
    }

    @Override
    public int writevFinal(byte[]... parts) throws IOException {
        return state.writevFinal(parts);
    }

    @Override
    public void setWriteDeadline(Instant deadline) throws IOException {
        applyWriteDeadline(deadline);
    }

    @Override
    public void updateMetadata(MetadataUpdate update) throws IOException {
        state.updateMetadata(update);
    }

    @Override
    public void closeWrite() throws IOException {
        state.closeWrite();
    }

    @Override
    public void cancelWrite(long code) throws IOException {
        state.cancelWrite(code);
    }

    @Override
    public void closeWithError(long code, String reason) throws IOException {
        state.closeWriteWithError(code, reason);
    }

    @Override
    public void close() throws IOException {
        try {
            closeWrite();
        } catch (IOException error) {
            if (!isBenignCloseError(error)) {
                throw error;
            }
        }
    }
}

final class NettyQuicRecvStream extends AbstractNettyQuicStream implements ZmuxRecvStream {
    NettyQuicRecvStream(NettyQuicStreamState state) {
        super(state);
    }

    @Override
    public int read(byte[] dst, int offset, int length) throws IOException {
        return state.read(dst, offset, length);
    }

    @Override
    public void setReadDeadline(Instant deadline) throws IOException {
        applyReadDeadline(deadline);
    }

    @Override
    public void closeRead() throws IOException {
        state.closeRead();
    }

    @Override
    public void cancelRead(long code) throws IOException {
        state.cancelRead(code);
    }

    @Override
    public void closeWithError(long code, String reason) throws IOException {
        state.closeReadWithError(code, reason);
    }

    @Override
    public void close() throws IOException {
        try {
            closeRead();
        } catch (IOException error) {
            if (!isBenignCloseError(error)) {
                throw error;
            }
        }
    }
}
