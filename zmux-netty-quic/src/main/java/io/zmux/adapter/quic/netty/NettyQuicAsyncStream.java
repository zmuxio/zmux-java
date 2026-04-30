package io.zmux.adapter.quic.netty;

import io.zmux.ZmuxAsyncStream;

public interface NettyQuicAsyncStream extends ZmuxAsyncStream, NettyQuicAsyncSendStream, NettyQuicAsyncRecvStream {
}
