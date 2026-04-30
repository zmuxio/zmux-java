package io.zmux.adapter.quic.netty;

import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.ZmuxAsyncRecvStream;

public interface NettyQuicAsyncRecvStream extends ZmuxAsyncRecvStream {
    QuicStreamChannel unsafeQuicStreamChannel();
}
