package io.zmux.adapter.quic.netty;

import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.ZmuxAsyncSession;

public interface NettyQuicAsyncSession extends ZmuxAsyncSession {
    QuicChannel unsafeQuicChannel();
}
