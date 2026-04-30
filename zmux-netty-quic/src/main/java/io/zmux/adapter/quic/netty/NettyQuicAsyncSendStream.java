package io.zmux.adapter.quic.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.ZmuxAsyncSendStream;

public interface NettyQuicAsyncSendStream extends ZmuxAsyncSendStream {
    ChannelFuture writeNettyAsync(ByteBuf data);

    ChannelFuture writeFinalNettyAsync(ByteBuf data);

    QuicStreamChannel unsafeQuicStreamChannel();
}
