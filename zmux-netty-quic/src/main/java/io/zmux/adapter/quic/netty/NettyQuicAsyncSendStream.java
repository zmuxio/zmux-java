package io.zmux.adapter.quic.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.ZmuxAsyncSendStream;

public interface NettyQuicAsyncSendStream extends ZmuxAsyncSendStream {
    /**
     * Returns Netty's write-and-flush future; not a peer acknowledgement.
     */
    ChannelFuture writeNettyAsync(ByteBuf data);

    /**
     * Returns the final write and local output shutdown future.
     */
    ChannelFuture writeFinalNettyAsync(ByteBuf data);

    QuicStreamChannel unsafeQuicStreamChannel();
}
