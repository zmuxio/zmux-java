package io.zmux.adapter.quic.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.zmux.ZmuxAsyncSendStream;

public interface NettyQuicAsyncSendStream extends ZmuxAsyncSendStream {
    /**
     * Returns Netty's write-and-flush future for this stream. Completion means Netty/native QUIC
     * accepted or rejected the outbound write, not that the peer received it.
     */
    ChannelFuture writeNettyAsync(ByteBuf data);

    /**
     * Returns a future that completes after Netty/native QUIC accepts or rejects the final data
     * write and local output shutdown, not after peer receipt.
     */
    ChannelFuture writeFinalNettyAsync(ByteBuf data);

    QuicStreamChannel unsafeQuicStreamChannel();
}
