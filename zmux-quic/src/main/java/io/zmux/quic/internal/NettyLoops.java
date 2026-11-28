package io.zmux.quic.internal;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;

/** Access to the event loop that Netty assigns to a channel. */
public final class NettyLoops {
    private NettyLoops() {}

    // The Netty group owns the loop; borrowing a channel's loop grants no shutdown right.
    //noinspection resource
    public static EventLoop getBorrowedEventLoop(Channel channel) { return channel.eventLoop(); }
}
