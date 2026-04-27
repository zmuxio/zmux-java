package io.zmux.adapter.quic.netty;

import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.ZmuxSession;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public final class NettyQuic {
    public static final Duration DEFAULT_ACCEPTED_PRELUDE_READ_TIMEOUT = Duration.ofSeconds(5);

    static final int MAX_ACCEPTED_PRELUDE_MAX_CONCURRENT = 1024;
    private static final int BUILTIN_ACCEPTED_PRELUDE_MAX_CONCURRENT = 8;
    private static final AtomicInteger DEFAULT_ACCEPTED_PRELUDE_MAX_CONCURRENT =
            new AtomicInteger(BUILTIN_ACCEPTED_PRELUDE_MAX_CONCURRENT);

    private NettyQuic() {
    }

    public static int defaultAcceptedPreludeMaxConcurrent() {
        int current = DEFAULT_ACCEPTED_PRELUDE_MAX_CONCURRENT.get();
        return current > 0 ? clampAcceptedPreludeMaxConcurrent(current) : 1;
    }

    public static void setDefaultAcceptedPreludeMaxConcurrent(int maxConcurrent) {
        DEFAULT_ACCEPTED_PRELUDE_MAX_CONCURRENT.set(
                maxConcurrent > 0
                        ? clampAcceptedPreludeMaxConcurrent(maxConcurrent)
                        : BUILTIN_ACCEPTED_PRELUDE_MAX_CONCURRENT
        );
    }

    static int clampAcceptedPreludeMaxConcurrent(int maxConcurrent) {
        return Math.min(maxConcurrent, MAX_ACCEPTED_PRELUDE_MAX_CONCURRENT);
    }

    public static ZmuxSession wrapSession(QuicChannel channel) {
        return wrapSessionWithOptions(channel, NettyQuicSessionOptions.defaults());
    }

    public static ZmuxSession wrapSession(QuicChannel channel, Duration acceptedPreludeReadTimeout) {
        return wrapSessionWithOptions(channel, NettyQuicSessionOptions.ofAcceptedPreludeReadTimeout(acceptedPreludeReadTimeout));
    }

    public static ZmuxSession wrapSession(QuicChannel channel, int acceptedPreludeMaxConcurrent) {
        return wrapSessionWithOptions(channel, NettyQuicSessionOptions.ofAcceptedPreludeMaxConcurrent(acceptedPreludeMaxConcurrent));
    }

    public static ZmuxSession wrapSession(QuicChannel channel,
                                          Duration acceptedPreludeReadTimeout,
                                          int acceptedPreludeMaxConcurrent) {
        return wrapSessionWithOptions(
                channel,
                NettyQuicSessionOptions.defaults()
                        .withAcceptedPreludeReadTimeout(acceptedPreludeReadTimeout)
                        .withAcceptedPreludeMaxConcurrent(acceptedPreludeMaxConcurrent)
        );
    }

    public static ZmuxSession wrapSession(QuicChannel channel, NettyQuicSessionOptions options) {
        return wrapSessionWithOptions(channel, options);
    }

    public static ZmuxSession wrapSessionWithOptions(QuicChannel channel, NettyQuicSessionOptions options) {
        if (channel == null) {
            return NettyQuicSession.closedSession();
        }
        return new NettyQuicSession(channel, options == null ? NettyQuicSessionOptions.defaults() : options);
    }
}
