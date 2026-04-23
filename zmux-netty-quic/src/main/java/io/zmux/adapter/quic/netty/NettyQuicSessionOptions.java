package io.zmux.adapter.quic.netty;

import java.time.Duration;
import java.util.Objects;

public final class NettyQuicSessionOptions {
    private final Duration acceptedPreludeReadTimeout;
    private final int acceptedPreludeMaxConcurrent;

    public NettyQuicSessionOptions(Duration acceptedPreludeReadTimeout, int acceptedPreludeMaxConcurrent) {
        this.acceptedPreludeReadTimeout = acceptedPreludeReadTimeout == null ? Duration.ZERO : acceptedPreludeReadTimeout;
        this.acceptedPreludeMaxConcurrent = acceptedPreludeMaxConcurrent;
    }

    public static NettyQuicSessionOptions defaults() {
        return new NettyQuicSessionOptions(Duration.ZERO, 0);
    }

    static Duration normalizeAcceptedPreludeReadTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero()) {
            return NettyQuic.DEFAULT_ACCEPTED_PRELUDE_READ_TIMEOUT;
        }
        if (timeout.isNegative()) {
            return null;
        }
        return timeout;
    }

    static int normalizeAcceptedPreludeMaxConcurrent(int maxConcurrent) {
        return maxConcurrent > 0
                ? maxConcurrent
                : NettyQuic.defaultAcceptedPreludeMaxConcurrent();
    }

    Duration normalizedAcceptedPreludeReadTimeout() {
        return normalizeAcceptedPreludeReadTimeout(acceptedPreludeReadTimeout);
    }

    int normalizedAcceptedPreludeMaxConcurrent() {
        return normalizeAcceptedPreludeMaxConcurrent(acceptedPreludeMaxConcurrent);
    }

    public Duration acceptedPreludeReadTimeout() {
        return acceptedPreludeReadTimeout;
    }

    public int acceptedPreludeMaxConcurrent() {
        return acceptedPreludeMaxConcurrent;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof NettyQuicSessionOptions)) {
            return false;
        }
        NettyQuicSessionOptions that = (NettyQuicSessionOptions) other;
        return acceptedPreludeMaxConcurrent == that.acceptedPreludeMaxConcurrent
                && Objects.equals(acceptedPreludeReadTimeout, that.acceptedPreludeReadTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(acceptedPreludeReadTimeout, acceptedPreludeMaxConcurrent);
    }

    @Override
    public String toString() {
        return "NettyQuicSessionOptions[acceptedPreludeReadTimeout=" + acceptedPreludeReadTimeout
                + ", acceptedPreludeMaxConcurrent=" + acceptedPreludeMaxConcurrent
                + "]";
    }
}
