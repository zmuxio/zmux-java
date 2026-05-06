package io.zmux;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public final class ZmuxEvent {
    private final ZmuxEventType type;
    private final SessionState sessionState;
    private final long streamId;
    private final ZmuxStreamInfo stream;
    private final boolean local;
    private final boolean bidirectional;
    private final Instant time;
    private final IOException error;
    private final boolean applicationVisible;

    public ZmuxEvent(ZmuxEventType type,
                     SessionState sessionState,
                     long streamId,
                     ZmuxStreamInfo stream,
                     boolean local,
                     boolean bidirectional,
                     Instant time,
                     IOException error,
                     boolean applicationVisible) {
        this.type = type;
        this.sessionState = sessionState;
        this.streamId = streamId;
        this.stream = stream;
        this.local = local;
        this.bidirectional = bidirectional;
        this.time = time;
        this.error = error;
        this.applicationVisible = applicationVisible;
    }

    public ZmuxEventType type() {
        return type;
    }

    public SessionState sessionState() {
        return sessionState;
    }

    public long streamId() {
        return streamId;
    }

    public ZmuxStreamInfo stream() {
        return stream;
    }

    public boolean local() {
        return local;
    }

    public boolean bidirectional() {
        return bidirectional;
    }

    public Instant time() {
        return time;
    }

    public IOException error() {
        return error;
    }

    public boolean applicationVisible() {
        return applicationVisible;
    }

    public ZmuxErrorDetails errorDetails() {
        return ZmuxErrors.details(error);
    }

    public boolean errorHasCode() {
        return ZmuxErrors.hasCode(error);
    }

    public long errorCode(long fallbackCode) {
        return ZmuxErrors.code(error, fallbackCode);
    }

    public String errorOperation() {
        return ZmuxErrors.operation(error);
    }

    public String errorReason() {
        return ZmuxErrors.reason(error);
    }

    public ZmuxErrorScope errorScope() {
        return ZmuxErrors.scope(error);
    }

    public ZmuxErrorSource errorSource() {
        return ZmuxErrors.source(error);
    }

    public ZmuxErrorDirection errorDirection() {
        return ZmuxErrors.direction(error);
    }

    public ZmuxTerminationKind errorTerminationKind() {
        return ZmuxErrors.terminationKind(error);
    }

    public boolean errorTimeout() {
        return ZmuxErrors.timeout(error);
    }

    public boolean errorInterrupted() {
        return ZmuxErrors.interrupted(error);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ZmuxEvent)) {
            return false;
        }
        ZmuxEvent that = (ZmuxEvent) other;
        return streamId == that.streamId
                && local == that.local
                && bidirectional == that.bidirectional
                && applicationVisible == that.applicationVisible
                && type == that.type
                && sessionState == that.sessionState
                && Objects.equals(stream, that.stream)
                && Objects.equals(time, that.time)
                && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, sessionState, streamId, stream, local, bidirectional, time, error, applicationVisible);
    }

    @Override
    public String toString() {
        return "ZmuxEvent[type=" + type
                + ", sessionState=" + sessionState
                + ", streamId=" + streamId
                + ", stream=" + stream
                + ", local=" + local
                + ", bidirectional=" + bidirectional
                + ", time=" + time
                + ", error=" + error
                + ", applicationVisible=" + applicationVisible
                + "]";
    }
}
