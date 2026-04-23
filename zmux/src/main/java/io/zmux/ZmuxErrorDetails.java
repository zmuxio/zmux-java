package io.zmux;

import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;

public interface ZmuxErrorDetails {
    default long code() {
        return -1L;
    }

    default boolean hasCode() {
        return code() >= 0L;
    }

    default String operation() {
        return "";
    }

    default String reason() {
        return "";
    }

    default ZmuxErrorScope scope() {
        return ZmuxErrorScope.UNKNOWN;
    }

    default ZmuxErrorSource source() {
        return ZmuxErrorSource.UNKNOWN;
    }

    default ZmuxErrorDirection direction() {
        return ZmuxErrorDirection.UNKNOWN;
    }

    default ZmuxTerminationKind terminationKind() {
        return ZmuxTerminationKind.UNKNOWN;
    }

    default boolean timeout() {
        return this instanceof SocketTimeoutException || terminationKind() == ZmuxTerminationKind.TIMEOUT;
    }

    default boolean interrupted() {
        return terminationKind() == ZmuxTerminationKind.INTERRUPTED
                || this instanceof InterruptedException
                || (this instanceof InterruptedIOException && !timeout());
    }
}
