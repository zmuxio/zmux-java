package io.zmux;

import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.IdentityHashMap;

public final class ZmuxErrors {
    private static final int MAX_ERROR_UNWRAP_DEPTH = 64;

    private ZmuxErrors() {
    }

    public static <T> T find(Throwable error, Class<T> type) {
        if (error == null || type == null) {
            return null;
        }
        return find(error, type, new IdentityHashMap<>(), 0);
    }

    private static <T> T find(Throwable error,
                              Class<T> type,
                              IdentityHashMap<Throwable, Boolean> seen,
                              int depth) {
        if (error == null || depth > MAX_ERROR_UNWRAP_DEPTH || seen.put(error, Boolean.TRUE) != null) {
            return null;
        }
        if (type.isInstance(error)) {
            return type.cast(error);
        }
        T found = find(error.getCause(), type, seen, depth + 1);
        if (found != null) {
            return found;
        }
        Throwable[] suppressed = error.getSuppressed();
        for (Throwable child : suppressed) {
            found = find(child, type, seen, depth + 1);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    public static ZmuxErrorDetails details(Throwable error) {
        return find(error, ZmuxErrorDetails.class);
    }

    public static ApplicationError applicationError(Throwable error) {
        return find(error, ApplicationError.class);
    }

    public static boolean hasCode(Throwable error) {
        ZmuxErrorDetails details = details(error);
        return details != null && details.hasCode();
    }

    public static long code(Throwable error, long fallbackCode) {
        ZmuxErrorDetails details = details(error);
        return details != null && details.hasCode() ? details.code() : fallbackCode;
    }

    public static ErrorCode code(Throwable error) {
        ZmuxErrorDetails details = details(error);
        if (details == null || !details.hasCode()) {
            return null;
        }
        try {
            return ErrorCode.fromCode(details.code());
        } catch (IllegalArgumentException unknown) {
            return null;
        }
    }

    public static boolean isCode(Throwable error, ErrorCode code) {
        return code != null && code.equals(code(error));
    }

    public static String operation(Throwable error) {
        ZmuxErrorDetails details = details(error);
        if (details == null) {
            return "";
        }
        String operation = details.operation();
        return operation == null ? "" : operation;
    }

    public static String reason(Throwable error) {
        if (error == null) {
            return "";
        }
        ZmuxErrorDetails details = details(error);
        if (details instanceof ApplicationError) {
            ApplicationError applicationError = (ApplicationError) details;
            return applicationError.reason();
        }
        if (details != null) {
            String reason = details.reason();
            if (reason != null && !reason.isEmpty()) {
                return reason;
            }
        }
        String message = error.getMessage();
        return message == null ? "" : message;
    }

    public static ZmuxErrorScope scope(Throwable error) {
        ZmuxErrorDetails details = details(error);
        return details == null ? ZmuxErrorScope.UNKNOWN : details.scope();
    }

    public static ZmuxErrorSource source(Throwable error) {
        ZmuxErrorDetails details = details(error);
        return details == null ? ZmuxErrorSource.UNKNOWN : details.source();
    }

    public static ZmuxErrorDirection direction(Throwable error) {
        ZmuxErrorDetails details = details(error);
        return details == null ? ZmuxErrorDirection.UNKNOWN : details.direction();
    }

    public static ZmuxTerminationKind terminationKind(Throwable error) {
        ZmuxErrorDetails details = details(error);
        return details == null ? ZmuxTerminationKind.UNKNOWN : details.terminationKind();
    }

    public static boolean timeout(Throwable error) {
        ZmuxErrorDetails details = details(error);
        return details != null ? details.timeout() : error instanceof SocketTimeoutException;
    }

    public static boolean adapterUnsupported(Throwable error) {
        return find(error, AdapterUnsupportedException.class) != null;
    }

    public static boolean priorityUpdateUnavailable(Throwable error) {
        return find(error, PriorityUpdateUnavailableException.class) != null;
    }

    public static boolean emptyMetadataUpdate(Throwable error) {
        return find(error, EmptyMetadataUpdateException.class) != null;
    }

    public static boolean openInfoUnavailable(Throwable error) {
        return find(error, OpenInfoUnavailableException.class) != null;
    }

    public static boolean openMetadataTooLarge(Throwable error) {
        return find(error, OpenMetadataTooLargeException.class) != null;
    }

    public static boolean openLimited(Throwable error) {
        return find(error, OpenLimitedException.class) != null;
    }

    public static boolean openExpired(Throwable error) {
        return find(error, OpenExpiredException.class) != null;
    }

    public static boolean priorityUpdateTooLarge(Throwable error) {
        return find(error, PriorityUpdateTooLargeException.class) != null;
    }

    public static boolean keepaliveTimeout(Throwable error) {
        ApplicationError applicationError = applicationError(error);
        return applicationError != null
                && applicationError.isCode(ErrorCode.IDLE_TIMEOUT)
                && "zmux: keepalive timeout".equals(applicationError.reason());
    }

    public static boolean sessionClosed(Throwable error) {
        return find(error, SessionClosedException.class) != null;
    }

    public static boolean readClosed(Throwable error) {
        return find(error, ReadClosedException.class) != null;
    }

    public static boolean streamNotReadable(Throwable error) {
        return find(error, StreamNotReadableException.class) != null;
    }

    public static boolean streamNotWritable(Throwable error) {
        return find(error, StreamNotWritableException.class) != null;
    }

    public static boolean writeClosed(Throwable error) {
        return find(error, WriteClosedException.class) != null;
    }

    public static boolean gracefulCloseTimeout(Throwable error) {
        return find(error, GracefulCloseTimeoutException.class) != null;
    }

    public static boolean interrupted(Throwable error) {
        ZmuxErrorDetails details = details(error);
        return details != null
                ? details.interrupted()
                : error instanceof InterruptedException
                  || (error instanceof InterruptedIOException && !(error instanceof SocketTimeoutException));
    }
}
