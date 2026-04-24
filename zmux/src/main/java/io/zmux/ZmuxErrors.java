package io.zmux;

import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;

public final class ZmuxErrors {
    private ZmuxErrors() {
    }

    public static ZmuxErrorDetails details(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof ZmuxErrorDetails) {
                return (ZmuxErrorDetails) current;
            }
            current = current.getCause();
        }
        return null;
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
        return details(error) instanceof AdapterUnsupportedException;
    }

    public static boolean priorityUpdateUnavailable(Throwable error) {
        return details(error) instanceof PriorityUpdateUnavailableException;
    }

    public static boolean sessionClosed(Throwable error) {
        return details(error) instanceof SessionClosedException;
    }

    public static boolean readClosed(Throwable error) {
        return details(error) instanceof ReadClosedException;
    }

    public static boolean writeClosed(Throwable error) {
        return details(error) instanceof WriteClosedException;
    }

    public static boolean interrupted(Throwable error) {
        ZmuxErrorDetails details = details(error);
        return details != null
                ? details.interrupted()
                : error instanceof InterruptedException
                  || (error instanceof InterruptedIOException && !(error instanceof SocketTimeoutException));
    }
}
