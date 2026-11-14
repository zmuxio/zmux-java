package io.zmux;

/**
 * Code the application supplied to zmux (a dial or redial hook, a limiter, a relay or
 * application-data callback) threw or broke its contract; the cause, when there is one, is what it
 * threw.
 */
public final class CallbackFailedException extends ZmuxException {
    public CallbackFailedException(String message) {
        super(message);
    }

    public CallbackFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
