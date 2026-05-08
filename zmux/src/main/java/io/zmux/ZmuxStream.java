package io.zmux;

import io.zmux.support.DeadlineSupport;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public interface ZmuxStream extends ZmuxSendStream, ZmuxRecvStream {
    @Override
    default void closeWithError(ErrorCode code, String reason) throws IOException {
        ZmuxSendStream.super.closeWithError(code, reason);
    }

    @Override
    void setDeadline(Instant deadline) throws IOException;

    default void setTimeout(Duration timeout) throws IOException {
        setDeadline(DeadlineSupport.after(timeout));
    }

    default void clearDeadline() throws IOException {
        setDeadline(null);
    }

    @Override
    default void close() throws IOException {
        IOException error = null;
        try {
            ZmuxSendStream.super.close();
        } catch (IOException closeError) {
            error = closeError;
        }
        try {
            ZmuxRecvStream.super.close();
        } catch (IOException closeError) {
            if (error == null) {
                error = closeError;
            } else {
                error.addSuppressed(closeError);
            }
        }
        if (error != null) {
            throw error;
        }
    }
}
