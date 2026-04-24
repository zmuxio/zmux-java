package io.zmux;

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
}
