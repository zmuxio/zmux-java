package io.zmux;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public interface ZmuxStream extends ZmuxSendStream, ZmuxRecvStream {
    @Override
    void setDeadline(Instant deadline) throws IOException;

    default void setTimeout(Duration timeout) throws IOException {
        setDeadline(DeadlineSupport.after(timeout));
    }
}
