package io.zmux;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

public interface ZmuxAsyncStream extends ZmuxStream, ZmuxAsyncSendStream, ZmuxAsyncRecvStream {
    CompletionStage<Void> closeAsync();

    @Override
    default CompletionStage<Void> closeWithErrorAsync(ErrorCode code, String reason) {
        Objects.requireNonNull(code, "code");
        return closeWithErrorAsync(code.code(), reason);
    }
}
