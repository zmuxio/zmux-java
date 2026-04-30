package io.zmux;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

public interface ZmuxAsyncRecvStream extends ZmuxRecvStream {
    CompletionStage<Void> closeReadAsync();

    default CompletionStage<Void> cancelReadAsync(ErrorCode code) {
        Objects.requireNonNull(code, "code");
        return cancelReadAsync(code.code());
    }

    CompletionStage<Void> cancelReadAsync(long code);

    default CompletionStage<Void> closeWithErrorAsync(ErrorCode code, String reason) {
        Objects.requireNonNull(code, "code");
        return closeWithErrorAsync(code.code(), reason);
    }

    CompletionStage<Void> closeWithErrorAsync(long code, String reason);
}
