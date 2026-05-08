package io.zmux;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

public interface ZmuxAsyncSession extends ZmuxSession {
    /**
     * Cancelling returned stages may not cancel the underlying operation.
     */
    default CompletionStage<ZmuxAsyncStream> openStreamAsync() {
        return openStreamAsync(OpenOptions.empty());
    }

    CompletionStage<ZmuxAsyncStream> openStreamAsync(OpenOptions options);

    default CompletionStage<ZmuxAsyncSendStream> openUniStreamAsync() {
        return openUniStreamAsync(OpenOptions.empty());
    }

    CompletionStage<ZmuxAsyncSendStream> openUniStreamAsync(OpenOptions options);

    CompletionStage<ZmuxAsyncStream> acceptStreamAsync();

    CompletionStage<ZmuxAsyncRecvStream> acceptUniStreamAsync();

    CompletionStage<Void> closeAsync();

    default CompletionStage<Void> closeWithErrorAsync(ErrorCode code, String reason) {
        Objects.requireNonNull(code, "code");
        return closeWithErrorAsync(code.code(), reason);
    }

    CompletionStage<Void> closeWithErrorAsync(long code, String reason);
}
