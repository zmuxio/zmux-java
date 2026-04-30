package io.zmux;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

public interface ZmuxAsyncSendStream extends ZmuxSendStream {
    /**
     * Returns a stage that completes when this write reaches the backend's write completion point.
     * Native zmux streams complete after the writer has written and flushed the bytes to the
     * underlying connection. Native transport adapters complete when their native write primitive
     * accepts or rejects the data. Completion does not imply peer receipt or acknowledgement.
     * Cancelling the returned stage is not guaranteed to cancel the underlying write.
     */
    CompletionStage<Void> writeAsync(byte[] src, int offset, int length);

    default CompletionStage<Void> writeAsync(byte[] src) {
        Objects.requireNonNull(src, "src");
        return writeAsync(src, 0, src.length);
    }

    /**
     * Writes the final payload and closes the local send side asynchronously.
     * Cancelling the returned stage is not guaranteed to cancel the underlying write.
     */
    CompletionStage<Void> writeFinalAsync(byte[] src, int offset, int length);

    default CompletionStage<Void> writeFinalAsync(byte[] src) {
        Objects.requireNonNull(src, "src");
        return writeFinalAsync(src, 0, src.length);
    }

    CompletionStage<Void> closeWriteAsync();

    default CompletionStage<Void> cancelWriteAsync(ErrorCode code) {
        Objects.requireNonNull(code, "code");
        return cancelWriteAsync(code.code());
    }

    CompletionStage<Void> cancelWriteAsync(long code);

    default CompletionStage<Void> closeWithErrorAsync(ErrorCode code, String reason) {
        Objects.requireNonNull(code, "code");
        return closeWithErrorAsync(code.code(), reason);
    }

    CompletionStage<Void> closeWithErrorAsync(long code, String reason);
}
