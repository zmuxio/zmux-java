package io.zmux;

import java.io.IOException;

/** A chunk read would exceed the session's allowance for outstanding receive buffers. */
public final class ChunkBudgetException extends IOException {
    ChunkBudgetException() {
        super("receive chunk budget exhausted");
    }
}
