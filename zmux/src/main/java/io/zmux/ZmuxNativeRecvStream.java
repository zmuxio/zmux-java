package io.zmux;

public interface ZmuxNativeRecvStream extends ZmuxRecvStream {
    boolean openedLocally();

    boolean bidirectional();

    boolean readClosed();
}
