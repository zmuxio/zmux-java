package io.zmux;

public interface ZmuxNativeSendStream extends ZmuxSendStream {
    boolean openedLocally();

    boolean bidirectional();

    boolean writeClosed();
}
