package io.zmux;

@FunctionalInterface
public interface ZmuxEventHandler {
    void onEvent(ZmuxEvent event);
}
