package io.zmux;

/**
 * A receiving part's window settings as it holds them ({@link ZmuxReceiveStream#receiveWindow()}).
 *
 * @param initial the initial window in bytes: the stream-level credit the part advertises first
 * @param max     the cap on the window auto-tuning may grow to; 0 when no fixed cap is set (automatic)
 */
public record ReceiveWindow(long initial, long max) {
    public ReceiveWindow {
        if (initial < 0 || max < 0) throw new IllegalArgumentException("negative receive window");
    }
}
