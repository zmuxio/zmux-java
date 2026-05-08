package io.zmux.protocol;

import java.util.Objects;

public final class ParsedFrame {
    private final Frame frame;
    private final int bytesRead;

    public ParsedFrame(Frame frame, int bytesRead) {
        this.frame = Objects.requireNonNull(frame, "frame");
        if (bytesRead <= 0) {
            throw new IllegalArgumentException("zmux parsed frame bytesRead must be > 0");
        }
        this.bytesRead = bytesRead;
    }

    public Frame frame() {
        return frame;
    }

    public int bytesRead() {
        return bytesRead;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ParsedFrame)) {
            return false;
        }
        ParsedFrame that = (ParsedFrame) other;
        return bytesRead == that.bytesRead && frame.equals(that.frame);
    }

    @Override
    public int hashCode() {
        return 31 * frame.hashCode() + Integer.hashCode(bytesRead);
    }

    @Override
    public String toString() {
        return "ParsedFrame[frame=" + frame + ", bytesRead=" + bytesRead + "]";
    }
}
