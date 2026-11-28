package io.zmux.internal.core;

import java.util.BitSet;

/** Occupied receive positions, indexed modulo width. The session loop owns the window. */
final class DatagramWindow {
    private final BitSet occupied;
    private final int width;
    private long highest = -1;

    DatagramWindow() { this(CoreConfig.DEFAULT_DATAGRAM_WINDOW); }

    DatagramWindow(int width) {
        if (width < 64 || width > 16384) throw new IllegalArgumentException("datagram window must be between 64 and 16384");
        this.width = width;
        occupied = new BitSet(width);
    }

    boolean stale(long sequence) { return highest >= 0 && sequence <= highest - width; }

    boolean occupied(long sequence) {
        return sequence <= highest && !stale(sequence) && occupied.get(index(sequence));
    }

    void observe(long sequence) {
        if (sequence <= highest) return;
        long advance = sequence - highest;
        if (advance >= width) {
            occupied.clear();
        } else {
            int first = index(highest + 1);
            int end = first + (int) advance;
            occupied.clear(first, Math.min(end, width));
            if (end > width) occupied.clear(0, end - width);
        }
        highest = sequence;
    }

    void occupy(long sequence) {
        if (sequence >= 0 && !stale(sequence)) occupied.set(index(sequence));
    }

    void release(long sequence) {
        if (sequence >= 0 && !stale(sequence)) occupied.clear(index(sequence));
    }

    private int index(long sequence) { return (int) (sequence % width); }
}
