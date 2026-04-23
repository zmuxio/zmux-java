package io.zmux;

final class RangeChecks {
    private RangeChecks() {
    }

    static void checkFromIndexSize(int fromIndex, int size, int length) {
        if ((fromIndex | size | length) < 0 || size > length - fromIndex) {
            throw new IndexOutOfBoundsException("range [" + fromIndex + ", " + fromIndex + " + " + size + ") out of bounds for length " + length);
        }
    }
}
