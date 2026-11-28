package io.zmux.internal.buffer;

/** Counts each pinned allocation once. The caller serializes all access. */
public final class RootPins {
    private static final class Pin { private int refs = 1; }

    private ReadBuf first;
    private int firstRefs;
    private IdentityIndex<ReadBuf, Pin> others;
    private long bytes;

    public long bytes() { return bytes; }

    public boolean pin(ReadBuf buffer, long limit) {
        ReadBuf root = buffer.root();
        if (first == root) {
            firstRefs++;
            return true;
        }
        Pin pin = others == null ? null : others.get(root);
        if (pin != null) {
            pin.refs++;
            return true;
        }
        if (root.capacity() > limit - bytes) return false;
        if (first == null) {
            first = root;
            firstRefs = 1;
        } else {
            if (others == null) others = new IdentityIndex<>();
            others.put(root, new Pin());
        }
        bytes += root.capacity();
        return true;
    }

    public void unpin(ReadBuf buffer) {
        ReadBuf root = buffer.root();
        if (first == root) {
            if (--firstRefs != 0) return;
            first = null;
        } else {
            if (--others.get(root).refs != 0) return;
            others.remove(root);
            if (others.isEmpty()) others = null;
        }
        bytes -= root.capacity();
    }
}
