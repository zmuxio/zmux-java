package io.zmux;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** A callback-scoped view of one splice admission and its current ordered outputs. */
public final class SpliceUnit {
    /** The source and destination legs in call order. */
    public enum Direction { A_TO_B, B_TO_A }
    /** The application data boundary of the admitted unit. */
    public enum Shape { STREAM, CAPSULE, DATAGRAM }
    /** How the unit entered the splice. */
    public enum Origin { FORWARDED, INJECTED }

    /**
     * Missing hop IDs, stream offsets, capsule types and Context IDs use empty optionals.
     * Tags are the labels supplied by the splice caller.
     */
    public record Metadata(Direction direction, Shape shape, Origin origin, long sequence, long generation,
                           OptionalLong inputOffset, OptionalLong sourceId, OptionalLong targetId,
                           OptionalLong capsuleType, OptionalLong contextId, boolean replayable,
                           Preamble preamble, String sourceTag, String targetTag) {}

    private final Metadata metadata;
    private final Thread owner = Thread.currentThread();
    private final SpliceLimits limits;
    private final int maxItem;
    private List<SplicePayload> current;
    private final List<Output> allocated = new ArrayList<>();
    private int bytes, segments;
    private boolean exceeded;
    long copied;

    private SplicePayload.BudgetExceeded breach() {
        exceeded = true;
        return new SplicePayload.BudgetExceeded();
    }

    void checkBudget() {
        if (exceeded) throw new SplicePayload.BudgetExceeded();
    }

    SpliceUnit(Metadata metadata, List<SplicePayload> current, SpliceLimits limits, int maxItem) {
        this.metadata = metadata;
        this.current = current;
        this.limits = limits;
        this.maxItem = maxItem;
    }

    public Metadata metadata() { return metadata; }

    public int size() { check(); return current.size(); }

    public Item item(int index) {
        check();
        Objects.checkIndex(index, current.size());
        return new Item(index);
    }

    private void check() {
        if (owner != Thread.currentThread() || current == null)
            throw new IllegalStateException("splice view used outside its callback");
    }

    /**
     * Allocates a zeroed output. Fill it with indexed operations and return it in a replacement.
     * The output is sealed when the callback returns.
     */
    public Output allocate(int length) {
        return allocate(length, length);
    }

    /** Allocates a bounded output view from a backing allocation of the given capacity. */
    public Output allocate(int length, int capacity) {
        check();
        if (capacity < length) throw new IllegalArgumentException("capacity below output length");
        reserve(length);
        SplicePayload.Budget budget = current.getFirst().budget();
        if (!budget.fits(null, capacity)) throw breach();
        byte[] data = budget.allocate(capacity);
        return register(SplicePayload.of(budget, data, length), data);
    }

    private void reserve(int length) {
        if (length < 0 || length > maxItem || length > limits.maxGroupBytes() - bytes
                || allocated.size() >= limits.maxOutputs())
            throw breach();
    }

    private Output register(SplicePayload payload, byte[] writable) {
        int n = payload.segments();
        if (n > limits.maxSegments() - segments) {
            payload.close();
            throw breach();
        }
        Output output = new Output(this, payload, writable);
        allocated.add(output);
        bytes += payload.length();
        segments += n;
        return output;
    }

    List<SplicePayload> take(List<Output> outputs) {
        check();
        if (outputs.size() > limits.maxOutputs()) throw new SplicePayload.BudgetExceeded();
        List<SplicePayload> result = new ArrayList<>(outputs.size());
        try {
            for (Output output : outputs) {
                if (output.scope != this || output.payload == null)
                    throw new IllegalArgumentException("foreign or repeated splice output");
                result.add(output.payload);
                output.payload = null;
            }
            return result;
        } catch (RuntimeException | Error failure) {
            result.forEach(SplicePayload::close);
            throw failure;
        }
    }

    void close() {
        current = null;
        for (Output output : allocated) {
            if (output.payload != null) output.payload.close();
            output.payload = null;
            output.writable = null;
        }
        allocated.clear();
    }

    /** One read-only item in the current group. */
    public final class Item {
        private final int index;
        private Item(int index) { this.index = index; }

        /** Borrows the current payload, owned by the group for this callback. */
        private SplicePayload getBorrowedPayload() { check(); return current.get(index); }

        public int length() { return getBorrowedPayload().length(); }
        public byte get(int offset) { return getBorrowedPayload().get(offset); }
        public void copyTo(int offset, ByteBuffer destination, int length) {
            getBorrowedPayload().copyTo(offset, destination, length);
            copied += length;
        }

        /** Selects an output view without copying the underlying bytes. */
        public Output slice(int offset, int length) {
            SplicePayload input = getBorrowedPayload();
            reserve(length);
            return register(input.slice(offset, length), null);
        }
    }

    /** A replacement handle from this callback. Only allocated handles accept writes. */
    public static final class Output {
        private final SpliceUnit scope;
        private SplicePayload payload;
        private byte[] writable;

        private Output(SpliceUnit scope, SplicePayload payload, byte[] writable) {
            this.scope = scope;
            this.payload = payload;
            this.writable = writable;
        }

        private byte[] writable() {
            scope.check();
            if (writable == null) throw new IllegalStateException("read-only splice output");
            return writable;
        }

        public void put(int offset, byte value) {
            byte[] target = writable();
            target[Objects.checkIndex(offset, payload.length())] = value;
        }

        public void put(int offset, ByteBuffer source) {
            byte[] target = writable();
            int length = source.remaining();
            Objects.checkFromIndexSize(offset, length, payload.length());
            source.get(target, offset, length);
            scope.copied += length;
        }

        public void copyFrom(int offset, Item source, int from, int length) {
            byte[] target = writable();
            Objects.checkFromIndexSize(offset, length, payload.length());
            source.copyTo(from, ByteBuffer.wrap(target, offset, length), length);
        }
    }
}
