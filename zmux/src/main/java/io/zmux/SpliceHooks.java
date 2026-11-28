package io.zmux;

import java.util.List;

/** Immutable policy chains for one direction. Empty chains select direct forwarding for that shape. */
public record SpliceHooks(List<SpliceHook> stream, List<SpliceHook> capsule, List<SpliceHook> datagram) {
    public static final SpliceHooks NONE = new SpliceHooks(List.of(), List.of(), List.of());

    public SpliceHooks {
        stream = List.copyOf(stream);
        capsule = List.copyOf(capsule);
        datagram = List.copyOf(datagram);
    }

    List<SpliceHook> forShape(SpliceUnit.Shape shape) {
        return switch (shape) {
            case STREAM -> stream;
            case CAPSULE -> capsule;
            case DATAGRAM -> datagram;
        };
    }

    boolean empty() { return stream.isEmpty() && capsule.isEmpty() && datagram.isEmpty(); }

    void validate(boolean packets, int maxSteps) {
        if (packets ? !stream.isEmpty() : !capsule.isEmpty())
            throw new IllegalArgumentException("hook chain does not match the splice mode");
        if (stream.size() > maxSteps || capsule.size() > maxSteps || datagram.size() > maxSteps)
            throw new IllegalArgumentException("splice hook chain exceeds its step budget");
    }
}
