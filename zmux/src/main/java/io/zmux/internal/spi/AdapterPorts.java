package io.zmux.internal.spi;

import io.zmux.AdmissionVerdict;
import io.zmux.JoinHint;
import io.zmux.adapter.JoinLookup;
import io.zmux.adapter.ListenerAdapter;
import io.zmux.adapter.SessionAdapter;
import io.zmux.carrier.PipeIo;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * The bridge between the objects of io.zmux and the views of io.zmux.adapter, inside the
 * module: an object implements a port, and io.zmux.adapter builds its view on the port.
 */
public final class AdapterPorts {
    private AdapterPorts() {}

    /** What a listener offers its adapter view. */
    public interface Listener {
        long stallNanos(InetSocketAddress source);

        JoinLookup knownJoin(ByteBuffer prefix);

        /** Takes a slot of source; false when there is none. */
        boolean reserve(InetSocketAddress source);

        /** Gives a reserved slot back unused. */
        void release(InetSocketAddress source);

        /** Gives a reserved slot back, counted as a failed outer handshake. */
        void handshakeFailed(InetSocketAddress source);

        /** Hands over a carrier whose slot is held. */
        Carrier transfer(PipeIo io, JoinHint hint, boolean admitted, Object value, io.zmux.internal.loop.ListenerHub.Entry entry);

        /** Closes a carrier refused at its slot. */
        Carrier refused(PipeIo io);

        void countSetupFailure();

        Duration firstRecordTimeout();

        long memoryUsed();
    }

    /**
     * A carrier handed to a listener: its admission verdict, whether a session took it, the end
     * of its lifetime, and (once the verdict is decided) the admission's value.
     */
    public static final class Carrier {
        private final CompletableFuture<AdmissionVerdict> verdict = new CompletableFuture<>();
        private final CompletableFuture<Boolean> taken = new CompletableFuture<>();
        private final CompletableFuture<Void> closed = new CompletableFuture<>();
        private volatile Object value;
        private volatile io.zmux.internal.loop.Handback takeover;

        /** The verdict; completed by the listener only. */
        public CompletionStage<AdmissionVerdict> verdict() { return verdict.minimalCompletionStage(); }

        /** The verdict once decided, or null. */
        public AdmissionVerdict decided() { return verdict.isDone() && !verdict.isCompletedExceptionally() ? verdict.join() : null; }

        public CompletionStage<Boolean> taken() { return taken.minimalCompletionStage(); }

        public CompletionStage<Void> closed() { return closed.minimalCompletionStage(); }

        /** The admission's value once the verdict is decided. */
        public Object value() { return value; }

        /** Reports the verdict with the admission's value; the listener's side. */
        public void decide(AdmissionVerdict v, Object admitted) { decide(v, admitted, null); }

        /** Reports the verdict with the admission's value and the takeover of the listener's fallback (or null). */
        public void decide(AdmissionVerdict v, Object admitted, io.zmux.internal.loop.Handback t) { value = admitted; takeover = t; verdict.complete(v); }

        /** Whether the listener's fallback took the carrier over, once the verdict is decided. */
        public boolean takenOver() { return takeover != null; }

        /** Whether the host detached the carrier it took over. */
        public boolean detached() { var t = takeover; return t != null && t.detached(); }

        /** Reports whether a session took the carrier; the listener's side. */
        public void taken(boolean t) { taken.complete(t); }

        /** Reports the end of the carrier's lifetime, failed with error (null: none); the listener's side. */
        public void ended(Throwable error) {
            if (error == null) closed.complete(null); else closed.completeExceptionally(error);
        }
    }

    /** What a session offers its adapter view. */
    public interface Session {
        boolean joinReady() throws java.io.IOException;
    }

    /** Builds the views; io.zmux.adapter installs it when its classes initialise. */
    public interface Views {
        ListenerAdapter listener(Listener port);

        SessionAdapter session(Session port);
    }

    private static volatile Views views;

    /** Installs io.zmux.adapter's view factory; once. */
    public static void install(Views v) {
        if (views == null) views = v;
    }

    private static Views views() {
        Views v = views;
        if (v == null) {
            try {
                Class.forName("io.zmux.adapter.ListenerAdapter", true, AdapterPorts.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
            v = views;
        }
        return v;
    }

    public static ListenerAdapter listener(Listener port) { return views().listener(port); }

    public static SessionAdapter session(Session port) { return views().session(port); }
}
