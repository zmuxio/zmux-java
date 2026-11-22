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

    /**
     * An HTTP fallback that dispatches on its own event loop. The adapter calls
     * handoff immediately before delivering the request, and drops the delivery if it returns false.
     */
    public record RequestFallback(boolean notFound,
            java.util.function.BiFunction<io.zmux.Takeover, java.util.function.BooleanSupplier, CompletionStage<Void>> dispatch)
            implements io.zmux.Admission.Fallback {
        @Override public CompletionStage<Void> serve(io.zmux.Takeover request) {
            return dispatch.apply(request, () -> true);
        }
    }

    /** What a listener offers its adapter view. */
    public interface Listener {
        long stallNanos(InetSocketAddress source);

        JoinLookup knownJoin(ByteBuffer prefix);

        /** Takes a slot of source; false when there is none. */
        io.zmux.internal.loop.CarrierSetup reserve(InetSocketAddress source);

        /** Gives a reserved slot back, counted as a failed outer handshake. */
        void handshakeFailed(io.zmux.internal.loop.CarrierSetup setup);

        /** Hands over a carrier whose slot is held. */
        Carrier transfer(io.zmux.internal.loop.CarrierSetup setup, PipeIo io, JoinHint hint, boolean admitted, Object value, io.zmux.internal.loop.ListenerHub.Entry entry);

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
        public final io.zmux.internal.loop.CarrierSetup setup;
        public Carrier(io.zmux.internal.loop.CarrierSetup setup) { this.setup = setup; }
        private final CompletableFuture<AdmissionVerdict> verdict = new CompletableFuture<>();
        private final CompletableFuture<Boolean> taken = new CompletableFuture<>();
        private volatile Object value;
        private volatile io.zmux.internal.loop.Handback takeover;

        /** The verdict; completed by the listener only. */
        public CompletionStage<AdmissionVerdict> verdict() { return verdict.minimalCompletionStage(); }

        /** The verdict once decided, or null. */
        public AdmissionVerdict decided() { return verdict.isDone() && !verdict.isCompletedExceptionally() ? verdict.join() : null; }

        public CompletionStage<Boolean> taken() { return taken.minimalCompletionStage(); }

        public CompletionStage<Void> closed() { return setup.closed(); }

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

    }

    /** What a session offers its adapter view. */
    public interface Session {
        boolean joinReady() throws java.io.IOException;
    }

    /** A half whose close delegates cleanup to an adapter event loop. */
    public interface ReleasingHalf {
        CompletionStage<Void> released();
    }

    /** Joining the actual adapter work after close() has been called on both halves. */
    public static CompletableFuture<Void> released(PipeIo io) {
        return CompletableFuture.allOf(releasedHalf(io.reader()), releasedHalf(io.writer()));
    }

    public static CompletableFuture<Void> releasedHalf(Object half) {
        return half instanceof ReleasingHalf releasing ? releasing.released().toCompletableFuture() : RELEASED;
    }

    private static final CompletableFuture<Void> RELEASED = CompletableFuture.completedFuture(null);

    /** The setup lifecycle observed by request and connection adapters. */
    public interface Setup {
        void interrupt();
        void onFinished(Runnable callback);
    }

    /** Builds the views; io.zmux.adapter installs it when its classes initialise. */
    public interface Views {
        ListenerAdapter listener(Listener port);

        SessionAdapter session(Session port);
        Setup setup(io.zmux.adapter.Handled handled);
        Setup reservation(io.zmux.adapter.Slot slot);
    }

    private static final java.util.concurrent.atomic.AtomicReference<Views> VIEWS = new java.util.concurrent.atomic.AtomicReference<>();

    /** Installs io.zmux.adapter's view factory; once. */
    public static void install(Views v) {
        VIEWS.compareAndSet(null, v);
    }

    private static Views views() {
        Views v = VIEWS.get();
        if (v == null) {
            try {
                Class.forName("io.zmux.adapter.ListenerAdapter", true, AdapterPorts.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
            v = VIEWS.get();
        }
        return v;
    }

    public static ListenerAdapter listener(Listener port) { return views().listener(port); }

    public static Setup reservation(io.zmux.adapter.Slot slot) { return views().reservation(slot); }

    public static Setup setup(io.zmux.adapter.Handled handled) { return views().setup(handled); }

    public static SessionAdapter session(Session port) { return views().session(port); }
}
