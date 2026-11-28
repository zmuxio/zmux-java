package io.zmux;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.IdentityHashMap;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.SpliceController.Receipt;
import io.zmux.SpliceController.Rejection;
import io.zmux.SpliceController.Accepted;
import io.zmux.SpliceUnit.Shape;
import io.zmux.internal.loop.SerialCallbacks;
import io.zmux.internal.wire.Capsule;
import io.zmux.internal.wire.VarInt;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** The bounded external mailbox and direction-owned configuration. */
final class SplicePolicy {
    sealed interface Command permits Install, Pause, Resume, Inject {}
    record Install(SpliceHooks hooks, CompletableFuture<Long> result) implements Command {}
    record Pause(CompletableFuture<Long> result) implements Command {}
    record Resume(long generation, CompletableFuture<Boolean> result) implements Command {}
    static final class Inject implements Command {
        final Shape shape;
        final long type, context;
        final CompletableFuture<Receipt> admission;
        final CompletableFuture<Receipt> disposition;
        private byte[] bytes;
        Accepted identity;
        QueueCharge charge;

        Inject(Shape shape, long type, long context, byte[] bytes, CompletableFuture<Receipt> admission,
               CompletableFuture<Receipt> disposition) {
            this.shape = shape;
            this.type = type;
            this.context = context;
            this.bytes = bytes;
            this.admission = admission;
            this.disposition = disposition;
            charge = new QueueCharge(Map.of(bytes, new SpliceRoots.Size(bytes.length)), 1);
        }

        byte[] take() {
            byte[] result = bytes;
            bytes = null;
            return result;
        }
    }

    final SpliceSettlement settlement;
    final Supplier<CompletionStage<SpliceDirection>> snapshot;
    private final boolean packets;
    private final int chunk, maxPacket, maxOpaque;
    private final SpliceLimits limits;
    private final Runnable wake;
    private final ArrayDeque<Command> inbox = new ArrayDeque<>();
    private final SerialCallbacks completions = new SerialCallbacks();
    private final CompletableFuture<SpliceEnd> finished;
    private int queuedUnits;
    private final SpliceRoots queueRoots = new SpliceRoots();
    record QueueCharge(Map<Object, SpliceRoots.Size> roots, int items) {}
    private final Consumer<SpliceController.Event> events;
    private Rejection closedReason = Rejection.CLOSED;
    private boolean closed;

    record Configuration(SpliceHooks hooks, long generation, long pauseGeneration, boolean paused) {}

    private volatile Configuration config;
    private CompletableFuture<Long> pendingPause;

    SplicePolicy(boolean packets, SpliceOptions options, SpliceHooks hooks, boolean controlled, Runnable wake, SpliceSettlement settlement, SpliceUnit.Direction direction,
                 Supplier<CompletionStage<SpliceDirection>> snapshot) {
        this.snapshot = snapshot;
        this.settlement = settlement;
        events = options.events == null ? null : event -> options.events.accept(direction, event);
        this.packets = packets;
        chunk = options.chunk();
        maxPacket = options.maxPacketPayload();
        maxOpaque = options.maxOpaqueValue();
        limits = options.limits();
        config = new Configuration(hooks, hooks.empty() ? 0 : 1, 0, false);
        this.wake = wake;
        finished = controlled ? new CompletableFuture<>() : null;
    }

    CompletionStage<Long> replaceHooks(SpliceHooks newHooks) {
        Objects.requireNonNull(newHooks).validate(packets, limits.maxHookSteps());
        CompletableFuture<Long> result = new CompletableFuture<>();
        if (offer(new Install(newHooks, result), result)) wake.run();
        return result.minimalCompletionStage();
    }

    CompletionStage<Long> pause() {
        CompletableFuture<Long> result = new CompletableFuture<>();
        if (offer(new Pause(result), result)) wake.run();
        return result.minimalCompletionStage();
    }

    CompletionStage<Boolean> resume(long pause) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        synchronized (this) {
            if (closed) return CompletableFuture.completedStage(false);
            if (!offer(new Resume(pause, result), result)) return result.minimalCompletionStage();
        }
        wake.run();
        return result.minimalCompletionStage();
    }

    private synchronized boolean offer(Command command, CompletableFuture<?> result) {
        if (closed || inbox.size() >= limits.maxQueuedUnits()) {
            result.completeExceptionally(new IllegalStateException("splice control closed or full"));
            return false;
        }
        inbox.addLast(command);
        return true;
    }

    CompletionStage<SpliceEnd> finished() { return finished.minimalCompletionStage(); }

    void event(SpliceController.Event event) {
        if (events != null) completions.execute(() -> events.accept(event));
    }

    void finish(SpliceEnd reason) {
        if (events != null) event(new SpliceController.DirectionFinished(reason));
        if (finished == null) return;
        if (reason == null) {
            completions.execute(() -> finished.completeExceptionally(new java.util.concurrent.CancellationException("splice cancelled")));
        } else complete(finished, reason);
    }

    SpliceController.Injection inject(Shape shape, long type, long context, ByteBuffer input) {
        Objects.requireNonNull(input);
        CompletableFuture<Receipt> admission = new CompletableFuture<>();
        CompletableFuture<Receipt> disposition = new CompletableFuture<>();
        var ticket = new SpliceController.Injection(admission.minimalCompletionStage(), disposition.minimalCompletionStage());
        int n = input.remaining();
        if (!valid(shape, type, context, n)) {
            var rejected = new SpliceController.Rejected(Rejection.INVALID_PARAMETER);
            admission.complete(rejected);
            disposition.complete(rejected);
            return ticket;
        }
        boolean queued;
        synchronized (this) {
            queued = !closed && queuedUnits < limits.maxQueuedUnits() && n <= limits.maxQueuedBytes() - queueRoots.bytes();
            if (queued) {
                byte[] data = new byte[n];
                input.duplicate().get(data);
                var command = new Inject(shape, type, context, data, admission, disposition);
                queueRoots.retain(data, n);
                queuedUnits++;
                inbox.addLast(command);
            } else {
                var rejected = new SpliceController.Rejected(closed ? closedReason : Rejection.SPLICE_BUDGET);
                admission.complete(rejected);
                disposition.complete(rejected);
            }
        }
        if (queued) wake.run();
        return ticket;
    }

    private boolean valid(Shape shape, long type, long context, int n) {
        if (n > maxItem(shape, type) || n > limits.maxGroupBytes()) return false;
        return switch (shape) {
            case STREAM -> !packets;
            case CAPSULE -> packets && type >= 0 && type <= VarInt.MAX
                    && (type == Capsule.DATAGRAM ? validContext(context) : context == -1);
            case DATAGRAM -> validContext(context);
        };
    }

    private static boolean validContext(long context) { return context >= 0 && context <= VarInt.MAX; }

    int maxItem(Shape shape, long type) {
        return switch (shape) {
            case STREAM -> chunk;
            case CAPSULE -> type == Capsule.DATAGRAM ? maxPacket : maxOpaque;
            case DATAGRAM -> maxPacket;
        };
    }

    synchronized QueueCharge reserveReceive(ReadBuf root) {
        if (queuedUnits >= limits.maxQueuedUnits() || root.capacity() > limits.maxBackingBytes()) return null;
        var roots = Map.<Object, SpliceRoots.Size>of(root.root(), new SpliceRoots.Size(root.capacity()));
        if (!queueRoots.retain(roots, limits.maxQueuedBytes())) return null;
        queuedUnits++;
        return new QueueCharge(roots, 1);
    }

    synchronized boolean queue(SpliceGroup group) {
        var roots = new IdentityHashMap<Object, SpliceRoots.Size>();
        for (var item : group.output) item.roots(roots);
        if (group.encodingCapacity > 0) roots.put(group, new SpliceRoots.Size(group.encodingCapacity));
        QueueCharge old = group.queueCharge;
        int items = group.output.size();
        if (items > limits.maxQueuedUnits() - queuedUnits + (old == null ? 0 : old.items())) return false;
        if (old != null) releaseQueue(old);
        if (!queueRoots.retain(roots, limits.maxQueuedBytes())) {
            if (old != null) {
                old.roots().forEach((root, size) -> queueRoots.retain(root, size.bytes()));
                queuedUnits += old.items();
            }
            return false;
        }
        group.queueCharge = new QueueCharge(roots, items);
        queuedUnits += items;
        return true;
    }

    synchronized void releaseQueue(QueueCharge charge) {
        queuedUnits -= charge.items();
        charge.roots().keySet().forEach(queueRoots::release);
    }

    synchronized Command poll() { return inbox.pollFirst(); }

    Configuration configuration() { return config; }

    void apply(Command command) {
        Configuration current = config;
        switch (command) {
            case Install install -> {
                long next = current.generation() + 1;
                config = new Configuration(install.hooks(), next, current.pauseGeneration(), current.paused());
                if (events != null) event(new SpliceController.HooksReplaced(next));
                complete(install.result(), next);
            }
            case Pause pause -> {
                if (!current.paused()) {
                    config = new Configuration(current.hooks(), current.generation(), current.pauseGeneration() + 1, true);
                }
                if (pendingPause != null) fail(pendingPause, "pause confirmation superseded");
                pendingPause = pause.result();
            }
            case Resume resume -> {
                boolean matches = resume.generation() == current.pauseGeneration() && current.pauseGeneration() != 0;
                if (matches) {
                    config = new Configuration(current.hooks(), current.generation(), current.pauseGeneration(), false);
                    if (pendingPause != null) fail(pendingPause, "resumed before pause confirmation");
                    pendingPause = null;
                }
                complete(resume.result(), matches);
            }
            case Inject ignored -> throw new AssertionError();
        }
    }

    void confirmPause(boolean atBoundary) {
        if (pendingPause != null && atBoundary) {
            if (events != null) event(new SpliceController.PauseAcknowledged(config.pauseGeneration()));
            complete(pendingPause, config.pauseGeneration());
            pendingPause = null;
        }
    }

    void rejected(Inject injection, Rejection reason) {
        injection.take();
        releaseQueue(injection.charge);
        var rejected = new SpliceController.Rejected(reason);
        complete(injection.admission, rejected);
        complete(injection.disposition, rejected);
    }

    void accepted(SpliceGroup group) {
        var injection = group.injection;
        injection.identity = new Accepted(group.metadata.generation(), group.metadata.sequence());
        group.queueCharge = injection.charge;
        injection.charge = null;
        complete(injection.admission, injection.identity);
    }

    void settled(SpliceGroup group, Splice.Disposition disposition, SpliceEnd end) {
        var injection = group.injection;
        long bytes = group.written;
        int items = group.submitted;
        switch (disposition) {
            case SUBMITTED -> complete(injection.disposition, new SpliceController.Submitted(injection.identity, bytes, items));
            case DROPPED -> complete(injection.disposition, new SpliceController.Dropped(injection.identity, group.dropReason));
            case DIRECTION_ENDED -> {
                if (end != null) complete(injection.disposition, new SpliceController.DirectionEnded(injection.identity, end, bytes, items));
                else finished.thenAccept(reason -> complete(injection.disposition,
                        new SpliceController.DirectionEnded(injection.identity, reason, bytes, items)));
            }
        }
    }

    <T> void complete(CompletableFuture<T> result, T value) {
        completions.execute(() -> result.complete(value));
    }

    private void fail(CompletableFuture<?> result, String message) {
        completions.execute(() -> result.completeExceptionally(new IllegalStateException(message)));
    }

    void close() { close(Rejection.CLOSED); }

    void close(Rejection why) {
        synchronized (this) {
            if (closed) return;
            closedReason = why;
            closed = true;
        }
        if (pendingPause != null) {
            fail(pendingPause, "splice direction closed");
            pendingPause = null;
        }
        for (Command command; (command = poll()) != null;) {
            switch (command) {
                case Install install -> fail(install.result(), "splice direction closed");
                case Pause pause -> fail(pause.result(), "splice direction closed");
                case Resume resume -> complete(resume.result(), false);
                case Inject inject -> rejected(inject, why);
            }
        }
    }
}
