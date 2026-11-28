package io.zmux;

import io.zmux.internal.wire.Capsule;
import io.zmux.helper.Permit;
import java.util.function.BiConsumer;
import io.zmux.internal.core.Session.DatagramOptions;
import io.zmux.internal.core.Time;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** One admission and its current output. Owned by a direction until submission or disposal. */
final class SpliceGroup implements AutoCloseable {
    final SpliceUnit.Metadata metadata;
    final long admittedAt, sourceEnd;
    final SplicePolicy.Inject injection;
    private final List<SpliceWait.Resume> signals = new ArrayList<>();
    private final List<Runnable> unregister = new ArrayList<>();
    List<SplicePayload> output;
    private final SplicePayload.Budget budget;
    ByteBuffer originalHeader;
    long earliest, expiry, fifoOrder;
    SplicePolicy.QueueCharge queueCharge;
    SpliceDirection.Drop dropReason;
    int member, segment, segmentOffset;
    long targetAt;
    DatagramOptions options;
    int largestDatagram() {
        int largest = 0;
        for (var item : output) largest = Math.max(largest, item.length());
        return largest;
    }
    BiConsumer<SpliceUnit.Direction, SpliceOptions.Work> probe;
    SpliceGrants.Request request;
    Permit reservation;
    ByteBuffer[] headers;
    long logicalBytes, encodingCapacity;

    void prepare(boolean asCapsules, SpliceLimits limits) {
        capsule = asCapsules;
        logicalBytes = 0;
        encodingCapacity = 0;
        long type = metadata.capsuleType().orElse(Capsule.DATAGRAM);
        long context = metadata.contextId().orElse(-1);
        for (SplicePayload item : output) {
            int headerSize = !capsule ? 0 : originalHeader == null
                    ? Capsule.headerLength(type, context, item.length()) : originalHeader.remaining();
            logicalBytes += (long) item.length() + headerSize;
            encodingCapacity += capsule ? (originalHeader == null ? headerSize : 0) : datagram() ? item.copyForDatagram() : 0;
        }
        if (logicalBytes > limits.maxGroupBytes()) throw new SplicePayload.BudgetExceeded();
        output.getFirst().budget().requireRoom(encodingCapacity);
        if (capsule) {
            headers = new ByteBuffer[output.size()];
            for (int i = 0; i < headers.length; i++) {
                headers[i] = originalHeader == null
                        ? Capsule.encodeHeader(type, context, output.get(i).length())
                        : originalHeader.duplicate();
            }
        }
    }
    boolean capsule, started;
    int hookCalls, replacements;
    long waitingSince = -1;
    int submitted;
    long written, copied;
    ByteBuffer header;

    SpliceGroup(SpliceUnit.Metadata metadata, SplicePayload input, long at, long sourceEnd, long expiry,
                SplicePolicy.Inject injection, ByteBuffer originalHeader) {
        this.metadata = metadata;
        output = List.of(input);
        admittedAt = at;
        earliest = at;
        this.expiry = expiry;
        this.sourceEnd = sourceEnd;
        this.injection = injection;
        budget = input.budget();
        if (originalHeader != null) budget.acquire(originalHeader, originalHeader.capacity());
        this.originalHeader = originalHeader;
        capsule = metadata.shape() == SpliceUnit.Shape.CAPSULE;
    }

    /** Returns a terminal decision or null for a validated, possibly empty, output. */
    SpliceEnd transform(List<SpliceHook> chain, SpliceLimits limits, int maxItem, Runnable wake) {
        for (SpliceHook hook : chain) {
            hookCalls++;
            var unit = new SpliceUnit(metadata, output, limits, maxItem);
            try {
                if (probe != null) {
                    probe.accept(metadata.direction(), SpliceOptions.Work.HOOK_VIEW);
                    probe.accept(metadata.direction(), SpliceOptions.Work.HOOK_DISPATCH);
                }
                var decision = Objects.requireNonNull(hook.apply(unit));
                unit.checkBudget();
                switch (decision) {
                    case SpliceHook.Pass pass -> addWait(pass.waitFor(), limits.maxSegments());
                    case SpliceHook.Replace replace -> {
                        List<SplicePayload> replacement = unit.take(replace.outputs());
                        output.forEach(SplicePayload::close);
                        output = replacement;
                        releaseHeader();
                        replacements++;
                        addWait(replace.waitFor(), limits.maxSegments());
                    }
                    case SpliceHook.Drop ignored -> {
                        output.forEach(SplicePayload::close);
                        output = List.of();
                    }
                    case SpliceHook.Reset reset -> { return new SpliceEnd.HookReset(reset.code()); }
                }
            } catch (Exception | AssertionError error) {
                return new SpliceEnd.HookFailed(java.util.Optional.of(error));
            } finally {
                copied += unit.copied;
                unit.close();
            }
            if (output.isEmpty()) break;
        }
        if (!output.isEmpty()) {
            for (SpliceWait.Resume signal : signals) unregister.add(signal.listen(wake));
        }
        return null;
    }

    private void addWait(SpliceWait wait, int maxSignals) {
        if (wait == null) return;
        if (wait.signals().size() > maxSignals - signals.size())
            throw new IllegalArgumentException("splice recovery signal budget exhausted");
        earliest = Math.max(earliest, Time.add(admittedAt, wait.delay().toNanos()));
        expiry = Math.min(expiry, Time.add(admittedAt, wait.timeout().toNanos()));
        signals.addAll(wait.signals());
    }

    boolean ready(long now) {
        if (now < earliest) return false;
        for (SpliceWait.Resume signal : signals) if (!signal.signalled()) return false;
        return true;
    }

    long nextWake(long now) { return now < earliest ? Math.min(earliest, expiry) : expiry; }

    private void releaseHeader() {
        if (originalHeader != null) budget.release(originalHeader);
        originalHeader = null;
    }

    boolean datagram() { return metadata.shape() == SpliceUnit.Shape.DATAGRAM; }
    boolean memberStarted() { return capsule && started; }

    @Override public void close() {
        releaseHeader();
        if (request != null) request.revoke();
        if (reservation != null) reservation.close();
        unregister.forEach(Runnable::run);
        unregister.clear();
        signals.clear();
        output.forEach(SplicePayload::close);
        output = List.of();
    }
}
