package io.zmux.internal.loop;

import io.zmux.internal.memory.SharedMemory;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.CompletionStage;

/** Accounts for each physical connection once, until its last I/O owner exits. */
final class CarrierCapacity {
    static final long OUTBOUND_WORKSPACE = io.zmux.internal.core.Batch.ENCODING_WORKSPACE
            + io.zmux.internal.core.Session.FRAME_HISTORY_WORKSPACE;

    private final SessionRuntime runtime;
    private static final class Users { int count; }

    private final IdentityHashMap<Object, Users> connections = new IdentityHashMap<>();

    CarrierCapacity(SessionRuntime runtime) { this.runtime = runtime; }

    /** Before attachment, reserve the provider, inbox and private workspace including a planned Sealed layer. */
    static boolean reserve(Transport transport, SharedMemory budget, boolean addSeal) {
        if (budget == null) return true;
        var info = transport.info();
        Object connection = info.connection();
        long local;
        try {
            local = transport.privateWorkspace();
            if ((transport.directions() & io.zmux.internal.core.Action.DIR_IN) != 0) {
                local = Math.addExact(local, transport.inputQueueCapacity());
            }
            if ((transport.directions() & io.zmux.internal.core.Action.DIR_OUT) != 0) {
                local = Math.addExact(local, OUTBOUND_WORKSPACE);
            }
            if (addSeal) local = Math.addExact(local, SealedTransport.workspace(transport.directions()));
        } catch (ArithmeticException unavailable) {
            return false;
        }
        var physical = (PhysicalTransport) transport;
        var tls = physical.tlsWorkspaces();
        if (connection == null && info.receiveMemory() == 0 && local == 0 && tls.isEmpty()) return true;
        Object backing = connection == null ? CarrierSetup.binding(transport) : connection;
        Runnable release = budget.acquireCarrier(io.zmux.internal.spi.CarrierBudget.physical(backing), info.receiveMemory(), local);
        if (release == null) return false;
        if (!tls.isEmpty() && !reserveTls(tls, budget)) {
            release.run();
            return false;
        }
        var closed = transport.whenClosed();
        if (addSeal) closed = physical.nextSeal().closeAfter(closed);
        if ((transport.directions() & io.zmux.internal.core.Action.DIR_OUT) != 0) {
            closed = physical.encoder().closeAfter(closed);
            closed = physical.history().closeAfter(closed);
        }
        closed.whenComplete((v, error) -> release.run());
        return true;
    }

    private static boolean reserveTls(List<TlsTransport.RecordWorkspace> workspaces, SharedMemory budget) {
        var leases = new Runnable[workspaces.size()];
        for (int i = 0; i < leases.length; i++) {
            leases[i] = workspaces.get(i).acquire(budget);
            if (leases[i] == null) {
                for (int made = 0; made < i; made++) leases[made].run();
                return false;
            }
        }
        for (int i = 0; i < leases.length; i++) workspaces.get(i).releaseOnClose(leases[i]);
        return true;
    }

    Lease acquire(Transport transport) {
        List<Object> ids = transport.physicalConnections();
        int newConnections = 0;
        for (Object id : ids) if (!connections.containsKey(id)) newConnections++;
        if (!runtime.core.acquireCarriers(newConnections)) return null;
        for (Object id : ids) connections.computeIfAbsent(id, key -> new Users()).count++;
        return new Lease(ids);
    }

    final class Lease {
        private final List<Object> ids;

        private Lease(List<Object> ids) { this.ids = ids; }

        void releaseAfter(CompletionStage<?> io) {
            io.whenComplete((value, error) -> runtime.executeCarrier(this::release));
        }

        private void release() {
            int last = 0;
            for (Object id : ids) {
                if (--connections.get(id).count == 0) {
                    connections.remove(id);
                    last++;
                }
            }
            runtime.core.releaseCarriers(last);
        }
    }
}
