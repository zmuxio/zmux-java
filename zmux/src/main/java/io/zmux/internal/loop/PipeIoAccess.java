package io.zmux.internal.loop;

import io.zmux.RecordMode;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.join.RecordProtection;

/**
 * The library's own access to what an Establish leaves on a {@link PipeIo}, which hosts neither set nor read: the
 * first pipe's record mode, the session's host value and a Sealed carrier's record protection. {@link PipeIo}
 * installs the one implementation when it is initialized; every caller holds a {@link PipeIo}, so it is in place.
 */
public final class PipeIoAccess {
    /** The operations {@link PipeIo} provides. */
    public interface Access {
        /**
         * io after an Establish ran on it: a secure first pipe with the handshake's record mode. exporter is the
         * handshake's; records protects the records when the mode is Sealed (ignored otherwise); peer (or null) is
         * the session's host value.
         */
        PipeIo withEstablished(PipeIo io, RecordMode mode, PipeIo.Exporter exporter, RecordProtection records, Object peer);

        /** io with the session's host value peer, and nothing else of an Establish. */
        PipeIo withCarrierPeer(PipeIo io, Object peer);

        /** The first pipe's record mode an Establish gave io; null without one. */
        RecordMode recordMode(PipeIo io);

        /** The session's host value an Establish gave io; null without one. */
        Object carrierPeer(PipeIo io);

        /** The outbound connection identity, even when a split has two different connections. */
        Object sendingConnection(PipeIo io);

        /** The physical connection identities of the carrier, or empty when unavailable. */
        java.util.List<Object> physicalConnections(PipeIo io);

        /** The record protection of a Sealed io; null otherwise. */
        RecordProtection records(PipeIo io);
    }

    private static volatile Access access;

    private PipeIoAccess() {}

    /** Called once, by {@link PipeIo}'s initialization. */
    public static void set(Access a) {
        if (access != null) throw new IllegalStateException("already set");
        access = a;
    }

    public static Access get() {
        Access a = access;
        if (a == null) {
            try {
                Class.forName(PipeIo.class.getName(), true, PipeIo.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e);
            }
            a = access;
        }
        return a;
    }
}
