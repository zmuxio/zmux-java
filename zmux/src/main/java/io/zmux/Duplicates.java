package io.zmux;

import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.wire.TransportParams;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/** No I/O and no second drain state: snapshots select; the losing session's core drains. */
final class Duplicates {
    private Duplicates() {}
    private record Input(boolean established, boolean localDialer, boolean knownDialer,
                         boolean multipath, boolean autoRole, byte[] sessionNonce, long roleNonce) {}
    private static Input input(ZmuxSession session) throws IOException {
        return (Input) session.run(op -> {
            var core = session.core; var local = core.localParams(); var peer = core.peerParams();
            CoreConfig.Role transport = core.transportRole();
            Boolean designated = session.designatedDialer();
            boolean known = transport != CoreConfig.Role.AUTO || designated != null;
            boolean dialer = transport == CoreConfig.Role.INITIATOR || transport == CoreConfig.Role.AUTO && Boolean.TRUE.equals(designated);
            var params = dialer ? local : peer;
            boolean auto = peer != null && local.has(TransportParams.ROLE) && peer.has(TransportParams.ROLE)
                    && local.role == TransportParams.ROLE_AUTO && peer.role == TransportParams.ROLE_AUTO;
            return new Input(core.established() && core.error() == null, dialer, known, core.features().multipath(), auto,
                    params == null ? new byte[0] : params.sessionNonce.clone(), params == null ? 0 : params.roleNonce);
        });
    }
    static DuplicateResult resolve(ZmuxSession a, ZmuxSession b, DuplicateOptions options) throws IOException {
        Objects.requireNonNull(a); Objects.requireNonNull(b); Objects.requireNonNull(options);
        if (a == b) throw new DuplicateResolutionException(DuplicateResolutionException.Reason.SAME_SESSION);
        Input x = input(a), y = input(b);
        if (!x.established || !y.established) throw new DuplicateResolutionException(DuplicateResolutionException.Reason.NOT_ESTABLISHED);
        if (!x.knownDialer || !y.knownDialer) throw new DuplicateResolutionException(DuplicateResolutionException.Reason.UNKNOWN_DIALER);
        if (x.localDialer == y.localDialer) throw new DuplicateResolutionException(DuplicateResolutionException.Reason.SAME_DIALER);
        int comparison; DuplicateResult.Rule rule;
        byte[] local = options.localIdentity(), peer = options.peerIdentity();
        if (local.length > 0) {
            comparison = x.localDialer ? Arrays.compareUnsigned(local, peer) : Arrays.compareUnsigned(peer, local);
            rule = DuplicateResult.Rule.ENDPOINT_IDENTITY;
        } else if (x.multipath && y.multipath) {
            comparison = Arrays.compareUnsigned(x.sessionNonce, y.sessionNonce); rule = DuplicateResult.Rule.SESSION_NONCE;
        } else if (x.autoRole && y.autoRole) {
            comparison = Long.compareUnsigned(x.roleNonce, y.roleNonce); rule = DuplicateResult.Rule.ROLE_NONCE;
        } else throw new DuplicateResolutionException(DuplicateResolutionException.Reason.NO_COMMON_INPUT);
        if (comparison == 0) throw new DuplicateResolutionException(DuplicateResolutionException.Reason.EQUAL_VALUES);
        ZmuxSession survivor = comparison < 0 ? a : b, loser = comparison < 0 ? b : a;
        boolean dialedLoser = comparison < 0 ? y.localDialer : x.localDialer;
        // Commit on the owner before returning: local openers cannot race a posted shutdown.
        loser.run(op -> { loser.core.shutdown(0, null); return Boolean.TRUE; });
        PipeMaintenance rejoin = null;
        try {
            if (options.rejoin() != null && dialedLoser && survivor.info().multipath() && survivor.joinReady())
                rejoin = PipeMaintenance.startOnce(survivor, options.rejoin(), options.cancellation());
        } catch (IOException ignored) { /* A lost survivor or admission race cannot undo resolution. */ }
        return new DuplicateResult(survivor, loser, rule, rejoin);
    }
}
