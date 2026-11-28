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
            boolean dialer = transport == CoreConfig.Role.INITIATOR || (transport == CoreConfig.Role.AUTO && Boolean.TRUE.equals(designated));
            var params = dialer ? local : peer;
            boolean auto = peer != null && local.has(TransportParams.ROLE) && peer.has(TransportParams.ROLE)
                    && local.role == TransportParams.ROLE_AUTO && peer.role == TransportParams.ROLE_AUTO;
            return new Input(core.established() && core.error() == null, dialer, known, core.features().multipath(), auto,
                    params == null ? new byte[0] : params.sessionNonce.clone(), params == null ? 0 : params.roleNonce);
        });
    }
    static DuplicateResult resolve(ZmuxSession a, ZmuxSession b, DuplicateOptions options) throws IOException {
        Objects.requireNonNull(a); Objects.requireNonNull(b); Objects.requireNonNull(options);
        if (a == b) throw new DuplicateUndecidedException(DuplicateUndecidedException.Reason.SAME_SESSION);
        Input x = input(a), y = input(b);
        if (!x.established || !y.established) throw new DuplicateUndecidedException(DuplicateUndecidedException.Reason.NOT_ESTABLISHED);
        if (!x.knownDialer || !y.knownDialer) throw new DuplicateUndecidedException(DuplicateUndecidedException.Reason.UNKNOWN_DIALER);
        if (x.localDialer == y.localDialer) throw new DuplicateUndecidedException(DuplicateUndecidedException.Reason.SAME_DIALER);
        int comparison; DuplicateResult.Rule rule;
        byte[] local = options.localIdentity(), peer = options.peerIdentity();
        if (local.length > 0) {
            comparison = x.localDialer ? Arrays.compareUnsigned(local, peer) : Arrays.compareUnsigned(peer, local);
            rule = DuplicateResult.Rule.ENDPOINT_IDENTITY;
        } else if (x.multipath && y.multipath) {
            comparison = Arrays.compareUnsigned(x.sessionNonce, y.sessionNonce); rule = DuplicateResult.Rule.SESSION_NONCE;
        } else if (x.autoRole && y.autoRole) {
            comparison = Long.compareUnsigned(x.roleNonce, y.roleNonce); rule = DuplicateResult.Rule.ROLE_NONCE;
        } else throw new DuplicateUndecidedException(DuplicateUndecidedException.Reason.NO_COMMON_INPUT);
        if (comparison == 0) throw new DuplicateUndecidedException(DuplicateUndecidedException.Reason.EQUAL_VALUES);
        ZmuxSession survivor = comparison < 0 ? a : b, loser = comparison < 0 ? b : a;
        boolean dialedLoser = comparison < 0 ? y.localDialer : x.localDialer;
        // Commit on the owner before returning: local openers cannot race a posted shutdown.
        loser.run(op -> { loser.core.shutdown(0, null); return true; });
        Attempt attempt = null;
        if (options.rejoin() != null && dialedLoser && survivor.info().multipath()) {
            try { attempt = rejoin(survivor, options); }
            catch (IOException ignored) { /* A lost survivor or admission race cannot undo resolution. */ }
        }
        return new DuplicateResult(survivor, loser, rule, attempt);
    }

    private static Attempt rejoin(ZmuxSession session, DuplicateOptions options) throws IOException {
        var route = options.rejoin();
        var capacity = session.capacity();
        var request = new ChangeRequest(capacity.policyRevision(), capacity.observed(),
                java.time.Duration.ofNanos(session.core.joinDeadline()), java.util.List.of(route.member()), java.util.Set.of(), 0);
        // The attempt completion callback owns the change and cancellation registration after startup.
        //noinspection resource
        var change = session.beginChange(request);
        try {
            // The callback closes this registration after the attempt, or startup closes it on failure.
            //noinspection resource
            Cancellation.Registration cancellation = options.cancellation() == null
                    ? () -> {} : options.cancellation().onCancel(change::cancel);
            try {
                Attempt attempt = change.startMember(0, route.opener());
                attempt.completion().whenComplete((result, failure) -> {
                    try (change; cancellation) {
                        if (result != null && result.disposition() == Attempt.Disposition.CONFIRMED) change.commit();
                    } catch (IOException ignored) {
                        // Duplicate resolution already committed.
                    }
                });
                return attempt;
            } catch (IOException | RuntimeException | Error failure) {
                cancellation.close();
                throw failure;
            }
        } catch (IOException | RuntimeException | Error failure) {
            change.close();
            throw failure;
        }
    }
}
