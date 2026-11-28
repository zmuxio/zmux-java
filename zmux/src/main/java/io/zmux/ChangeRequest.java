package io.zmux;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** A capacity change against the policy revision and observation of a session. */
public record ChangeRequest(long policyRevision, long observed, Duration timeout, List<MemberSpec> add,
                            Set<MemberId> drain, long minEpoch) {
    public ChangeRequest {
        Objects.requireNonNull(timeout);
        if (timeout.isNegative() || timeout.isZero() || minEpoch < 0) throw new IllegalArgumentException("invalid change deadline or epoch");
        add = List.copyOf(add);
        drain = Set.copyOf(drain);
    }
}
