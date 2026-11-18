package io.zmux;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The state of a pool at one look ({@link PoolPolicy#decide}).
 *
 * @param members the pool's members; a replacement being joined is not one until it replaces its member
 * @param failures consecutive failed dial attempts of the pool (redials and replacements alike), 0 once a dial gives a
 *     usable member
 * @param sinceLastDial the time since the start of the pool's last dial attempt; empty before the first
 * @param dialing the pool's own growth dials in progress: the attempts it started to reach its count (first dials
 *     and redials), and a replacement whose member has gone meanwhile; not a replacement of a member still there, and
 *     not an on-demand dial (a dial the pool started for a caller waiting for a member). A count below the pool's
 *     size, members and dialing together, cancels these first. A pool that dials inside its looks (the pipe pools)
 *     always reports 0
 *
 * <p>The pool builds the view; the one public constructor is for a policy's tests. A shorter form is added only
 * once a release has shipped it.
 */
public record PoolView(List<PoolMember> members, int failures, Optional<Duration> sinceLastDial, int dialing) {
    public PoolView {
        members = List.copyOf(members);
        Objects.requireNonNull(sinceLastDial);
        if (failures < 0 || dialing < 0) throw new IllegalArgumentException("negative count");
    }
}
