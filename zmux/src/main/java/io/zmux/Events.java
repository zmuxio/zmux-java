package io.zmux;

import io.zmux.internal.core.Action;
import io.zmux.internal.core.Event;
import java.time.Duration;

/** The single mapping of protocol events to their application representation. */
final class Events {
    private Events() {}
    static SessionEvent publicEvent(Event e) {
        return switch (e.kind()) {
            case GOAWAY_RECEIVED -> new SessionEvent.GoAwayReceived(e.maxBidi(), e.maxUni(), e.code());
            case PIPE_ADDED -> new SessionEvent.PipeAdded(PipeId.of(e.pipe()), e.carrier(), direction(e));
            case PIPE_JOINED -> new SessionEvent.PipeJoined(PipeId.of(e.pipe()), e.carrier(), direction(e));
            case PIPE_SUSPECT -> new SessionEvent.PipeSuspect(PipeId.of(e.pipe()), e.carrier(), direction(e));
            case PIPE_RECOVERED -> new SessionEvent.PipeRecovered(PipeId.of(e.pipe()), e.carrier(), direction(e));
            case PIPE_FAILED -> new SessionEvent.PipeFailed(PipeId.of(e.pipe()), e.carrier(), direction(e));
            case PIPE_CLOSED -> new SessionEvent.PipeClosed(PipeId.of(e.pipe()), e.carrier(), direction(e));
            case JOIN_AUTH_FAILED -> new SessionEvent.JoinAuthFailed(e.reason(), e.count());
            case SUSPENDED -> new SessionEvent.SessionSuspended();
            case RESUME_UNATTENDED -> new SessionEvent.ResumeUnattended();
            case RESUMED -> new SessionEvent.SessionResumed();
            case TIER_SATURATION -> new SessionEvent.TierSaturation(e.code(), e.flag());
            case PIPE_PRIORITY -> new SessionEvent.PipePriorityChanged(PipeId.of(e.pipe()), e.carrier(), e.code(), e.flag(), e.count(),
                    e.initiator() ? PipeStats.SettingSource.PEER : PipeStats.SettingSource.LOCAL);
            case PIPE_QUALITY -> new SessionEvent.PipeQualityChanged(PipeId.of(e.pipe()), e.carrier(), Duration.ofNanos(e.rtt()), (long) e.rate());
            case CAPACITY_BELOW -> new SessionEvent.CapacityBelow((long) e.rate());
            case CAPACITY_RESTORED -> new SessionEvent.CapacityRestored((long) e.rate());
            case EPOCH_COMMITTED -> new SessionEvent.EpochCommitted(e.code());
            case EPOCH_DELETED -> new SessionEvent.EpochDeleted(e.code());
            default -> null;
        };
    }

    /**
     * The event as the tracer sees it: each occurrence, before the event queue merges it, so a failed Hello has
     * count 1 and each retired pipe is one {@link SessionEvent.PipesRetired} of its own; null: not traced.
     */
    static SessionEvent traceEvent(Event e) {
        if (e.kind() != Event.Kind.PIPE_STATE) return publicEvent(e);
        if (e.state() != io.zmux.internal.core.PipeInfo.Out.RETIRED) return null;
        return new SessionEvent.PipesRetired(1, e.flag() ? 1 : 0, e.flag() ? 0 : 1, java.util.List.of(PipeId.of(e.pipe())));
    }

    private static PipeOptions.Direction direction(Event e) {
        return e.dir() == Action.DIR_OUT ? PipeOptions.Direction.SEND_ONLY : PipeOptions.Direction.RECEIVE_ONLY;
    }
}
