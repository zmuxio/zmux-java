package io.zmux;

import io.zmux.internal.core.Action;
import io.zmux.internal.core.Event;
import io.zmux.internal.core.PipeInfo;
import java.time.Duration;

/** The single mapping of protocol events to their application representation. */
final class Events {
    private Events() {}
    static SessionEvent publicEvent(Event e) {
        return switch (e.kind()) {
            case GOAWAY_RECEIVED -> new SessionEvent.GoAwayReceived(e.maxBidi(), e.maxUni(), e.code());
            case DATAGRAM_DROPPED -> new SessionEvent.DatagramsDropped(e.count());
            case PIPE_STATE -> e.dir() == Action.DIR_OUT
                    ? new SessionEvent.OutboundPipeStateChanged(e.pipe(), e.carrier(), ((PipeInfo.Out) e.state()).toPublic())
                    : new SessionEvent.InboundPipeStateChanged(e.pipe(), e.carrier(), ((PipeInfo.In) e.state()).toPublic());
            case JOIN_AUTH_FAILED -> new SessionEvent.JoinAuthFailed(e.reason());
            case SUSPENDED -> new SessionEvent.Suspended();
            case RESUMED -> new SessionEvent.Resumed();
            case TIER_SATURATION -> new SessionEvent.TierSaturation(e.code(), e.flag());
            case PIPE_PRIORITY -> new SessionEvent.PipePriorityChanged(e.pipe(), e.carrier(), e.code(), e.flag(), e.count());
            case PIPE_QUALITY -> new SessionEvent.PipeQualityChanged(e.pipe(), e.carrier(), Duration.ofNanos(e.rtt()), e.rate());
            case CAPACITY_BELOW -> new SessionEvent.CapacityBelow(e.rate());
            case CAPACITY_RESTORED -> new SessionEvent.CapacityRestored(e.rate());
            default -> null;
        };
    }
}
