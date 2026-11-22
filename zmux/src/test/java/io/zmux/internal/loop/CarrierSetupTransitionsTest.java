package io.zmux.internal.loop;

import static io.zmux.internal.loop.CarrierSetup.State.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.zmux.internal.loop.CarrierSetup.Event;
import io.zmux.internal.loop.CarrierSetup.State;
import java.util.EnumMap;
import org.junit.jupiter.api.Test;

class CarrierSetupTransitionsTest {
    private final EnumMap<State, EnumMap<Event, State>> accepted = new EnumMap<>(State.class);

    private void allow(State from, State to, Event... events) {
        for (var event : events) accepted.get(from).put(event, to);
    }

    @Test void everyStateEventPairHasItsContractualSuccessor() {
        for (var state : State.values()) accepted.put(state, new EnumMap<>(Event.class));
        for (var state : State.values()) {
            if (state == FINISHED || state == DETACHED_DONE) continue;
            var terminal = state == DETACHED || state == DETACHED_WITHDRAWN ? DETACHED_DONE : FINISHED;
            allow(state, terminal, Event.REJECT, Event.LOST, Event.FAIL);
        }
        allow(RESERVED, PENDING, Event.ARRIVE);
        for (var state : new State[]{RESERVED, PENDING, FALLBACK}) allow(state, WITHDRAWN, Event.CANCEL, Event.STOP);
        allow(PENDING, MATCHED, Event.MATCH);
        allow(PENDING, PUBLISHING, Event.CLAIM);
        allow(PENDING, STALLED, Event.STALL);
        allow(PENDING, FALLBACK, Event.FALLBACK);
        allow(MATCHED, MATCHED_WITHDRAWN, Event.CANCEL, Event.STOP);
        allow(MATCHED, PUBLISHING, Event.PAIR_CLAIM);
        allow(MATCHED_WITHDRAWN, PUBLISHING, Event.PAIR_CLAIM);
        allow(STALLED, WITHDRAWN, Event.STOP);
        allow(FALLBACK, DETACHED, Event.DETACH);
        allow(DETACHED, DETACHED_WITHDRAWN, Event.CANCEL, Event.STOP);
        for (var state : new State[]{PUBLISHING, FALLBACK, WITHDRAWN}) allow(state, FINISHED, Event.SUCCESS);
        for (var state : new State[]{DETACHED, DETACHED_WITHDRAWN}) allow(state, DETACHED_DONE, Event.SUCCESS);
        for (var state : State.values()) for (var event : Event.values()) {
            assertEquals(accepted.get(state).get(event), CarrierSetup.next(state, event), () -> state + " + " + event);
        }
    }
}
