package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.AttachmentSpec;
import io.zmux.CapacitySnapshot;
import io.zmux.CarrierKind;
import io.zmux.Directions;
import io.zmux.PipeOptions;
import io.zmux.Role;
import io.zmux.SessionController;
import io.zmux.SessionTarget;
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A paired POST and GET supplies one bidirectional member. */
@Timeout(90)
class SplitJoinTest {
    static final String CANDIDATE = "split";

    static SessionController controller(HttpCarrierClient client, URI uri, PipeOptions options) {
        var candidate = new SessionController.Candidate(CANDIDATE, "split", CarrierKind.SPLIT, "", 0, false, false,
                List.of(new AttachmentSpec(Directions.BOTH, options)), client.splitOpener(uri, Map.of()));
        return new SessionController(new SessionController.Options(UUID.randomUUID().toString(), List.of(candidate),
                SessionController.Choice.STATIC, SessionController.Limits.DEFAULT.withMaximumOpeners(1).withAttemptTimeout(Duration.ofSeconds(45)), null, null, java.util.Map.of()));
    }

    static SessionTarget target() {
        return new SessionTarget(1, List.of(new SessionTarget.Group("route",
                List.of(new SessionTarget.Quota("member", 1, Directions.BOTH, List.of(CANDIDATE))), SessionTarget.Rotation.NONE)), List.of());
    }

    static CapacitySnapshot.Member member(ZmuxSession session, Predicate<CapacitySnapshot.Member> test) throws Exception {
        var capacity = session.capacity();
        for (;;) {
            var found = capacity.members().stream()
                    .filter(m -> m.candidate().equals(CANDIDATE) && m.covered() == Directions.BOTH && test.test(m))
                    .findFirst();
            if (found.isPresent()) return found.orElseThrow();
            capacity = session.awaitCapacityChange(capacity.observed()).get(45, TimeUnit.SECONDS);
        }
    }

    @Test void aSplitJoinUsesOneAttachmentAcrossItsPairedRequests() throws Exception {
        try (var f = new Fixture(ModeHttpTest.session(true, null), b -> b, b -> b.streaming(), false, null);
             var c = f.client(b -> b); var controller = controller(c, f.base, PipeOptions.defaults());
             var s = Zmux.open(c.pipe(f.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config);
             var server = f.listener.accept(Duration.ofSeconds(10)); var control = controller.manage(s, target())) {
            var first = member(s, m -> true);
            assertEquals(1, first.pipes().size());
            s.pipes().stream().filter(p -> p.id().equals(first.pipes().getFirst())).findFirst().orElseThrow().abort();
            var second = member(s, m -> !m.id().equals(first.id()));
            assertEquals(1, second.pipes().size());
            assertFalse(control.done().isDone());
            Fixture.exchange(s, server, 4);
        }
    }
}
