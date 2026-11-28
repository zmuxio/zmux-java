package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.Framing;
import io.zmux.carrier.MemoryPipe;
import io.zmux.carrier.PipeIo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class CapacityChangeTest {
    @Test void aSplitMemberConsumesTwoAttachmentsButSharesOnePhysicalConnection() throws Exception {
        try (var loops = EventLoops.create(2); var fixture = new SessionTestSupport.Fixture(SessionTestSupport.config(loops, 3))) {
            var initial = fixture.client.capacity();
            var spec = new MemberSpec("split", List.of(new AttachmentSpec(Directions.SEND, PipeOptions.defaults()),
                    new AttachmentSpec(Directions.RECEIVE, PipeOptions.defaults())));
            var request = new ChangeRequest(initial.policyRevision(), initial.observed(), Duration.ofSeconds(45), List.of(spec), Set.of(), 0);
            var observed = fixture.client.awaitCapacityChange(initial.observed());
            try (var change = fixture.client.beginChange(request)) {
                // Progress on the bootstrap pipe can also advance the observation revision.
                var snapshot = observed.get(30, TimeUnit.SECONDS);
                while (snapshot.reserved() != 2) {
                    snapshot = fixture.client.awaitCapacityChange(snapshot.observed()).get(30, TimeUnit.SECONDS);
                }
                var opening = new java.util.concurrent.CompletableFuture<Void>();
                Object physical = new Object();
                Attempt attempt = change.startMember(0, (setup, args) -> {
                    opening.join();
                    var opened = new ArrayList<PipeIo>();
                    for (int i = 0; i < args.layout().size(); i++) {
                        var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                        fixture.listener.adapter().handle(pair[1].withCarrierConn(physical), JoinHint.inBand(null));
                        opened.add(pair[0].withCarrierConn(physical));
                    }
                    return new CarrierOpener.Opened(opened);
                });
                try {
                    assertTrue(attempt.completion().cancel(false));
                    assertEquals(CapacityChange.Status.WAITING_CONFIRMATION, change.commit().status());
                } finally {
                    opening.complete(null);
                }
                var result = attempt.completion().get(30, TimeUnit.SECONDS);
                assertEquals(Attempt.Disposition.CONFIRMED, result.disposition(), () -> String.valueOf(result.error()));
                assertEquals(2, result.pipes().size());
                var committed = change.commit();
                assertEquals(CapacityChange.Status.COMMITTED, committed.status());
                var capacity = committed.capacity();
                assertEquals(3, capacity.localAttachments());
                assertEquals(0, capacity.reserved());
                assertEquals(2, capacity.localCarriers());
                var member = capacity.members().stream().filter(m -> m.id().equals(result.member())).findFirst().orElseThrow();
                assertEquals(result.pipes(), member.pipes());
                assertEquals(Directions.BOTH, member.covered());
            }
        }
    }
}
