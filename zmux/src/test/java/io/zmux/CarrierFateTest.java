package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class CarrierFateTest {
    @Test void pooledFateIsPreservedAcrossCapabilitiesAndEncodedByTheOriginalJoin() throws Exception {
        try(EventLoops loops=EventLoops.create(2);var f=new PipePoolTest.Fixture(PipePoolTest.config(loops,4))) {
            var pair=MemoryPipe.pair(Framing.BYTE_STREAM);f.listener.handle(pair[1], JoinHint.inBand(TestAddr.peer("pooled"))).closed();
            var io=pair[0].withFateGroup(123).withProtocol("").withLabel("pooled").withReceiveMemory(0).withLaneOpener(null).withExporter(null);
            assertEquals(123,io.capabilities().fateGroup());
            var pipe=f.client.paths().add(io,PipeOptions.defaults());
            assertEquals(123L,f.client.run(op -> f.client.core.pipes().fateGroup(pipe.id())));
            assertEquals(123L,f.server.run(op -> f.server.core.pipes().fateGroup(pipe.id())));
            var invalid=MemoryPipe.pair(Framing.BYTE_STREAM);f.listener.handle(invalid[1], JoinHint.inBand(TestAddr.peer("conflict"))).closed();
            var override=f.client.paths().add(invalid[0].withFateGroup(123),PipeOptions.builder().fateGroup(124).build());
            assertEquals(124L,f.client.run(op -> f.client.core.pipes().fateGroup(override.id())));
            assertEquals(124L,f.server.run(op -> f.server.core.pipes().fateGroup(override.id())));
            assertEquals(PipeState.Outbound.ACTIVE, pipe.state().orElseThrow().outbound());
            assertThrows(IllegalArgumentException.class,() -> io.withFateGroup(1L<<62));
        }
    }
    @Test void reservationChoosesAnExplicitFateBeforeSigningRatherThanChangingItOnAttach() throws Exception {
        try(EventLoops loops=EventLoops.create(2);var f=new PipePoolTest.Fixture(PipePoolTest.config(loops,4))) {
            try(var reservation=f.client.paths().reserve(PipeOptions.builder().fateGroup(321).build())) {
                var pair=MemoryPipe.pair(Framing.BYTE_STREAM);
                var lifetime=f.listener.handle(pair[1],JoinHint.join(reservation.header(), TestAddr.peer("pooled"))).closed();
                var pipe=reservation.attach(pair[0].withFateGroup(123));assertEquals(PipeState.Outbound.ACTIVE, pipe.state().orElseThrow().outbound());
                assertEquals(321L,f.client.run(op -> f.client.core.pipes().fateGroup(pipe.id())));
                assertEquals(321L,f.server.run(op -> f.server.core.pipes().fateGroup(pipe.id())));
                pipe.abort();lifetime.toCompletableFuture().get(3,TimeUnit.SECONDS);
            }
            try(var reservation=f.client.paths().reserve(PipeOptions.defaults())) {
                var pair=MemoryPipe.pair(Framing.BYTE_STREAM);
                assertThrows(IllegalStateException.class,() -> reservation.attach(pair[0].withFateGroup(123)));
                pair[1].reader().close();pair[1].writer().close();
            }
        }
    }
}
