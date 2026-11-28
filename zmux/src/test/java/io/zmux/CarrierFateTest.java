package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class CarrierFateTest {
    @Test void pooledFateIsPreservedAcrossCapabilitiesAndEncodedByTheOriginalJoin() throws Exception {
        try(EventLoops loops=EventLoops.create(2);var f=new SessionTestSupport.Fixture(SessionTestSupport.config(loops,4))) {
            var pair=MemoryPipe.pair(Framing.BYTE_STREAM);f.listener.adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("pooled"))).closed();
            var io=pair[0].withFateGroup(123).withProtocol("").withLabel("pooled").withReceiveMemory(0).withLaneOpener(null).withExporter(null);
            assertEquals(123,io.capabilities().fateGroup());
            var pipe=f.client.addPipe(io,PipeOptions.defaults());
            assertEquals(123L,f.client.run(op -> f.client.core.pipes().fateGroup(pipe.id().value())));
            assertEquals(123L,f.server.run(op -> f.server.core.pipes().fateGroup(pipe.id().value())));
            var invalid=MemoryPipe.pair(Framing.BYTE_STREAM);f.listener.adapter().handle(invalid[1], JoinHint.inBand(TestAddr.peer("conflict"))).closed();
            var override=f.client.addPipe(invalid[0].withFateGroup(123),PipeOptions.builder().fateGroup(124).build());
            assertEquals(124L,f.client.run(op -> f.client.core.pipes().fateGroup(override.id().value())));
            assertEquals(124L,f.server.run(op -> f.server.core.pipes().fateGroup(override.id().value())));
            assertEquals(PipeState.Outbound.ACTIVE, pipe.state().orElseThrow().outbound());
            assertThrows(IllegalArgumentException.class,() -> io.withFateGroup(1L<<62));
        }
    }
    @Test void anExplicitFateWinsOverTheCarriersDefault() throws Exception {
        try(EventLoops loops=EventLoops.create(2);var f=new SessionTestSupport.Fixture(SessionTestSupport.config(loops,4))) {
            var pair=MemoryPipe.pair(Framing.BYTE_STREAM);
            var lifetime=f.listener.adapter().handle(pair[1],JoinHint.inBand(TestAddr.peer("pooled"))).closed();
            var pipe=f.client.addPipe(pair[0].withFateGroup(123),PipeOptions.builder().fateGroup(321).build());
            assertEquals(PipeState.Outbound.ACTIVE, pipe.state().orElseThrow().outbound());
            assertEquals(321L,f.client.run(op -> f.client.core.pipes().fateGroup(pipe.id().value())));
            ListenerEntryTest.await(() -> f.server.pipes().size() == 2);
            assertEquals(321L,f.server.run(op -> f.server.core.pipes().fateGroup(pipe.id().value())));
            pipe.abort();lifetime.toCompletableFuture().get(3,TimeUnit.SECONDS);
        }
    }
}
