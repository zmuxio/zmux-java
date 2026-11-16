package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.channel.DefaultEventLoop;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class ExchangeCarrierLoopTest {
    /**
     * A carrier that lost its creation is dropped unregistered: its loop's termination neither ends it
     * nor makes the server remember a carrier that never existed.
     */
    @Test void aCarrierThatNeverWonItsCreationIgnoresItsLoopTerminating() throws Exception {
        try (var f = Fixture.plain(b -> b.exchange(ExchangeTest.cfg().build()))) {
            ExchangeServer server = f.endpoint.exchange;
            var loop = new DefaultEventLoop();
            byte[] cid = new byte[Exchange.CID];
            ExchangeTest.RANDOM.nextBytes(cid);
            var loser = server.new Carrier(loop, cid, Exchange.key(ExchangeTest.PSK, cid));
            // Listeners of the termination future run in registration order: this one after any the carrier added.
            var after = new CountDownLatch(1);
            loop.terminationFuture().addListener(x -> after.countDown());
            assertTrue(loop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).await(10, TimeUnit.SECONDS));
            assertTrue(after.await(10, TimeUnit.SECONDS));
            assertEquals(0, server.remembered(), "a carrier that never existed was remembered as gone");
            assertNotNull(loser);
        }
    }
}
