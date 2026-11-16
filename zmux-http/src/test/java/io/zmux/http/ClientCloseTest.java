package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.CarrierClientClosedException;
import io.zmux.Role;
import io.zmux.Zmux;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** closeAsync closes the client without waiting, once; later dials fail as final. */
@Timeout(30)
class ClientCloseTest {
    @Test void closeAsyncCompletesOnceAndEndsTheRunningCarriers() throws Exception {
        var c = ExchangeTest.cfg().build();
        try (var f = Fixture.plain(b -> b.exchange(c))) {
            var hc = f.client(b -> b);
            var session = Zmux.open(hc.exchangePipe(f.base.toString(), c), Role.CLIENT, f.config);
            var closing = hc.closeAsync();
            assertSame(closing.toCompletableFuture().get(10, TimeUnit.SECONDS), hc.closeAsync().get(1, TimeUnit.SECONDS));
            assertThrows(CarrierClientClosedException.class, () -> hc.exchangePipe(f.base.toString(), c));
            hc.close(); // after closeAsync: returns at once
            session.abort(0, "");
        }
    }
}
