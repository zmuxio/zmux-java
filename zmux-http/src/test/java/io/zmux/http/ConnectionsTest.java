package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ConnectionsTest {
    @Test void afterCloseAConnectionIsClosedInsteadOfPooled() {
        var conns = new Connections(null, null, null, Connections.Version.HTTP_1_1, Duration.ofSeconds(1));
        URI u = URI.create("http://localhost:1/z");
        var kept = new EmbeddedChannel();
        conns.release(u, kept, true);
        assertTrue(kept.isActive());
        conns.close(); // closes the connections it opened; kept was only lent to the pool, which forgets it
        kept.close();
        var late = new EmbeddedChannel();
        conns.release(u, late, true);
        assertFalse(late.isActive(), "nothing goes back to a closed pool");
        assertTrue(conns.open(u, true, false).isCompletedExceptionally());
    }
}
