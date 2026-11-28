package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Carriers through a relay that cuts connections, swallows bytes or refuses new connections. */
@Timeout(60)
class RelayTest {
    static ZmuxConfig.Builder multipath(int pipes) {
        return ZmuxConfig.builder().sessionPolicy(new SessionPolicy(new CapacityLimits(pipes))).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd()).noProbation();
    }

    static ZmuxConfig config(EventLoops loops, ZmuxConfig.Builder m) { return m.eventLoops(loops).build(); }



    static <T extends SessionEvent> T event(ZmuxSession s, Class<T> kind, java.util.function.Predicate<T> match) throws Exception {
        while (true) {
            SessionEvent e = s.nextEvent();
            if (kind.isInstance(e) && match.test(kind.cast(e))) return kind.cast(e);
        }
    }

    @SuppressWarnings("try") // closes a resource of the try early on purpose; closing it again is a no-op
    @Test void anIdlePipeIntoABlackHoleEventuallyFails() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops, multipath(4));
            try (var listener = Zmux.listen(NetworkTest.any(), ListenerConfig.of(cfg)); var relay = new TcpRelay(listener.localAddress());
                 var client = Zmux.connect(listener.localAddress(), cfg); var server = listener.accept(Duration.ofSeconds(5))) {
                var pipe = client.addPipe(relay.address(), PipeOptions.builder().liveness(PipeLiveness.builder().probeInterval(Duration.ofSeconds(1)).build()).build());
                pipe.awaitAttached();
                relay.blackhole = true;
                event(client, SessionEvent.PipeFailed.class, e -> e.pipe().equals(pipe.id()));
                client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
            }
            Leaks.none(loops);
        }
    }



}
