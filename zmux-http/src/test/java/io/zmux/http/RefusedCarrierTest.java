package io.zmux.http;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.Admission;
import io.zmux.PipeOptions;
import io.zmux.Role;
import io.zmux.Zmux;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Without an admission, a carrier the listener does not take and on which nothing was written
 * goes to the host's handler with its body replayed.
 */
@Timeout(60)
class RefusedCarrierTest {
    static Fixture fixture(LinkedBlockingQueue<HostHandler.Seen> seen) throws Exception {
        return new Fixture(b -> b, l -> l, e -> e.streaming(), false, () -> new HostHandler(seen));
    }

    static String chunked(byte[] body) {
        return Integer.toHexString(body.length) + "\r\n" + new String(body) + "\r\n0\r\n\r\n";
    }

    static final byte[] GARBAGE = "GET / HTTP/1.1 is not a zmux record, and it goes on for a while longer".getBytes();

    @Test void aRefusedFirstRecordGoesToTheHostWithItsBody() throws Exception {
        var seen = new LinkedBlockingQueue<HostHandler.Seen>();
        try (var f = fixture(seen)) {
            String got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n", chunked(GARBAGE).getBytes());
            assertTrue(got.startsWith("HTTP/1.1 200") && got.endsWith("host"), "[" + got + "]");
            var s = seen.poll(2, TimeUnit.SECONDS);
            assertNotNull(s);
            assertEquals("POST", s.method());
            assertArrayEquals(GARBAGE, s.body());
            // Normal sessions are unaffected and never reach the host.
            for (boolean h1 : new boolean[] {true, false}) {
                try (var c = f.client(b -> h1 ? b.http1() : b.http2())) {
                    try (var z = Zmux.open(c.pipe(f.base, Map.of(), PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, f.config)) {
                        f.echo(z);
                    }
                    try (var z = Zmux.open(c.splitPipe(f.base, Map.of(), null), Role.CLIENT, f.config)) {
                        f.echo(z);
                    }
                }
            }
            assertNull(seen.poll(300, TimeUnit.MILLISECONDS), "a taken carrier reached the host");
        }
    }

    @Test void anUnknownJoinGoesToTheHost() throws Exception {
        var seen = new LinkedBlockingQueue<HostHandler.Seen>();
        try (var f = fixture(seen)) {
            for (String join : new String[] {"not-a-join", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}) {
                byte[] body = ("uplink bytes for " + join).getBytes();
                String got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nZmux-Pipe-Join: " + join
                        + "\r\nTransfer-Encoding: chunked\r\n\r\n", chunked(body).getBytes());
                assertTrue(got.endsWith("host"), got);
                var s = seen.poll(2, TimeUnit.SECONDS);
                assertNotNull(s, join);
                assertArrayEquals(body, s.body());
            }
            // A downlink with an unknown join, which has no body.
            String got = RawHttp.send(f.base, "GET /z HTTP/1.1\r\nHost: x\r\nZmux-Pipe-Join: not-a-join\r\n\r\n", null);
            assertTrue(got.endsWith("host"), got);
            assertEquals("GET", seen.poll(2, TimeUnit.SECONDS).method());
        }
    }

    @Test void aClosedListenerHandsCarriersToTheHost() throws Exception {
        var seen = new LinkedBlockingQueue<HostHandler.Seen>();
        try (var f = fixture(seen)) {
            f.listener.close();
            String got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n", chunked(GARBAGE).getBytes());
            assertTrue(got.endsWith("host"), got);
            assertArrayEquals(GARBAGE, seen.poll(2, TimeUnit.SECONDS).body());
        }
    }

    @Test void aCarrierWithAWrittenHeaderIsNotHandedOver() throws Exception {
        // With an admission the header goes out on Accept: a carrier the listener then refuses
        // ends without reaching the host; Close and Stall do not reach it either.
        for (var d : new Admission.Decision[] {Admission.Decision.accept(0), Admission.Decision.CLOSE, Admission.Decision.STALL}) {
            var seen = new LinkedBlockingQueue<HostHandler.Seen>();
            try (var f = new Fixture(b -> b, l -> l.admission(Admission.of(0, Admission.Decider.immediate((info, prefix) -> d))), e -> e.streaming(), false,
                    () -> new HostHandler(seen))) {
                String got = RawHttp.send(f.base, "POST /z HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n", chunked(GARBAGE).getBytes());
                assertFalse(got.endsWith("host"), d + ": " + got);
                assertEquals(d.verdict() == Admission.Verdict.ACCEPT, got.startsWith("HTTP/1.1 200"), d + ": " + got);
                assertNull(seen.poll(300, TimeUnit.MILLISECONDS), d + " reached the host");
            }
        }
    }
}
