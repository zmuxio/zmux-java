package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.zmux.internal.join.Framing;
import io.zmux.internal.join.Hello;
import io.zmux.internal.join.HelloPayload;
import io.zmux.internal.join.JoinPipe;
import io.zmux.internal.join.Joiner;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.Suite;
import org.junit.jupiter.api.Test;

/** A join's check of its Suite: a Suite outside the session's set is refused as unknown_suite, before any AEAD. */
class JoinRefusalReasonTest {
    @Test void aSuiteOutsideTheSessionsSetIsUnknownSuite() throws Exception {
        Session s = PipeTargetTest.session(); // no join key for any Suite
        byte[] w = Joiner.create(Suite.CHACHA, Mode.SEALED, Framing.STREAM, new byte[8], new byte[32], new byte[0])
                .writeHello(new HelloPayload(new JoinPipe(1, 2, 0, 0, false), null, 0));
        Hello h = Hello.parseExact(w, 0, w.length, Framing.STREAM);
        Joins.Opened o = s.openHello(h, Framing.STREAM, new byte[0]);
        assertEquals(Joins.JoinResult.CLOSE, o.result());
        assertEquals("unknown_suite", o.reason());
        assertEquals(0, s.stats().joinAuthFailures(), "not an authentication failure");
    }
}
