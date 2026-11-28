package io.zmux.internal.core;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/** The custom policy's questions, taken by no driver: the test takes them itself, when it chooses. */
class CustomPickTest {
    /** Builds one record on lane 0 of p, as its writer would. */
    static void write(Session s, Pipe p) {
        Batch b = new Batch(); RecordWriter w = new RecordWriter();
        try {
            w.init(b, Framing.BYTES, 65536, 65536);
            w.retainFor(p, p.out().lanes.getFirst().log.room());
            s.mp().write(w, p, p.out().lanes.getFirst()); w.end();
            s.tx.afterBatch(w, p, p.out().lanes.getFirst());
        } finally { b.release(); }
    }

    @Test void aDirectionAskedBeforeItsOpeningIsNamedByItsOwnIdentifier() throws Exception {
        Session s = CounterSemanticsTest.established();
        s.setPolicy(CoreConfig.Policy.CUSTOM);
        Stream first = s.streams.openStream(true);
        s.streams.pinId(first); // stream 0
        Stream st = s.streams.openStream(true);
        s.send.write(st, ByteBuffer.allocate(100), false);
        write(s, s.pipes.get(0));
        assertFalse(st.tab.idSet(), "asked before its first frame");
        var req = new PathScheduler.PickRequest();
        assertTrue(s.takePick(req));
        assertTrue(st.tab.idSet(), "pinned when the question is taken");
        assertNotEquals(first.tab.id(), st.tab.id());
        int i = 0;
        while (req.stream(i) != st) i++;
        assertEquals(st.tab.id(), req.frames.get(i).stream());
        for (var f : req.frames) if (f != req.frames.get(i)) assertNotEquals(st.tab.id(), f.stream());
    }

    @Test void aQuestionWithdrawnAfterTheFallbackCarriedTheDataIsAskedAgain() throws Exception {
        Session s = CounterSemanticsTest.established();
        s.setPolicy(CoreConfig.Policy.CUSTOM);
        Pipe p = s.pipes.get(0);
        Stream st = s.streams.openStream(true);
        s.streams.pinId(st);
        s.send.write(st, ByteBuffer.allocate(100), false);
        write(s, p);
        assertTrue(st.snd.mps().sched.pickAsked());
        // The policy has not answered within the wait: the fallback carries everything.
        s.setNow(Time.add(s.now(), Time.SECOND));
        write(s, p);
        assertFalse(st.sendable(), "the fallback carried the data");
        var req = new PathScheduler.PickRequest();
        assertFalse(s.takePick(req), "nothing waits for the question");
        assertFalse(st.snd.mps().sched.pickAsked(), "withdrawn, not left asked");
        // New data asks the policy again rather than going straight to the fallback.
        s.send.write(st, ByteBuffer.allocate(100), false);
        write(s, p);
        assertTrue(s.wantsPick());
        req = new PathScheduler.PickRequest();
        assertTrue(s.takePick(req));
        assertEquals(st.tab.id(), req.frames.getFirst().stream());
    }
}
