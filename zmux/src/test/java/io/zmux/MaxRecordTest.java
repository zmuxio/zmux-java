package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.carrier.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** {@link PipeIo#withMaxRecord} caps the records zmux writes on that pipe and the datagrams it sizes. */
@Timeout(45)
class MaxRecordTest {
    static final int CAP = PipeIo.MIN_RECORD;
    /** A Sealed record's seal on a message carrier: its tag. */
    static final int SEAL = 16;

    static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).maxPipes(4).pipeAuth(PipeAuth.psk(new byte[32]).withEndToEnd())
                .noProbation().enableDatagrams(true).build();
    }

    /** The pipe input with its writer recording the largest message (one record on a message carrier). */
    static PipeIo recorded(PipeIo io, AtomicInteger largest, AtomicInteger records) {
        PipeWriter w = io.writer();
        return io.withHalves(io.reader(), new PipeWriter() {
            @Override public void write(OutboundBatch batch, Completion done) {
                for (int m = 0, from = 0; m < batch.messageCount(); m++) {
                    int to = batch.messageEnd(m), n = 0;
                    for (int i = from; i < to; i++) n += batch.segment(i).remaining();
                    largest.accumulateAndGet(n, Math::max);
                    records.incrementAndGet();
                    from = to;
                }
                w.write(batch, done);
            }
            @Override public OptionalLong unsent() { return w.unsent(); }
            @Override public OptionalLong retransmitted() { return w.retransmitted(); }
            @Override public void close() { w.close(); }
            @Override public void abort(Throwable cause) { w.abort(cause); }
        });
    }

    /**
     * Large stream writes stay within the carrier's cap on the sender's records, and the largest datagram fits a
     * record of plain bytes; one byte more is refused.
     */
    static void capped(ZmuxSession sender, ZmuxSession receiver, AtomicInteger largest, AtomicInteger records, int plain, int cap) throws Exception {
        int max = sender.datagrams().maxPayload();
        assertTrue(max > 0 && max <= plain - 3, "max datagram payload " + max);
        assertTrue(sender.datagrams().send(ByteBuffer.allocate(max)));
        assertEquals(max, receiver.datagrams().receive(Duration.ofSeconds(5)).length);
        assertThrows(DatagramTooLargeException.class, () -> sender.datagrams().send(ByteBuffer.allocate(max + 1)));
        int before = records.get();
        byte[] data = new byte[50_000];
        try (var s = sender.openStream()) {
            s.write(data); s.finish();
            try (var t = receiver.acceptStream(Duration.ofSeconds(5))) {
                assertEquals(data.length, SessionTest.readAll(t).length);
            }
        }
        assertTrue(records.get() - before >= data.length / cap, "records " + (records.get() - before));
        assertTrue(largest.get() <= cap, "largest record " + largest.get());
    }

    @Test void rejectsOutOfRange() {
        var io = MemoryPipe.pair(Framing.MESSAGES)[0];
        assertThrows(IllegalArgumentException.class, () -> io.withMaxRecord(-1));
        assertThrows(IllegalArgumentException.class, () -> io.withMaxRecord(1));
        assertThrows(IllegalArgumentException.class, () -> io.withMaxRecord(1000));
        assertThrows(IllegalArgumentException.class, () -> io.withMaxRecord(PipeIo.MIN_RECORD - 1));
        assertEquals(PipeIo.MIN_RECORD, io.withMaxRecord(PipeIo.MIN_RECORD).capabilities().maxRecord());
        // No upper bound: a large cap is reduced by the peer's and zmux's own limits.
        assertEquals(1 << 20, io.withMaxRecord(1 << 20).capabilities().maxRecord());
        assertEquals(0, io.capabilities().maxRecord());
    }

    @Test void openedPipeIsCapped() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            var largest = new AtomicInteger(); var records = new AtomicInteger();
            try (var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var pair = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("open")));
                try (var client = Zmux.open(recorded(pair[0].withMaxRecord(CAP), largest, records), Role.CLIENT, cfg);
                     var server = b.accept(Duration.ofSeconds(5))) {
                    capped(client, server, largest, records, CAP, CAP);
                }
            }
        }
    }

    @Test void listenerHandledPipeIsCapped() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            var largest = new AtomicInteger(); var records = new AtomicInteger();
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var pair = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(recorded(pair[1].withMaxRecord(CAP), largest, records), JoinHint.inBand(TestAddr.peer("accepted")));
                try (var client = a.open(pair[0], Role.CLIENT); var server = b.accept(Duration.ofSeconds(5))) {
                    capped(server, client, largest, records, CAP, CAP);
                }
            }
        }
    }

    @Test void addedPipeIsCapped() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            var largest = new AtomicInteger(); var records = new AtomicInteger();
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var bootstrap = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("bootstrap")));
                try (var client = a.open(bootstrap[0], Role.CLIENT); var server = b.accept(Duration.ofSeconds(5))) {
                    var pair = MemoryPipe.pair(Framing.MESSAGES);
                    b.adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("joined")));
                    var joined = client.addPipe(recorded(pair[0].withMaxRecord(CAP + SEAL), largest, records), PipeOptions.defaults()); // Sealed, as the PSK asks
                    assertEquals(PipeState.Outbound.ACTIVE, joined.state().orElseThrow().outbound());
                    // The bootstrap pipe goes, so the stream bytes travel on the capped pipe.
                    client.pipes().stream().filter(p -> !p.id().equals(joined.id())).findFirst().orElseThrow().abort();
                    capped(client, server, largest, records, CAP, CAP + SEAL);
                }
            }
        }
    }

    /** A Sealed join keeps both ends' carrier limits, less the seal: the sealed records the carriers see stay within them. */
    @Test void sealedJoinedPipeIsCapped() throws Exception {
        int carrier = CAP + SEAL;
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            var largest = new AtomicInteger(); var records = new AtomicInteger();
            var serverLargest = new AtomicInteger(); var serverRecords = new AtomicInteger();
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var bootstrap = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("bootstrap")));
                try (var client = a.open(bootstrap[0], Role.CLIENT); var server = b.accept(Duration.ofSeconds(5))) {
                    var pair = MemoryPipe.pair(Framing.MESSAGES);
                    b.adapter().handle(recorded(pair[1].withMaxRecord(carrier), serverLargest, serverRecords), JoinHint.inBand(TestAddr.peer("joined")));
                    var joined = client.addPipe(recorded(pair[0].withMaxRecord(carrier), largest, records),
                            PipeOptions.builder().recordMode(RecordMode.SEALED).build());
                    assertEquals(RecordMode.SEALED, joined.stats().orElseThrow().recordMode());
                    long until = System.nanoTime() + 5_000_000_000L;
                    while (server.pipes().size() < 2 && System.nanoTime() < until) Thread.sleep(10);
                    client.pipes().stream().filter(p -> !p.id().equals(joined.id())).findFirst().orElseThrow().abort();
                    // The sealed records within the carrier's limit hold CAP bytes of plaintext.
                    capped(client, server, largest, records, CAP, carrier);
                    capped(server, client, serverLargest, serverRecords, CAP, carrier);
                }
            }
        }
    }

    /** A Sealed join on a carrier whose limit leaves less than the floor once sealed is refused when the pipe is added. */
    @Test void sealedJoinBelowTheFloorIsRefused() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var bootstrap = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("bootstrap")));
                try (var client = a.open(bootstrap[0], Role.CLIENT); var server = b.accept(Duration.ofSeconds(5))) {
                    var pair = MemoryPipe.pair(Framing.MESSAGES);
                    b.adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("joined")));
                    assertThrows(IllegalArgumentException.class, () -> client.addPipe(pair[0].withMaxRecord(CAP),
                            PipeOptions.builder().recordMode(RecordMode.SEALED).build()));
                    assertEquals(1, client.pipes().size());
                    assertFalse(server.isClosed(), "only the pipe is refused");
                }
            }
        }
    }

    /**
     * Every pipe capped at the floor, the first one included: the session epoch exchange on a Sealed join completes,
     * with acknowledgements and data flowing on the capped pipes.
     */
    @Test void epochCompletesWithEveryPipeAtTheFloor() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            var largest = new AtomicInteger(); var records = new AtomicInteger();
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var bootstrap = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(bootstrap[1].withMaxRecord(CAP), JoinHint.inBand(TestAddr.peer("bootstrap")));
                try (var client = a.open(recorded(bootstrap[0].withMaxRecord(CAP), largest, records), Role.CLIENT);
                     var server = b.accept(Duration.ofSeconds(5))) {
                    var pair = MemoryPipe.pair(Framing.MESSAGES);
                    b.adapter().handle(pair[1].withMaxRecord(CAP + SEAL), JoinHint.inBand(TestAddr.peer("joined")));
                    client.addPipe(recorded(pair[0].withMaxRecord(CAP + SEAL), largest, records), PipeOptions.builder().recordMode(RecordMode.SEALED).build());
                    ListenerEntryTest.await(() -> {
                        try { return client.epoch() == 1 && server.epoch() == 1; } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
                    });
                    byte[] data = new byte[200_000];
                    try (var s = client.openStream()) {
                        s.write(data); s.finish();
                        try (var t = server.acceptStream(Duration.ofSeconds(5))) {
                            assertEquals(data.length, SessionTest.readAll(t).length);
                        }
                    }
                    assertTrue(largest.get() <= CAP + SEAL, "largest record " + largest.get());
                    assertFalse(client.isClosed() || server.isClosed());
                }
            }
        }
    }

    /**
     * A queued datagram that fits the pipe it was sized for but no pipe left is dropped and counted once that pipe
     * goes; the datagrams behind it are not held up.
     */
    @Test void aDatagramNoPipeHoldsIsDropped() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            var gate = new NonBlockingCallsTest.Gate();
            try (var a = Zmux.listener(ListenerConfig.of(cfg)); var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var bootstrap = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("bootstrap")));
                try (var client = a.open(gate.wrap(bootstrap[0]), Role.CLIENT); var server = b.accept(Duration.ofSeconds(5))) {
                    int big = client.datagrams().maxPayload();
                    assertTrue(big > CAP);
                    // The bootstrap pipe's write stays in progress: the next datagrams wait in the session's queue.
                    synchronized (gate) { gate.closed = true; }
                    assertTrue(client.datagrams().send(ByteBuffer.allocate(1)));
                    Thread.sleep(50);
                    assertTrue(client.datagrams().send(ByteBuffer.allocate(big)));
                    var pair = MemoryPipe.pair(Framing.MESSAGES);
                    b.adapter().handle(pair[1], JoinHint.inBand(TestAddr.peer("joined")));
                    var joined = client.addPipe(pair[0].withMaxRecord(CAP + SEAL), PipeOptions.defaults());
                    long dropped = client.stats().datagramsDroppedSend();
                    client.pipes().stream().filter(p -> !p.id().equals(joined.id())).findFirst().orElseThrow().abort();
                    assertTrue(client.datagrams().send(ByteBuffer.allocate(7)));
                    long until = System.nanoTime() + 5_000_000_000L;
                    byte[] got;
                    do got = server.datagrams().receive(Duration.ofSeconds(5)); while (got.length != 7 && System.nanoTime() < until);
                    assertEquals(7, got.length, "the datagram behind the dropped one arrives");
                    assertTrue(client.stats().datagramsDroppedSend() > dropped, "the large datagram is dropped and counted");
                }
            }
        }
    }

    /**
     * On pipes capped at the floor, every preamble the peer's Maximum Preamble Size allows (zmux takes at most 4096
     * of a larger one) fits the offset-0 STREAM frame and arrives; a larger one fails locally before anything is sent.
     */
    @Test void aPreambleFitsAFloorCappedPipeOrFailsLocally() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var cfg = config(loops);
            var largest = new AtomicInteger(); var records = new AtomicInteger();
            try (var b = Zmux.listener(ListenerConfig.of(cfg))) {
                var pair = MemoryPipe.pair(Framing.MESSAGES);
                b.adapter().handle(pair[1].withMaxRecord(CAP), JoinHint.inBand(TestAddr.peer("open")));
                try (var client = Zmux.open(recorded(pair[0].withMaxRecord(CAP), largest, records), Role.CLIENT, cfg);
                     var server = b.accept(Duration.ofSeconds(5))) {
                    var tooLarge = assertThrows(PreambleTooLargeException.class, () -> client.openStream(Preamble.builder().openInfo(new byte[5000]).build()));
                    assertEquals(4096, tooLarge.max(), "the peer's bound, within the record floor");
                    int info = 4000; // with its TLV header, near the 4096-byte bound
                    try (var s = client.openStream(Preamble.builder().openInfo(new byte[info]).build())) {
                        s.write(new byte[CAP * 2]); s.finish();
                        try (var t = server.acceptStream(Duration.ofSeconds(5))) {
                            assertEquals(0, t.id(), "the refused open took no stream");
                            assertEquals(info, t.preamble().openInfo().length);
                            assertEquals(CAP * 2, SessionTest.readAll(t).length);
                        }
                    }
                    assertTrue(largest.get() <= CAP, "largest record " + largest.get());
                }
            }
        }
    }
}
