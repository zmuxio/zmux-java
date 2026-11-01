package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class ReservationTest {
    static ZmuxConfig config(EventLoops loops) {
        return ZmuxConfig.builder().eventLoops(loops).multipath(MultipathConfig.defaults().withMaxPipes(4).withAuth(PipeAuth.psk(new byte[32])).withProbation(Duration.ZERO)).build();
    }
    @Test void reservedJoinsAttachRealCarriersOnceWithTheirActualFraming() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = config(loops);
            try (var listener = Zmux.listener(ListenerConfig.of(config))) {
                var bootstrap = MemoryPipe.pair(Framing.BYTE_STREAM);
                listener.handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("client"))).closed();
                try (var client = Zmux.open(bootstrap[0], Role.CLIENT, config);
                     var server = listener.accept(Duration.ofSeconds(5))) {
                    for (Framing framing : Framing.values()) {
                        try (var reservation = client.paths().reserve(PipeOptions.builder().probation(Duration.ZERO).build())) {
                            var before = client.paths().stats().stream().filter(p -> p.pipe() == reservation.id()).findFirst().orElseThrow();
                            assertEquals(0, before.recordsSent(), "reservation must not poll a dummy writer");
                            var pair = MemoryPipe.pair(framing);
                            var lifetime = listener.handle(pair[1], JoinHint.join(reservation.header(), TestAddr.peer("client"))).closed();
                            var pipe = reservation.attach(pair[0]);
                            assertEquals(reservation.id(), pipe.id()); assertFalse(lifetime.toCompletableFuture().isDone());
                            long acknowledged = pipe.stats().orElseThrow().bytesAcked().orElse(0);
                            assertThrows(IllegalStateException.class, () -> reservation.attach(pair[0]));
                            AtomicInteger returned = new AtomicInteger();
                            PipeWriter unused = new PipeWriter() {
                                @Override public void write(io.zmux.carrier.OutboundBatch bytes, Completion done) { fail("overlapping carrier wrote data"); }
                                @Override public void close() { returned.incrementAndGet(); }
                            };
                            assertThrows(IllegalStateException.class, () -> reservation.attach(PipeIo.of(pair[0].reader(), unused)));
                            assertEquals(1, returned.get(), "new half of an overlapping input must be returned");
                            PipeWriter throwing = new PipeWriter() {
                                @Override public void write(io.zmux.carrier.OutboundBatch bytes, Completion done) { fail("overlapping carrier wrote data"); }
                                @Override public void close() { throw new UnsupportedOperationException("adapter close failed"); }
                            };
                            assertThrows(IllegalStateException.class, () -> reservation.attach(PipeIo.of(pair[0].reader(), throwing)));
                            pipe.revalidate();
                            ListenerResourcesTest.until(() -> {
                                try { return pipe.stats().orElseThrow().bytesAcked().orElse(0) > acknowledged; }
                                catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
                            });
                            var duplicate = MemoryPipe.pair(framing);
                            assertThrows(IllegalStateException.class, () -> reservation.attach(duplicate[0]));
                            duplicate[1].reader().close(); duplicate[1].writer().close();
                            assertEquals(PipeState.Outbound.ACTIVE, pipe.state().orElseThrow().outbound(), "duplicate attach must not abort the first attachment");
                            pipe.abort(); lifetime.toCompletableFuture().get(5, TimeUnit.SECONDS);
                        }
                    }
                    client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
                }
            }
            ListenerResourcesTest.until(() -> loops.outstandingBuffers() == 0);
            assertEquals(0, loops.outstandingBuffers());
        }
    }
    @Test void headerOnlyJoinCanAttachReceiveOnlyAndSendOnlyPipes() throws Exception {
        try (EventLoops loops = EventLoops.create(2)) {
            var config = config(loops);
            try (var listener = Zmux.listener(ListenerConfig.of(config))) {
                var bootstrap = MemoryPipe.pair(Framing.BYTE_STREAM); listener.handle(bootstrap[1], JoinHint.inBand(TestAddr.peer("client"))).closed();
                try (var client = Zmux.open(bootstrap[0], Role.CLIENT, config); var server = listener.accept(Duration.ofSeconds(5))) {
                    for (var direction : new PipeOptions.Direction[] {PipeOptions.Direction.RECEIVE_ONLY, PipeOptions.Direction.SEND_ONLY}) {
                        try (var reservation = client.paths().reserve(PipeOptions.builder().direction(direction).probation(Duration.ZERO).build())) {
                            var pair = MemoryPipe.pair(Framing.BYTE_STREAM);
                            boolean receiving = direction == PipeOptions.Direction.RECEIVE_ONLY;
                            PipeIo local = PipeIo.of(receiving ? pair[0].reader() : null, receiving ? null : pair[0].writer());
                            PipeIo remote = PipeIo.of(receiving ? null : pair[1].reader(), receiving ? pair[1].writer() : null);
                            var lifetime = listener.handle(remote, JoinHint.join(reservation.header(), TestAddr.peer("client"))).closed();
                            var attached = reservation.attach(local);
                            assertEquals(receiving ? null : PipeState.Outbound.ACTIVE, attached.state().orElseThrow().outbound());
                            assertEquals(receiving ? PipeState.Inbound.OPEN : null, attached.state().orElseThrow().inbound());
                            attached.abort(); lifetime.toCompletableFuture().get(5, TimeUnit.SECONDS);
                        }
                    }
                    client.close(); server.close(); client.awaitClosed(); server.awaitClosed();
                }
            }
            ListenerResourcesTest.until(() -> loops.outstandingBuffers() == 0);
            assertEquals(0, loops.outstandingBuffers());
        }
    }
    @Test void malformedHeadersCloseHalvesWithoutStartingReaders() throws Exception {
        try (EventLoops loops = EventLoops.create(1); var listener = Zmux.listener(ListenerConfig.of(config(loops)))) {
            for (String header : new String[] {"=", "AA==", "A".repeat(87), "AA", "-_"}) {
                AtomicInteger closes = new AtomicInteger();
                PipeReader reader = new PipeReader() {
                    @Override public void start(Sink sink) { fail("invalid headers must not start a reader"); }
                    @Override public void close() { closes.incrementAndGet(); }
                };
                PipeWriter writer = new PipeWriter() {
                    @Override public void write(io.zmux.carrier.OutboundBatch data, Completion done) { fail("invalid header must not write"); }
                    @Override public void close() { closes.incrementAndGet(); }
                };
                listener.handle(PipeIo.of(reader, writer), JoinHint.join(header, TestAddr.peer("bad"))).closed()
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
                assertEquals(2, closes.get());
            }
        }
    }
}
