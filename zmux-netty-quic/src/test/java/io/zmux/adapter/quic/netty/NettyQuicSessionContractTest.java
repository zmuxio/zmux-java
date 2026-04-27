package io.zmux.adapter.quic.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.zmux.*;
import io.zmux.internal.TimeoutBudget;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static io.zmux.adapter.quic.netty.NettyQuicTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

class NettyQuicSessionContractTest {
    private static void assertMetadata(StreamMetadata metadata, long priority, Long group, String openInfo) {
        assertEquals(priority, metadata.priority());
        assertEquals(group, metadata.group());
        assertArrayEquals(openInfo.getBytes(StandardCharsets.UTF_8), metadata.openInfo());
    }

    private static void awaitTrackedState(NettyQuicStreamState state,
                                          int expectedOpenInfoBytes,
                                          int expectedBufferedBytes,
                                          Duration timeout) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (state.retainedOpenInfoBytes() >= expectedOpenInfoBytes
                    && state.bufferedInboundBytes() >= expectedBufferedBytes) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("timed out waiting for tracked adapter stream state");
    }

    private static SessionStats awaitStats(ZmuxSession session,
                                           Duration timeout,
                                           java.util.function.Predicate<SessionStats> predicate) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            SessionStats snapshot = session.stats();
            if (predicate.test(snapshot)) {
                return snapshot;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("timed out waiting for session stats predicate");
    }

    private static void stallEventLoopUntilReleased(CountDownLatch blocked, CountDownLatch release) {
        blocked.countDown();
        try {
            if (!release.await(5L, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting to release stalled Netty event loop");
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<NettyQuicBidiStream> bidiAcceptSnapshot(NettyQuicSession session) throws Exception {
        NettyQuicSupport.AcceptQueue<NettyQuicBidiStream> queue =
                (NettyQuicSupport.AcceptQueue<NettyQuicBidiStream>) getField(session, "bidiAcceptQueue");
        return queue.snapshot();
    }

    private static QuicStreamChannel openRawBidiStream(NettyQuicTestSupport.SessionPair pair) throws IOException {
        return NettyQuicSupport.awaitFuture(
                pair.rawClient.createStream(QuicStreamType.BIDIRECTIONAL, quietHandler())
        );
    }

    private static QuicConnectionCloseEvent newCloseEvent(boolean applicationClose, int error, byte[] reason) throws Exception {
        Constructor<QuicConnectionCloseEvent> constructor = QuicConnectionCloseEvent.class.getDeclaredConstructor(
                boolean.class,
                int.class,
                byte[].class
        );
        constructor.setAccessible(true);
        return constructor.newInstance(applicationClose, error, reason);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        Object current = field.get(target);
        if (current instanceof java.util.concurrent.atomic.AtomicBoolean) {
            ((java.util.concurrent.atomic.AtomicBoolean) current).set((Boolean) value);
            return;
        }
        field.set(target, value);
    }

    private static Object invokePrivate(Object target, String name, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = target.getClass().getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    @SuppressWarnings("unchecked")
    private static void injectDuplicateTrackedState(NettyQuicSession session, NettyQuicStreamState state) throws Exception {
        Set<NettyQuicStreamState> preparingStreams =
                (Set<NettyQuicStreamState>) getField(session, "preparingStreams");
        ArrayDeque<NettyQuicStreamState> pendingPrepare =
                (ArrayDeque<NettyQuicStreamState>) getField(session, "pendingPrepare");
        ReentrantLock prepareLock = (ReentrantLock) getField(session, "prepareLock");

        preparingStreams.add(state);
        prepareLock.lock();
        try {
            pendingPrepare.addLast(state);
        } finally {
            prepareLock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private static void removeInjectedDuplicateTrackedState(NettyQuicSession session, NettyQuicStreamState state) throws Exception {
        Set<NettyQuicStreamState> preparingStreams =
                (Set<NettyQuicStreamState>) getField(session, "preparingStreams");
        ArrayDeque<NettyQuicStreamState> pendingPrepare =
                (ArrayDeque<NettyQuicStreamState>) getField(session, "pendingPrepare");
        ReentrantLock prepareLock = (ReentrantLock) getField(session, "prepareLock");

        preparingStreams.remove(state);
        prepareLock.lock();
        try {
            pendingPrepare.removeFirstOccurrence(state);
        } finally {
            prepareLock.unlock();
        }
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void writeRaw(QuicStreamChannel stream, byte[] bytes) throws IOException {
        NettyQuicSupport.awaitChannelFuture(stream.writeAndFlush(Unpooled.wrappedBuffer(bytes)));
    }

    @Test
    void wrapSessionNullReturnsClosedSafeSession() throws Exception {
        ZmuxSession session = NettyQuic.wrapSession(null);
        assertNotNull(session);
        assertTrue(session.isClosed());
        assertEquals(SessionState.INVALID, session.state());
        assertEquals(SessionState.INVALID, session.stats().state());
        assertTrue(session.awaitTermination(Duration.ofMillis(1)));
        SessionClosedException closed = assertThrows(SessionClosedException.class, session::openStream);
        assertEquals(ZmuxErrorSource.LOCAL, closed.source());
        session.close();
    }

    @Test
    void adapterStreamsExposeNativeStateQueries() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream bidi = pair.client.openStream();
            assertTrue(bidi instanceof ZmuxNativeStream, "adapter bidi stream should expose native state queries");
            ZmuxNativeStream nativeBidi = (ZmuxNativeStream) bidi;
            assertTrue(nativeBidi.openedLocally());
            assertTrue(nativeBidi.bidirectional());
            assertFalse(nativeBidi.readClosed());
            assertFalse(nativeBidi.writeClosed());

            ZmuxSendStream outboundUni = pair.client.openUniAndSend(utf8("x"));
            assertTrue(outboundUni instanceof ZmuxNativeSendStream, "adapter send-only stream should expose native send state");
            assertFalse(outboundUni instanceof ZmuxNativeRecvStream, "adapter send-only stream must stay send-only");
            ZmuxNativeSendStream nativeSend = (ZmuxNativeSendStream) outboundUni;
            assertTrue(nativeSend.openedLocally());
            assertFalse(nativeSend.bidirectional());
            assertTrue(nativeSend.writeClosed(), "openUniAndSend should leave the adapter send side closed");

            ZmuxRecvStream acceptedUni = pair.server.acceptUniStream(Duration.ofSeconds(5));
            assertTrue(acceptedUni instanceof ZmuxNativeRecvStream, "accepted adapter recv stream should expose native recv state");
            assertFalse(acceptedUni instanceof ZmuxNativeSendStream, "accepted adapter recv stream must stay recv-only");
            ZmuxNativeRecvStream nativeRecv = (ZmuxNativeRecvStream) acceptedUni;
            assertFalse(nativeRecv.openedLocally());
            assertFalse(nativeRecv.bidirectional());
            assertEquals(1, acceptedUni.read(new byte[1]));
            assertEquals(-1, acceptedUni.read(new byte[1]));
            assertTrue(nativeRecv.readClosed(), "adapter recv stream should report closed after peer finish");

            acceptedUni.close();
            outboundUni.close();
            bidi.close();
        }
    }

    @Test
    void bidiOpenAcceptRoundTripsPayload() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();
            byte[] payload = utf8("adapter-contract-bidi");
            clientStream.write(payload);
            clientStream.closeWrite();

            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(payload, readAll(accepted));
            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void stableStreamHelpersWorkThroughAdapterStreams() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();

            ByteBuffer directSource = ByteBuffer.allocateDirect(2);
            directSource.put((byte) 'b').put((byte) 'c').flip();
            assertEquals(2, clientStream.write(directSource));
            assertFalse(directSource.hasRemaining());

            OutputStream output = clientStream.asOutputStream();
            output.write('d');
            output.close();

            ZmuxStream accepted = await(acceptedFuture);
            ByteBuffer directTarget = ByteBuffer.allocateDirect(2);
            while (directTarget.hasRemaining()) {
                int read = accepted.read(directTarget);
                assertTrue(read > 0, "adapter ByteBuffer read should make progress before EOF");
            }
            directTarget.flip();
            assertEquals((byte) 'b', directTarget.get());
            assertEquals((byte) 'c', directTarget.get());

            InputStream input = accepted.asInputStream();
            assertEquals('d', input.read());
            assertEquals(-1, input.read());
            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void uniOpenAcceptRoundTripsPayload() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxRecvStream> acceptedFuture = async(() -> pair.server.acceptUniStream(Duration.ofSeconds(5)));
            ZmuxSendStream send = pair.client.openUniStream();
            byte[] payload = utf8("adapter-contract-uni");
            send.write(payload);

            ZmuxRecvStream accepted = await(acceptedFuture);
            send.closeWrite();
            assertArrayEquals(payload, readAll(accepted));
            accepted.close();
            send.close();
        }
    }

    @Test
    void openUniAndSendUsesWriteFinalSemantics() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxRecvStream> firstAcceptedFuture = async(() -> pair.server.acceptUniStream(Duration.ofSeconds(5)));
            ZmuxSendStream first = pair.client.openUniAndSend(utf8("adapter-contract-openUniAndSend"));
            ZmuxRecvStream firstAccepted = await(firstAcceptedFuture);
            assertArrayEquals(utf8("adapter-contract-openUniAndSend"), readAll(firstAccepted));
            firstAccepted.close();
            first.close();

            CompletableFuture<ZmuxRecvStream> secondAcceptedFuture = async(() -> pair.server.acceptUniStream(Duration.ofSeconds(5)));
            ZmuxSendStream second = pair.client.openUniAndSend(new byte[0]);
            ZmuxRecvStream secondAccepted = await(secondAcceptedFuture);
            assertEquals(-1, secondAccepted.read(new byte[1]));
            secondAccepted.close();
            second.close();
        }
    }

    @Test
    void writevFinalOnAdapterCombinesMultipartPayloadAndClosesStream() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxRecvStream> acceptedFuture = async(() -> pair.server.acceptUniStream(Duration.ofSeconds(5)));
            ZmuxSendStream send = pair.client.openUniStream(new OpenOptions(5L, 0L, utf8("uni")));

            int written = send.writevFinal(utf8("hel"), utf8("lo"));

            ZmuxRecvStream accepted = await(acceptedFuture);
            assertEquals(5, written);
            assertMetadata(accepted.metadata(), 5L, 0L, "uni");
            assertArrayEquals(utf8("uni"), accepted.openInfo());
            assertArrayEquals(utf8("hello"), readAll(accepted));
            assertNotNull(
                    pair.client.stats().progress().controlProgressAt(),
                    "combined writevFinal opener should count as adapter control progress"
            );
            accepted.close();
            send.close();
        }
    }

    @Test
    void zeroLengthWriteDoesNotSubmitAdapterPrelude() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream clientStream = pair.client.openStream();

            clientStream.write(new byte[0]);

            assertThrows(
                    AcceptTimeoutException.class,
                    () -> pair.server.acceptStream(Duration.ofMillis(150)),
                    "ordinary zero-length write must not make the stream peer-visible"
            );

            clientStream.write(utf8("x"));
            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
            assertArrayEquals(utf8("x"), readExactly(accepted, 1));

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void statsExposeAcceptBacklogAndBufferedPressure() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            byte[] openInfo = utf8("meta");
            byte[] payload = utf8("buffered");
            ZmuxStream stream = pair.client.openStream(new OpenOptions(7L, null, openInfo));
            stream.write(payload);

            SessionStats stats = awaitStats(pair.server, Duration.ofSeconds(5), snapshot ->
                    snapshot.acceptBacklog().count() == 1L
                            && snapshot.acceptBacklog().bytes() >= payload.length
                            && snapshot.retainedOpenInfoBytes() >= openInfo.length
                            && snapshot.pressure().trackedSessionMemoryBytes() >= payload.length
            );

            assertEquals(SessionState.READY, stats.state());
            assertTrue(stats.openStreams() >= 1L, "accepted-but-not-yet-consumed stream should count as open");
            assertEquals(1L, stats.acceptBacklog().count(), "accept backlog count mismatch");
            assertTrue(stats.acceptBacklog().bytes() >= payload.length, "accept backlog bytes should reflect buffered data");
            assertTrue(stats.retainedOpenInfoBytes() >= openInfo.length, "retained open_info should surface in adapter stats");
            assertEquals(0, stats.queues().urgentFrames(), "adapter does not expose core urgent-queue frame counts");
            assertEquals(0, stats.queues().advisoryStreams(), "adapter does not expose core advisory queue counts");
            assertEquals(0, stats.queues().dataFrames(), "adapter does not expose core data-frame queue counts");
            assertTrue(stats.queues().queuedDataBytes() >= payload.length, "adapter queued-data bytes should include buffered inbound payload");
            assertTrue(
                    stats.pressure().bufferedReceiveBytes() >= payload.length,
                    "buffered receive bytes should include unread accepted payload"
            );
            assertTrue(
                    stats.pressure().bufferedReceiveStorageBytes() >= stats.pressure().bufferedReceiveBytes(),
                    "buffered receive storage bytes should not undercount pinned inbound payload"
            );
            assertTrue(
                    stats.pressure().recvSessionReceivedBytes() >= payload.length,
                    "adapter pressure should surface total received app bytes including pending unread payload"
            );
            assertTrue(
                    stats.pressure().recvSessionPendingBytes() >= payload.length,
                    "adapter pressure should surface unread pending receive bytes"
            );
            assertTrue(
                    stats.pressure().trackedRetainedStateMemoryBytes() >= openInfo.length,
                    "tracked retained-state bytes should include retained open_info/prelude state"
            );
            assertEquals(
                    1L,
                    stats.pressure().retainedStateBreakdown().acceptBacklog().count(),
                    "accept backlog retained bucket should count queued accepted streams"
            );
            assertTrue(
                    stats.pressure().retainedStateBreakdown().acceptBacklog().bytes() >= openInfo.length,
                    "accept backlog retained bucket should include retained open_info state"
            );
            assertEquals(
                    0L,
                    stats.pressure().retainedStateBreakdown().hiddenControl().count(),
                    "visible accept backlog should not leak into hidden retained-state accounting"
            );
            assertEquals(0, stats.hiddenState().retained(), "ready accept backlog should not count as hidden state");
            assertTrue(
                    stats.pressure().trackedSessionMemoryBytes() >= stats.pressure().bufferedReceiveBytes(),
                    "tracked memory should include buffered accepted payload"
            );

            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
            accepted.close();
            stream.close();
        }
    }

    @Test
    void statsSaturateReceivedPlusBufferedBytes() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream stream = pair.client.openStream();
            stream.write(utf8("xx"));

            NettyQuicSession server = (NettyQuicSession) pair.server;
            awaitStats(
                    server,
                    Duration.ofSeconds(5),
                    snapshot -> snapshot.pressure().bufferedReceiveBytes() >= 2L
            );
            AtomicLong receivedDataBytes = (AtomicLong) getField(server, "receivedDataBytes");
            receivedDataBytes.set(Long.MAX_VALUE - 1L);

            SessionStats stats = server.stats();

            assertEquals(
                    Long.MAX_VALUE,
                    stats.pressure().recvSessionReceivedBytes(),
                    "received bytes plus unread buffered bytes must saturate instead of overflowing"
            );
            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
            accepted.close();
            stream.close();
        }
    }

    @Test
    void reducedStatsSurfaceKeepsUnsupportedFieldsExplicitlyEmptyDuringNormalTraffic() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream clientStream = pair.client.openStream(new OpenOptions(5L, 0L, utf8("q")));
            clientStream.write(utf8("x"));
            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
            assertArrayEquals(utf8("x"), readExactly(accepted, 1));

            SessionStats client = pair.client.stats();
            SessionStats server = pair.server.stats();

            assertEquals(0, client.queues().urgentFrames(), "adapter stats should not emulate core urgent queue item counts");
            assertEquals(0, client.queues().advisoryStreams(), "adapter stats should not emulate core advisory queue item counts");
            assertEquals(0, client.queues().dataFrames(), "adapter stats should not emulate core data queue item counts");
            assertEquals(0L, client.queues().reservedSendBytes(), "adapter stats should not emulate core reserved send bytes");
            assertEquals(0L, client.queues().urgentQueuedControlBytes(), "adapter stats should keep unsupported urgent control bytes empty");
            assertEquals(0L, client.queues().ordinaryQueuedControlBytes(), "adapter stats should keep unsupported ordinary control bytes empty");
            assertEquals(0L, client.queues().pendingControlBytes(), "adapter stats should keep unsupported pending control bytes empty");
            assertEquals(0L, client.queues().pendingPriorityBytes(), "adapter stats should keep unsupported pending priority bytes empty");
            assertEquals(0L, client.queues().writerHeldRetainedBytes(), "adapter stats should keep unsupported writer-held retained bytes empty");

            assertEquals(0, client.provisionals().bidi(), "adapter stats should not expose synthetic provisional-open counts");
            assertEquals(0, client.provisionals().uni(), "adapter stats should not expose synthetic provisional-open counts");
            assertEquals(0, client.provisionals().softCap(), "adapter stats should keep unsupported provisional limits empty");
            assertEquals(0, client.provisionals().hardCap(), "adapter stats should keep unsupported provisional limits empty");
            assertEquals(0L, client.provisionals().maxAgeNanos(), "adapter stats should keep unsupported provisional max-age empty");
            assertFalse(client.provisionals().bidiAtSoftCap(), "adapter stats should not fabricate provisional cap signals");
            assertFalse(client.provisionals().uniAtHardCap(), "adapter stats should not fabricate provisional cap signals");
            assertEquals(0L, client.provisionals().limited(), "adapter stats should keep unsupported provisional-limit diagnostics empty");
            assertEquals(0L, client.provisionals().expired(), "adapter stats should keep unsupported provisional-expiry diagnostics empty");

            assertEquals(0L, client.acceptBacklog().bytesLimit(), "adapter stats should keep unsupported accept-backlog byte cap empty");
            assertFalse(client.acceptBacklog().atBytesCap(), "adapter stats should not fabricate accept-backlog byte-cap signals");
            assertEquals(0L, client.acceptBacklog().refused(), "adapter stats should not reuse accept backlog for hidden refusal counts");

            assertEquals(0, server.hiddenState().softCap(), "adapter stats should keep unsupported hidden-state caps empty");
            assertEquals(0, server.hiddenState().hardCap(), "adapter stats should keep unsupported hidden-state caps empty");
            assertFalse(server.hiddenState().atSoftCap(), "adapter stats should not fabricate hidden-state cap signals");
            assertFalse(server.hiddenState().atHardCap(), "adapter stats should not fabricate hidden-state cap signals");
            assertEquals(0, server.hiddenState().visibleTombstones(), "adapter stats should keep unsupported tombstone counts empty");
            assertEquals(0, server.hiddenState().markerOnly(), "adapter stats should keep unsupported marker-only counts empty");

            assertEquals(0L, client.keepalive().intervalNanos(), "adapter stats should not fabricate app-level keepalive interval");
            assertEquals(0L, client.keepalive().maxPingIntervalNanos(), "adapter stats should not fabricate app-level max ping interval");
            assertEquals(0L, client.keepalive().lastPingRttNanos(), "adapter stats should not fabricate app-level ping RTT");
            assertFalse(client.keepalive().pingOutstanding(), "adapter stats should not fabricate app-level outstanding ping state");
            assertFalse(client.keepalive().pingStalled(), "adapter stats should not fabricate app-level stalled ping state");
            assertNull(client.progress().pingSentAt(), "adapter stats should keep unsupported ping progress timestamps empty");
            assertNull(client.progress().pongAt(), "adapter stats should keep unsupported pong progress timestamps empty");

            assertEquals(0L, client.diagnostics().droppedPriorityUpdates(), "adapter stats should keep unsupported dropped priority diagnostics empty");
            assertEquals(0L, client.diagnostics().droppedLocalPriorityUpdates(), "adapter stats should keep unsupported dropped local priority diagnostics empty");
            assertEquals(0L, client.diagnostics().visibleTerminalChurnEvents(), "adapter stats should keep unsupported terminal churn diagnostics empty");
            assertEquals(0L, client.diagnostics().groupRebucketEvents(), "adapter stats should keep unsupported group rebucket diagnostics empty");
            assertEquals(0L, client.diagnostics().protocolBacklogBlocked(), "adapter stats should keep unsupported protocol backlog diagnostics empty");
            assertEquals(0L, client.diagnostics().skippedCloseOnDeadIO(), "adapter stats should keep unsupported dead-IO diagnostics empty");
            assertEquals(0L, client.diagnostics().closeFrameFlushErrors(), "adapter stats should keep unsupported close-flush diagnostics empty");
            assertEquals(0L, client.diagnostics().closeCompletionTimeouts(), "adapter stats should keep unsupported close-completion diagnostics empty");
            assertEquals(0L, client.diagnostics().gracefulCloseTimeouts(), "adapter stats should keep unsupported graceful-close timeout diagnostics empty");

            assertEquals(0L, client.pressure().sessionMemoryHighThresholdBytes(), "adapter stats should keep unsupported memory thresholds empty");
            assertEquals(0L, client.pressure().sessionMemoryHardCapBytes(), "adapter stats should keep unsupported memory hard cap empty");
            assertFalse(client.pressure().memoryPressureHigh(), "adapter stats should not fabricate memory-pressure high signals");
            assertEquals(0L, client.pressure().recvSessionAdvertisedBytes(), "adapter stats should keep unsupported receive-advertised window empty");
            assertEquals(0L, client.pressure().outstandingPingBytes(), "adapter stats should keep unsupported outstanding ping bytes empty");
            assertEquals(0L, client.pressure().retainedStateBreakdown().provisionals().count(), "adapter stats should keep unsupported provisional retained bucket empty");
            assertEquals(0L, client.pressure().retainedStateBreakdown().visibleTombstones().count(), "adapter stats should keep unsupported tombstone retained bucket empty");
            assertEquals(0L, client.pressure().retainedStateBreakdown().markerOnly().count(), "adapter stats should keep unsupported marker-only retained bucket empty");

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void statsDoNotDoubleCountTrackedStateAcrossPreparationBuckets() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            byte[] openInfo = utf8("dup-open-info");
            byte[] payload = utf8("dup-buffered");
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));

            ZmuxStream clientStream = pair.client.openStream(new OpenOptions(9L, 0L, openInfo));
            clientStream.write(payload);

            NettyQuicBidiStream accepted = (NettyQuicBidiStream) await(acceptedFuture);
            awaitTrackedState(accepted.state, openInfo.length, payload.length, Duration.ofSeconds(5));

            NettyQuicSession server = (NettyQuicSession) pair.server;
            injectDuplicateTrackedState(server, accepted.state);
            try {
                long expectedOpenInfoBytes = accepted.state.retainedOpenInfoBytes();
                long expectedBufferedBytes = accepted.state.bufferedInboundBytes();
                long expectedTrackedMemoryBytes = accepted.state.trackedMemoryBytes();

                SessionStats snapshot = server.stats();

                assertEquals(1L, snapshot.openStreams(), "same tracked stream must not be counted multiple times");
                assertEquals(expectedOpenInfoBytes, snapshot.retainedOpenInfoBytes(), "retained open_info must be counted once");
                assertEquals(expectedBufferedBytes, snapshot.queues().queuedDataBytes(), "queued inbound bytes must be counted once");
                assertEquals(expectedBufferedBytes, snapshot.pressure().bufferedReceiveBytes(), "buffered receive bytes must be counted once");
                assertEquals(
                        expectedBufferedBytes,
                        snapshot.pressure().bufferedReceiveStorageBytes(),
                        "buffered receive storage bytes must be counted once"
                );
                assertEquals(
                        expectedTrackedMemoryBytes,
                        snapshot.pressure().trackedSessionMemoryBytes(),
                        "tracked session memory must not double count the same stream"
                );
                assertEquals(
                        Math.max(0L, expectedTrackedMemoryBytes - expectedBufferedBytes),
                        snapshot.pressure().trackedRetainedStateMemoryBytes(),
                        "tracked retained-state memory must not double count the same stream"
                );
                assertEquals(1, snapshot.hiddenState().retained(), "duplicate preparation buckets should still count one hidden state");
                assertEquals(
                        1L,
                        snapshot.pressure().retainedStateBreakdown().hiddenControl().count(),
                        "hidden retained breakdown must deduplicate the same tracked state"
                );
                assertEquals(
                        Math.max(0L, expectedTrackedMemoryBytes - expectedBufferedBytes),
                        snapshot.pressure().retainedStateBreakdown().hiddenControl().bytes(),
                        "hidden retained breakdown bytes must stay aligned with the deduplicated tracked state"
                );
            } finally {
                removeInjectedDuplicateTrackedState(server, accepted.state);
            }

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void drainedInboundOverflowDropsRetainedDequeBacking() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicStreamState state = NettyQuicStreamState.localBidi(
                    (NettyQuicSession) pair.client,
                    OpenOptions.empty()
            );
            ByteBuf first = Unpooled.wrappedBuffer(utf8("a"));
            ByteBuf second = Unpooled.wrappedBuffer(utf8("b"));
            try {
                invokePrivate(
                        state,
                        "enqueueInboundLocked",
                        new Class<?>[]{ByteBuf.class, int.class},
                        first,
                        first.readableBytes()
                );
                invokePrivate(
                        state,
                        "enqueueInboundLocked",
                        new Class<?>[]{ByteBuf.class, int.class},
                        second,
                        second.readableBytes()
                );

                assertNotNull(getField(state, "inboundOverflow"), "second inbound buffer should allocate overflow storage");

                byte[] drained = new byte[2];
                int copied = (Integer) invokePrivate(
                        state,
                        "drainInboundLocked",
                        new Class<?>[]{byte[].class, int.class, int.class},
                        drained,
                        0,
                        drained.length
                );

                assertEquals(2, copied, "drain should consume both queued inbound buffers");
                assertArrayEquals(utf8("ab"), drained, "drained inbound payload mismatch");
                assertNull(getField(state, "inboundOverflow"), "draining the last overflow buffer should release deque backing");
                assertEquals(0, first.refCnt(), "head buffer should be released after drain");
                assertEquals(0, second.refCnt(), "overflow buffer should be released after drain");
            } finally {
                if (first.refCnt() > 0) {
                    first.release();
                }
                if (second.refCnt() > 0) {
                    second.release();
                }
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void acceptedPreludePendingQueueIsBounded() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession server = (NettyQuicSession) pair.server;
            Semaphore prepareSlots = (Semaphore) getField(server, "prepareSlots");
            int drainedPermits = prepareSlots.drainPermits();
            try {
                int capacity = (Integer) getField(server, "pendingPrepareCapacity");
                ArrayDeque<NettyQuicStreamState> pendingPrepare =
                        (ArrayDeque<NettyQuicStreamState>) getField(server, "pendingPrepare");
                ReentrantLock prepareLock = (ReentrantLock) getField(server, "prepareLock");

                prepareLock.lock();
                try {
                    for (int i = 0; i < capacity; i++) {
                        pendingPrepare.addLast(NettyQuicStreamState.localBidi(server, OpenOptions.empty()));
                    }
                } finally {
                    prepareLock.unlock();
                }

                long refusedBefore = server.stats().hiddenState().refused();
                long refusedReasonBefore = server.stats().reasons().abort()
                        .getOrDefault(ErrorCode.REFUSED_STREAM.code(), 0L);
                NettyQuicStreamState overflow = NettyQuicStreamState.localBidi(server, OpenOptions.empty());
                invokePrivate(server, "scheduleAccepted", new Class<?>[]{NettyQuicStreamState.class}, overflow);

                assertEquals(capacity, pendingPrepare.size(), "overflow accepted stream must not grow pending-prelude queue");
                assertEquals(
                        refusedBefore + 1L,
                        server.stats().hiddenState().refused(),
                        "overflow accepted stream should be hidden-refused"
                );
                assertEquals(
                        refusedReasonBefore + 1L,
                        server.stats().reasons().abort().get(ErrorCode.REFUSED_STREAM.code()),
                        "overflow accepted stream should be rejected with REFUSED_STREAM"
                );
            } finally {
                prepareSlots.release(drainedPermits);
            }
        }
    }

    @Test
    void closeStartDropsRetainedPendingPreludeQueueBacking() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession server = (NettyQuicSession) pair.server;
            int capacity = (Integer) getField(server, "pendingPrepareCapacity");
            @SuppressWarnings("unchecked")
            ArrayDeque<NettyQuicStreamState> firstPendingPrepare =
                    (ArrayDeque<NettyQuicStreamState>) getField(server, "pendingPrepare");
            ReentrantLock prepareLock = (ReentrantLock) getField(server, "prepareLock");

            prepareLock.lock();
            try {
                for (int i = 0; i < capacity; i++) {
                    firstPendingPrepare.addLast(NettyQuicStreamState.localBidi(server, OpenOptions.empty()));
                }
            } finally {
                prepareLock.unlock();
            }

            invokePrivate(
                    server,
                    "beginClosing",
                    new Class<?>[]{IOException.class},
                    new SessionClosedException(ZmuxErrorSource.LOCAL)
            );

            assertNotSame(firstPendingPrepare, getField(server, "pendingPrepare"),
                    "close-start cleanup should replace the retained pending-prelude queue backing");
            assertTrue(((ArrayDeque<?>) getField(server, "pendingPrepare")).isEmpty(),
                    "close-start cleanup should leave the replacement pending-prelude queue empty");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void closeStartReleasesActiveStreamRegistryReferences() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession server = (NettyQuicSession) pair.server;
            Set<NettyQuicStreamState> activeStreams =
                    (Set<NettyQuicStreamState>) getField(server, "activeStreams");
            NettyQuicStreamState active = NettyQuicStreamState.localBidi(server, OpenOptions.empty());
            IOException closeError = new SessionClosedException(ZmuxErrorSource.LOCAL);
            activeStreams.add(active);

            invokePrivate(server, "beginClosing", new Class<?>[]{IOException.class}, closeError);

            assertTrue(activeStreams.isEmpty(), "close-start should not retain active stream state references");
            assertSame(closeError, getField(active, "sessionError"),
                    "active stream state should still observe the session close");
        }
    }

    @Test
    void acceptedStreamScheduledAfterCloseIsHiddenReaped() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession server = (NettyQuicSession) pair.server;
            setField(server, "closing", true);
            long reapedBefore = server.stats().hiddenState().reaped();

            NettyQuicStreamState late = NettyQuicStreamState.localBidi(server, OpenOptions.empty());
            invokePrivate(server, "scheduleAccepted", new Class<?>[]{NettyQuicStreamState.class}, late);

            assertEquals(
                    reapedBefore + 1L,
                    server.stats().hiddenState().reaped(),
                    "accepted stream scheduled after close-start should be counted as hidden-reaped"
            );
            assertEquals(0L, late.bufferedInboundBytes(), "late accepted state should not retain unread inbound bytes");
        }
    }

    @Test
    void statsExposeProgressFlushLatencyAndByteTotalsForAdapterTraffic() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            byte[] payload = utf8("adapter-progress");
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));

            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(payload);
            clientStream.closeWrite();

            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(payload, readAll(accepted));

            SessionStats clientStats = awaitStats(pair.client, Duration.ofSeconds(5), snapshot ->
                    snapshot.sentDataBytes() >= payload.length
                            && snapshot.flush().count() >= 1L
                            && snapshot.flush().lastBytes() >= payload.length
                            && snapshot.progress().transportWriteAt() != null
                            && snapshot.progress().streamProgressAt() != null
                            && snapshot.progress().applicationProgressAt() != null
                            && snapshot.keepalive().sendRateEstimateBytesPerSecond() > 0L
                            && snapshot.lastOpenLatencyNanos() > 0L
            );
            assertTrue(clientStats.blockedWriteTotalNanos() >= 0L, "blocked write total should be surfaced for adapter writes");
            assertTrue(
                    clientStats.keepalive().sendRateEstimateBytesPerSecond() > 0L,
                    "adapter keepalive stats should surface an observed send-rate estimate once writes complete"
            );

            SessionStats serverStats = awaitStats(pair.server, Duration.ofSeconds(5), snapshot ->
                    snapshot.receivedDataBytes() >= payload.length
                            && snapshot.progress().inboundFrameAt() != null
                            && snapshot.progress().streamProgressAt() != null
                            && snapshot.progress().applicationProgressAt() != null
            );
            assertEquals(payload.length, serverStats.receivedDataBytes(), "adapter server should surface received app bytes");

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void firstOrdinaryWriteCoalescesOpenPreludeAndPayloadIntoOneFlush() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            byte[] payload = utf8("coalesced-first-write");
            long flushBefore = pair.client.stats().flush().count();
            int expectedFlushBytes = NettyQuicPrelude.encode(OpenOptions.empty()).length + payload.length;
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));

            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(payload);

            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(payload, readExactly(accepted, payload.length));
            SessionStats clientStats = awaitStats(pair.client, Duration.ofSeconds(5), snapshot ->
                    snapshot.flush().count() >= flushBefore + 1L
                            && snapshot.flush().lastBytes() == expectedFlushBytes
            );
            assertEquals(
                    flushBefore + 1L,
                    clientStats.flush().count(),
                    "first ordinary write should combine adapter prelude and payload into one transport flush"
            );

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void closeWriteAdvancesControlProgressAfterPreludeWasAlreadySent() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("x"));

            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(utf8("x"), readExactly(accepted, 1));

            SessionStats beforeCloseWrite = awaitStats(
                    pair.client,
                    Duration.ofSeconds(5),
                    snapshot -> snapshot.progress().controlProgressAt() != null
            );
            Instant firstControlProgress = beforeCloseWrite.progress().controlProgressAt();
            assertNotNull(firstControlProgress, "open prelude write should establish an initial control-progress timestamp");

            Thread.sleep(20L);
            clientStream.closeWrite();

            SessionStats afterCloseWrite = awaitStats(
                    pair.client,
                    Duration.ofSeconds(5),
                    snapshot -> snapshot.progress().controlProgressAt() != null
                            && snapshot.progress().controlProgressAt().isAfter(firstControlProgress)
            );
            assertTrue(
                    afterCloseWrite.progress().controlProgressAt().isAfter(firstControlProgress),
                    "write-side terminal control should advance control-progress after the initial open prelude"
            );

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void sessionCloseMarksControlProgressForLocalAndRemoteSides() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            assertNull(pair.client.stats().progress().controlProgressAt(), "fresh adapter session should start without control progress");
            assertNull(pair.server.stats().progress().controlProgressAt(), "fresh adapter session should start without control progress");

            pair.client.close();

            assertTrue(pair.client.awaitTermination(Duration.ofSeconds(5)));
            assertTrue(pair.server.awaitTermination(Duration.ofSeconds(5)));

            assertNotNull(
                    pair.client.stats().progress().controlProgressAt(),
                    "local session close should mark control progress"
            );
            assertNotNull(
                    pair.server.stats().progress().controlProgressAt(),
                    "remote close event should mark control progress on the peer adapter session"
            );
        }
    }

    @Test
    void statsExposeReducedTransportKeepaliveSurface() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            SessionStats snapshot = awaitStats(
                    pair.client,
                    Duration.ofSeconds(5),
                    stats -> stats.keepalive().timeoutNanos() > 0L
            );

            assertTrue(snapshot.keepalive().enabled(), "adapter keepalive surface should expose transport idle-timeout");
            assertTrue(snapshot.keepalive().timeoutNanos() > 0L, "transport idle-timeout should be surfaced");
            assertFalse(snapshot.keepalive().pingOutstanding(), "adapter does not synthesize ping state");
            assertFalse(snapshot.keepalive().pingStalled(), "fresh session should not look stalled");
            assertEquals(0L, snapshot.keepalive().intervalNanos(), "adapter does not expose app-level ping cadence");
            assertEquals(0L, snapshot.keepalive().maxPingIntervalNanos(), "adapter does not expose app-level ping cadence");
        }
    }

    @Test
    void openMetadataPreludeIsVisibleOnAcceptedStreams() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> bidiAcceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            OpenOptions bidiOptions = new OpenOptions(7L, 0L, utf8("ssh"));
            ZmuxStream bidi = pair.client.openStream(bidiOptions);
            ZmuxStream acceptedBidi = await(bidiAcceptedFuture);
            assertMetadata(acceptedBidi.metadata(), 7L, 0L, "ssh");
            assertArrayEquals(utf8("ssh"), acceptedBidi.openInfo());

            CompletableFuture<ZmuxRecvStream> uniAcceptedFuture = async(() -> pair.server.acceptUniStream(Duration.ofSeconds(5)));
            ZmuxSendStream send = pair.client.openUniStream(new OpenOptions(null, 0L, utf8("uni")));
            ZmuxRecvStream acceptedUni = await(uniAcceptedFuture);
            assertMetadata(acceptedUni.metadata(), 0L, 0L, "uni");
            assertArrayEquals(utf8("uni"), acceptedUni.openInfo());

            acceptedUni.close();
            send.close();
            acceptedBidi.close();
            bidi.close();
        }
    }

    @Test
    void acceptWithHugeTimeoutOnClosedAdapterSessionDoesNotOverflow() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            pair.server.close();

            assertThrows(
                    IOException.class,
                    () -> pair.server.acceptStream(Duration.ofSeconds(Long.MAX_VALUE))
            );
        }
    }

    @Test
    void metadataSnapshotIsReusedUntilPendingMetadataChanges() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream stream = pair.client.openStream();

            StreamMetadata initial = stream.metadata();
            assertSame(initial, stream.metadata(), "local adapter stream should reuse the same metadata snapshot while unchanged");

            stream.updateMetadata(new MetadataUpdate(5L, 0L));

            StreamMetadata updated = stream.metadata();
            assertNotSame(initial, updated, "metadata snapshot should refresh after local pending metadata changes");
            assertSame(updated, stream.metadata(), "updated metadata snapshot should be reused until the next change");
            assertMetadata(updated, 5L, 0L, "");

            stream.close();
        }
    }

    @Test
    void acceptedStreamMetadataSnapshotIsStableAndDefensive() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream stream = pair.client.openStream(new OpenOptions(7L, 0L, utf8("ssh")));

            ZmuxStream accepted = await(acceptedFuture);
            StreamMetadata first = accepted.metadata();
            StreamMetadata second = accepted.metadata();
            assertSame(first, second, "accepted adapter stream should reuse a stable metadata snapshot");

            byte[] exposed = first.openInfo();
            exposed[0] = 'x';
            assertArrayEquals(utf8("ssh"), accepted.openInfo(), "open_info accessor must remain defensive");
            assertArrayEquals(utf8("ssh"), accepted.metadata().openInfo(), "metadata snapshot must remain defensive");

            accepted.close();
            stream.close();
        }
    }

    @Test
    void readDeadlineTimesOutLocallyAndCanBeCleared() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("x"));

            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
            assertEquals(1, accepted.read(new byte[1]));

            accepted.setReadTimeout(Duration.ofMillis(50));
            ReadTimeoutException timeout = assertThrows(ReadTimeoutException.class, () -> accepted.read(new byte[1]));
            ZmuxErrorDetails details = ZmuxErrors.details(timeout);
            assertNotNull(details);
            assertEquals("read", details.operation());
            assertEquals(ZmuxErrorScope.STREAM, details.scope());
            assertTrue(details.timeout());

            accepted.clearReadDeadline();
            clientStream.writeFinal(utf8("y"));
            assertEquals(1, accepted.read(new byte[1]));
            assertEquals(-1, accepted.read(new byte[1]));

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void updateMetadataBeforeVisibilityUsesPrelude() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream stream = pair.client.openStream();
            stream.updateMetadata(new MetadataUpdate(5L, 0L));

            ZmuxStream accepted = await(acceptedFuture);
            assertMetadata(accepted.metadata(), 5L, 0L, "");
            accepted.close();
            stream.close();
        }
    }

    @Test
    void updateMetadataAfterVisibilityIsUnavailable() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream stream = pair.client.openStream();
            stream.write(utf8("x"));
            ZmuxStream accepted = await(acceptedFuture);

            PriorityUpdateUnavailableException localFailure = assertThrows(
                    PriorityUpdateUnavailableException.class,
                    () -> stream.updateMetadata(new MetadataUpdate(13L, null))
            );
            assertInstanceOf(AdapterUnsupportedException.class, localFailure);

            PriorityUpdateUnavailableException acceptedFailure = assertThrows(
                    PriorityUpdateUnavailableException.class,
                    () -> accepted.updateMetadata(new MetadataUpdate(14L, null))
            );
            assertInstanceOf(AdapterUnsupportedException.class, acceptedFailure);

            accepted.close();
            stream.close();
        }
    }

    @Test
    void localMetadataUpdateStaysCommittedOncePreludeIsSubmittedBeforeSessionCloseCompletes() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession client = (NettyQuicSession) pair.client;
            ZmuxStream stream = pair.client.openStream();

            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall client Netty event loop");

            try {
                AtomicReference<Throwable> updateError = new AtomicReference<>();
                Thread updater = new Thread(() -> {
                    try {
                        stream.updateMetadata(new MetadataUpdate(13L, null));
                    } catch (Throwable failure) {
                        updateError.set(failure);
                    }
                }, "adapter-metadata-close-race");
                updater.start();

                updater.join(Duration.ofSeconds(2).toMillis());
                assertFalse(updater.isAlive(), "metadata update should complete once the prelude is submitted to Netty");
                assertNull(updateError.get(), "submitted metadata update should not fail only because the Netty event loop is still flushing");
                assertEquals(13L, stream.metadata().priority(), "submitted metadata update must stay consistent with the queued prelude");

                QuicConnectionCloseEvent closeEvent = newCloseEvent(false, 29, utf8("metadata-close-race"));
                setField(client, "closeEvent", closeEvent);
                invokePrivate(
                        client,
                        "beginClosing",
                        new Class<?>[]{IOException.class},
                        NettyQuicSupport.connectionCloseError(closeEvent, null)
                );

                assertEquals(13L, stream.metadata().priority(), "session close after prelude submission must not roll metadata back");
            } finally {
                releaseEventLoop.countDown();
            }
        }
    }

    @Test
    void successfulLocalOpenPreludeSubmissionReleasesRetainedBuffer() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            byte[] openInfo = utf8("prelude-retention");
            NettyQuicBidiStream stream = (NettyQuicBidiStream) pair.client.openStream(
                    new OpenOptions(7L, 3L, openInfo)
            );

            assertTrue((Boolean) getField(stream.state, "preludeSent"), "open prelude should be submitted");
            assertNull(getField(stream.state, "prelude"), "submitted open prelude buffer should be released");
            assertArrayEquals(openInfo, stream.metadata().openInfo(),
                    "metadata snapshot must remain available after releasing prelude storage");

            stream.close();
        }
    }

    @Test
    void emptyMetadataUpdateFailsWithTypedLocalError() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream stream = pair.client.openStream();
            EmptyMetadataUpdateException error = assertThrows(
                    EmptyMetadataUpdateException.class,
                    () -> stream.updateMetadata(new MetadataUpdate(null, null))
            );
            assertEquals(EmptyMetadataUpdateException.MESSAGE, error.getMessage());
            stream.close();
        }
    }

    @Test
    void duplicateAcceptedPreludeMetadataIsDroppedWithoutHidingStream() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            QuicStreamChannel malformed = openRawBidiStream(pair);
            try {
                byte[] duplicateMetadata = new byte[]{
                        0x01, 0x01, 0x05,
                        0x01, 0x01, 0x06
                };
                byte[] unreadTail = utf8("tail");
                byte[] prelude = new byte[1 + duplicateMetadata.length + unreadTail.length];
                prelude[0] = (byte) duplicateMetadata.length;
                System.arraycopy(duplicateMetadata, 0, prelude, 1, duplicateMetadata.length);
                System.arraycopy(unreadTail, 0, prelude, 1 + duplicateMetadata.length, unreadTail.length);
                writeRaw(malformed, prelude);

                ZmuxStream duplicate = pair.server.acceptStream(Duration.ofSeconds(5));
                assertEquals(malformed.streamId(), duplicate.streamId());
                assertMetadata(duplicate.metadata(), 0L, null, "");
                assertArrayEquals(unreadTail, readExactly(duplicate, unreadTail.length));
                assertEquals(0L, pair.server.stats().hiddenState().refused());

                ZmuxStream good = pair.client.openStream(new OpenOptions(9L, null, utf8("good")));
                good.write(utf8("z"));

                ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
                assertEquals(good.streamId(), accepted.streamId());
                assertMetadata(accepted.metadata(), 9L, null, "good");
                assertArrayEquals(utf8("z"), readExactly(accepted, 1));

                duplicate.close();
                accepted.close();
                good.close();
            } finally {
                malformed.close().syncUninterruptibly();
            }
        }
    }

    @Test
    void sessionCloseReapsHiddenPreparingAcceptedStreams() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            QuicStreamChannel stalled = openRawBidiStream(pair);
            try {
                writeRaw(stalled, new byte[]{0x40});

                awaitStats(
                        pair.server,
                        Duration.ofSeconds(5),
                        snapshot -> snapshot.hiddenState().retained() >= 1
                );

                pair.server.close();

                SessionStats reapedStats = awaitStats(
                        pair.server,
                        Duration.ofSeconds(5),
                        snapshot -> snapshot.hiddenState().reaped() >= 1L
                );
                assertEquals(1L, reapedStats.hiddenState().reaped(), "session close should reap hidden preparing streams");
            } finally {
                stalled.close().syncUninterruptibly();
            }
        }
    }

    @Test
    void closeReadStopsPeerWritesAndClosesLocalReadSide() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("p"));
            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(utf8("p"), readExactly(accepted, 1));

            accepted.closeRead();
            ReadClosedException readClosed = assertThrows(ReadClosedException.class, () -> accepted.read(new byte[1]));
            assertEquals(ReadClosedException.MESSAGE, readClosed.getMessage());

            IOException writeFailure = waitForWriteFailure(clientStream, utf8("x"), Duration.ofSeconds(3));
            assertNotNull(writeFailure, "peer writer never observed STOP_SENDING");
            ZmuxErrorDetails details = ZmuxErrors.details(writeFailure);
            assertNotNull(details, () -> "unexpected write failure: " + writeFailure);
            assertEquals(ZmuxErrorSource.REMOTE, details.source());
            assertEquals(ZmuxErrorDirection.WRITE, details.direction());
            assertEquals(ZmuxTerminationKind.STOPPED, details.terminationKind());
            if (details instanceof ApplicationError) {
                assertEquals(ErrorCode.CANCELLED.code(), ((ApplicationError) details).code());
            }

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void closeReadOnFreshLocalBidiSubmitsPreludeBeforeStopSending() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();

            clientStream.closeRead();

            ZmuxStream accepted = await(acceptedFuture);
            assertEquals(clientStream.streamId(), accepted.streamId());
            assertArrayEquals(new byte[0], accepted.openInfo());

            IOException writeFailure = waitForWriteFailure(accepted, utf8("x"), Duration.ofSeconds(3));
            assertNotNull(writeFailure, "peer writer never observed STOP_SENDING from fresh local closeRead");
            ZmuxErrorDetails details = ZmuxErrors.details(writeFailure);
            assertNotNull(details, () -> "unexpected write failure: " + writeFailure);
            assertEquals(ZmuxErrorSource.REMOTE, details.source());
            assertEquals(ZmuxErrorDirection.WRITE, details.direction());
            assertEquals(ZmuxTerminationKind.STOPPED, details.terminationKind());
            if (details instanceof ApplicationError) {
                assertEquals(ErrorCode.CANCELLED.code(), ((ApplicationError) details).code());
            }

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void remoteStreamResetSurfacesReadResetAndUpdatesReasonStats() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();

            clientStream.cancelWrite(41L);
            clientStream.write(new byte[0]);
            ZmuxStream accepted = await(acceptedFuture);

            ApplicationError reset = assertInstanceOf(
                    ApplicationError.class,
                    assertThrows(IOException.class, () -> accepted.read(new byte[1]))
            );
            assertEquals(41L, reset.code());
            ZmuxErrorDetails details = ZmuxErrors.details(reset);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.REMOTE, details.source());
            assertEquals(ZmuxErrorDirection.READ, details.direction());
            assertEquals(ZmuxTerminationKind.RESET, details.terminationKind());

            SessionStats clientStats = awaitStats(
                    pair.client,
                    Duration.ofSeconds(5),
                    snapshot -> Long.valueOf(1L).equals(snapshot.reasons().reset().get(41L))
            );
            SessionStats serverStats = awaitStats(
                    pair.server,
                    Duration.ofSeconds(5),
                    snapshot -> Long.valueOf(1L).equals(snapshot.reasons().reset().get(41L))
            );

            assertEquals(Long.valueOf(1L), clientStats.reasons().reset().get(41L), "local reset reason count mismatch");
            assertEquals(Long.valueOf(1L), serverStats.reasons().reset().get(41L), "remote observed reset reason count mismatch");

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void cancelWriteAfterCloseWriteStaysGracefullyClosedLocally() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();

            clientStream.closeWrite();
            clientStream.write(new byte[0]);

            WriteClosedException closed = assertInstanceOf(
                    WriteClosedException.class,
                    assertThrows(IOException.class, () -> clientStream.cancelWrite(91L)),
                    "cancelWrite must not override an already committed graceful close"
            );
            ZmuxErrorDetails details = ZmuxErrors.details(closed);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.LOCAL, details.source());
            assertEquals(ZmuxErrorDirection.WRITE, details.direction());
            assertEquals(ZmuxTerminationKind.GRACEFUL, details.terminationKind());

            WriteClosedException invalidCodeClosed = assertInstanceOf(
                    WriteClosedException.class,
                    assertThrows(IOException.class, () -> clientStream.cancelWrite(0x1_0000_0000L)),
                    "already committed graceful close must win over adapter code capability checks"
            );
            assertEquals(ZmuxErrorSource.LOCAL, invalidCodeClosed.source());
            assertEquals(ZmuxTerminationKind.GRACEFUL, invalidCodeClosed.terminationKind());

            ZmuxStream accepted = await(acceptedFuture);
            assertEquals(-1, accepted.read(new byte[1]), "remote peer should still observe the graceful EOF");
            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void sendStreamCloseWithErrorAfterCloseWritePrefersWriteClosedOverTooWideCode() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxRecvStream> acceptedFuture = async(() -> pair.server.acceptUniStream(Duration.ofSeconds(5)));
            ZmuxSendStream send = pair.client.openUniStream();

            send.closeWrite();

            WriteClosedException cancelClosed = assertInstanceOf(
                    WriteClosedException.class,
                    assertThrows(IOException.class, () -> send.cancelWrite(0x1_0000_0000L)),
                    "send-side cancel after graceful close should not expose adapter code validation"
            );
            assertEquals(ZmuxErrorSource.LOCAL, cancelClosed.source());
            assertEquals(ZmuxTerminationKind.GRACEFUL, cancelClosed.terminationKind());

            WriteClosedException resetClosed = assertInstanceOf(
                    WriteClosedException.class,
                    assertThrows(IOException.class, () -> send.closeWithError(0x1_0000_0000L, "too-wide")),
                    "send-side error close after graceful close should stay write-closed"
            );
            assertEquals(ZmuxErrorSource.LOCAL, resetClosed.source());
            assertEquals(ZmuxTerminationKind.GRACEFUL, resetClosed.terminationKind());

            ZmuxRecvStream accepted = await(acceptedFuture);
            assertEquals(-1, accepted.read(new byte[1]), "remote peer should still observe graceful EOF");
            accepted.close();
            send.close();
        }
    }

    @Test
    void cancelWriteRejectsTooWideCodeWithoutClosingWriteSide() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();

            AdapterUnsupportedException error = assertThrows(
                    AdapterUnsupportedException.class,
                    () -> clientStream.cancelWrite(0x1_0000_0000L)
            );
            assertTrue(error.getMessage().contains("32-bit"));
            assertEquals("write", error.operation());
            assertEquals(ZmuxErrorScope.STREAM, error.scope());
            assertEquals(ZmuxErrorDirection.WRITE, error.direction());

            clientStream.write(utf8("x"));
            clientStream.closeWrite();
            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(utf8("x"), readExactly(accepted, 1));
            assertEquals(-1, accepted.read(new byte[1]));
            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void closeWithErrorRejectsTooWideCodeWithoutClosingStream() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();

            AdapterUnsupportedException error = assertThrows(
                    AdapterUnsupportedException.class,
                    () -> clientStream.closeWithError(0x1_0000_0000L, "too-wide")
            );
            assertTrue(error.getMessage().contains("32-bit"));
            assertEquals("close", error.operation());
            assertEquals(ZmuxErrorScope.STREAM, error.scope());
            assertEquals(ZmuxErrorDirection.BOTH, error.direction());

            clientStream.write(utf8("y"));
            clientStream.closeWrite();
            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(utf8("y"), readExactly(accepted, 1));
            assertEquals(-1, accepted.read(new byte[1]));
            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void localReadClosureRemainsStickyAfterSessionAbort() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("p"));

            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(utf8("p"), readExactly(accepted, 1));

            accepted.closeRead();
            pair.client.closeWithError(77L, "adapter-contract-session-abort");

            ReadClosedException readClosed = assertInstanceOf(
                    ReadClosedException.class,
                    assertThrows(IOException.class, () -> accepted.read(new byte[1]))
            );
            ZmuxErrorDetails details = ZmuxErrors.details(readClosed);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.LOCAL, details.source());
            assertEquals(ZmuxErrorDirection.READ, details.direction());
            assertEquals(ZmuxTerminationKind.STOPPED, details.terminationKind());
        }
    }

    @Test
    void recvStreamErrorControlsAfterCloseReadPreferReadClosedOverTooWideCode() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxRecvStream> acceptedFuture = async(() -> pair.server.acceptUniStream(Duration.ofSeconds(5)));
            ZmuxSendStream clientStream = pair.client.openUniStream();
            clientStream.write(utf8("r"));

            ZmuxRecvStream accepted = await(acceptedFuture);
            accepted.closeRead();

            ReadClosedException cancelClosed = assertInstanceOf(
                    ReadClosedException.class,
                    assertThrows(IOException.class, () -> accepted.cancelRead(0x1_0000_0000L)),
                    "read-side cancel after closeRead should not expose adapter code validation"
            );
            assertEquals(ZmuxErrorSource.LOCAL, cancelClosed.source());
            assertEquals(ZmuxTerminationKind.STOPPED, cancelClosed.terminationKind());

            ReadClosedException stopClosed = assertInstanceOf(
                    ReadClosedException.class,
                    assertThrows(IOException.class, () -> accepted.closeWithError(0x1_0000_0000L, "too-wide")),
                    "read-side error close after closeRead should stay read-closed"
            );
            assertEquals(ZmuxErrorSource.LOCAL, stopClosed.source());
            assertEquals(ZmuxTerminationKind.STOPPED, stopClosed.terminationKind());

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void cancelReadRejectsTooWideCodeWithoutDroppingBufferedData() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("r"));

            ZmuxStream accepted = await(acceptedFuture);
            AdapterUnsupportedException error = assertThrows(
                    AdapterUnsupportedException.class,
                    () -> accepted.cancelRead(0x1_0000_0000L)
            );
            assertTrue(error.getMessage().contains("32-bit"));
            assertEquals("read", error.operation());
            assertEquals(ZmuxErrorScope.STREAM, error.scope());
            assertEquals(ZmuxErrorDirection.READ, error.direction());
            assertArrayEquals(utf8("r"), readExactly(accepted, 1));

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void cancelReadAfterSessionCloseDoesNotDiscardBufferedData() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("s"));
            ZmuxStream accepted = await(acceptedFuture);

            pair.client.closeWithError(88L, "adapter-session-close");
            awaitStats(pair.server, Duration.ofSeconds(5), snapshot -> snapshot.state() != SessionState.READY);

            ApplicationError closeError = assertInstanceOf(
                    ApplicationError.class,
                    assertThrows(IOException.class, () -> accepted.cancelRead(12L)),
                    "cancelRead after session close should prefer the structured session error"
            );
            assertEquals(88L, closeError.code());
            assertEquals("adapter-session-close", closeError.reason());

            ApplicationError invalidCodeCloseError = assertInstanceOf(
                    ApplicationError.class,
                    assertThrows(IOException.class, () -> accepted.cancelRead(0x1_0000_0000L)),
                    "cancelRead after session close should prefer session terminal state over adapter code validation"
            );
            assertEquals(88L, invalidCodeCloseError.code());
            assertEquals("adapter-session-close", invalidCodeCloseError.reason());

            assertArrayEquals(utf8("s"), readExactly(accepted, 1), "failed cancelRead must not discard buffered data");

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void recvCloseWithErrorRejectsTooWideCodeWithoutDroppingBufferedData() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxRecvStream> acceptedFuture = async(() -> pair.server.acceptUniStream(Duration.ofSeconds(5)));
            ZmuxSendStream clientStream = pair.client.openUniStream();
            clientStream.write(utf8("u"));

            ZmuxRecvStream accepted = await(acceptedFuture);
            AdapterUnsupportedException error = assertThrows(
                    AdapterUnsupportedException.class,
                    () -> accepted.closeWithError(0x1_0000_0000L, "too-wide")
            );
            assertTrue(error.getMessage().contains("32-bit"));
            assertEquals("read", error.operation());
            assertEquals(ZmuxErrorScope.STREAM, error.scope());
            assertEquals(ZmuxErrorDirection.READ, error.direction());
            assertArrayEquals(utf8("u"), readExactly(accepted, 1));

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void localWriteClosureRemainsStickyAfterSessionAbort() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("p"));

            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(utf8("p"), readExactly(accepted, 1));

            clientStream.closeWrite();
            pair.server.closeWithError(78L, "adapter-contract-session-abort");

            clientStream.write(new byte[0]);
            WriteClosedException writeClosed = assertInstanceOf(
                    WriteClosedException.class,
                    assertThrows(IOException.class, () -> clientStream.write(utf8("q")))
            );
            ZmuxErrorDetails details = ZmuxErrors.details(writeClosed);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.LOCAL, details.source());
            assertEquals(ZmuxErrorDirection.WRITE, details.direction());
            assertEquals(ZmuxTerminationKind.GRACEFUL, details.terminationKind());
        }
    }

    @Test
    void writevFinalAfterCloseWriteReturnsStickyLocalWriteClosed() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("p"));

            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(utf8("p"), readExactly(accepted, 1));

            clientStream.closeWrite();
            clientStream.write(new byte[0]);

            WriteClosedException writeClosed = assertInstanceOf(
                    WriteClosedException.class,
                    assertThrows(IOException.class, () -> clientStream.writevFinal(utf8("q")))
            );
            ZmuxErrorDetails details = ZmuxErrors.details(writeClosed);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.LOCAL, details.source());
            assertEquals(ZmuxErrorDirection.WRITE, details.direction());
            assertEquals(ZmuxTerminationKind.GRACEFUL, details.terminationKind());

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void combinedDeadlineSetterFailsAfterAdapterSessionClose() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream clientStream = pair.client.openStream();
            ZmuxSendStream sendOnly = pair.client.openUniStream();

            pair.client.closeWithError(79L, "adapter-deadline-close");

            ApplicationError error = assertInstanceOf(
                    ApplicationError.class,
                    assertThrows(IOException.class, () -> clientStream.setDeadline(Instant.now())),
                    "combined deadline update after session close should surface the structured session error"
            );
            assertEquals(79L, error.code());
            assertEquals("adapter-deadline-close", error.reason());
            ZmuxErrorDetails details = ZmuxErrors.details(error);
            assertNotNull(details);
            assertEquals(ZmuxErrorScope.SESSION, details.scope());
            assertEquals(ZmuxErrorDirection.BOTH, details.direction());
            assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
            assertEquals("read", details.operation());

            ApplicationError sendOnlyError = assertInstanceOf(
                    ApplicationError.class,
                    assertThrows(IOException.class, () -> sendOnly.setDeadline(Instant.now())),
                    "send-only combined deadline update after session close should preserve write operation"
            );
            assertEquals(79L, sendOnlyError.code());
            assertEquals("adapter-deadline-close", sendOnlyError.reason());
            assertEquals("write", sendOnlyError.operation());
        }
    }

    @Test
    void blockingReadSurfacedAsInterruptedIoWhenThreadInterrupted() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream(new OpenOptions(null, null, utf8("prelude")));
            ZmuxStream accepted = await(acceptedFuture);
            AtomicReference<Throwable> readError = new AtomicReference<>();

            Thread reader = new Thread(() -> {
                try {
                    accepted.read(new byte[1]);
                } catch (Throwable failure) {
                    readError.set(failure);
                }
            }, "adapter-interrupt-read");
            reader.start();

            Thread.sleep(100L);
            reader.interrupt();
            reader.join(Duration.ofSeconds(2).toMillis());

            assertFalse(reader.isAlive(), "read should unblock when the caller thread is interrupted");
            assertInstanceOf(InterruptedIOException.class, readError.get(), "blocking read should surface InterruptedIOException");

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void blockingOpenPreservesInterruptedExceptionAndDoesNotLeakRawStream() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall Netty event loop");

            AtomicReference<Throwable> openError = new AtomicReference<>();
            Thread opener = new Thread(() -> {
                try {
                    pair.client.openStream();
                } catch (Throwable failure) {
                    openError.set(failure);
                }
            }, "adapter-interrupt-open");
            opener.start();

            Thread.sleep(100L);
            opener.interrupt();
            opener.join(Duration.ofSeconds(2).toMillis());
            releaseEventLoop.countDown();

            assertFalse(opener.isAlive(), "open should unblock when the caller thread is interrupted");
            assertInstanceOf(InterruptedException.class, openError.get(), "blocking open should preserve InterruptedException");
            assertThrows(
                    AcceptTimeoutException.class,
                    () -> pair.server.acceptStream(Duration.ofMillis(200)),
                    "interrupted local open must not leak a raw QUIC stream onto the peer accept queue"
            );
        }
    }

    @Test
    void blockingOpenWithTimeoutFailsPromptlyAndDoesNotLeakRawStream() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall Netty event loop");

            OpenTimeoutException timeout = assertThrows(
                    OpenTimeoutException.class,
                    () -> pair.client.openStreamWithTimeout(Duration.ofMillis(100)),
                    "blocking open should surface the typed open-timeout exception"
            );
            assertEquals(OpenTimeoutException.MESSAGE, timeout.getMessage());

            releaseEventLoop.countDown();

            assertThrows(
                    AcceptTimeoutException.class,
                    () -> pair.server.acceptStream(Duration.ofMillis(200)),
                    "timed-out local open must not leak a raw QUIC stream onto the peer accept queue"
            );
        }
    }

    @Test
    void expiredOpenWithTimeoutFailsBeforeSubmittingRawStream() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            assertThrows(
                    OpenTimeoutException.class,
                    () -> pair.client.openStreamWithTimeout(Duration.ZERO),
                    "expired adapter open timeout should fail before raw stream submission"
            );

            assertThrows(
                    AcceptTimeoutException.class,
                    () -> pair.server.acceptStream(Duration.ofMillis(150)),
                    "expired adapter open timeout must not leak a raw QUIC stream"
            );
        }
    }

    @Test
    void blockingOpenUniAndSendWithTimeoutFailsPromptlyAndDoesNotLeakRawStream() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall Netty event loop");

            OpenTimeoutException timeout = assertThrows(
                    OpenTimeoutException.class,
                    () -> pair.client.openUniAndSendWithTimeout(Duration.ofMillis(100), utf8("x")),
                    "combined open+send helper should surface the typed open-timeout exception while local open is blocked"
            );
            assertEquals(OpenTimeoutException.MESSAGE, timeout.getMessage());

            releaseEventLoop.countDown();

            assertThrows(
                    AcceptTimeoutException.class,
                    () -> pair.server.acceptUniStream(Duration.ofMillis(200)),
                    "timed-out combined open+send must not leak a raw QUIC stream onto the peer accept queue"
            );
        }
    }

    @Test
    void blockingOpenFailsPromptlyWhenSessionCloseStartsBeforeTransportCloseCompletes() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall client Netty event loop");

            AtomicReference<Throwable> openError = new AtomicReference<>();
            Thread opener = new Thread(() -> {
                try {
                    pair.client.openStream();
                } catch (Throwable failure) {
                    openError.set(failure);
                }
            }, "adapter-close-open");
            opener.start();

            Thread.sleep(100L);

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            Thread closer = new Thread(() -> {
                try {
                    pair.client.close();
                } catch (Throwable failure) {
                    closeError.set(failure);
                }
            }, "adapter-close-open-session");
            closer.start();

            opener.join(Duration.ofSeconds(2).toMillis());

            assertFalse(opener.isAlive(), "blocking open should unblock once session close starts");
            assertInstanceOf(SessionClosedException.class, openError.get(), "in-flight open should fail with session close once shutdown begins");
            assertThrows(
                    AcceptTimeoutException.class,
                    () -> pair.server.acceptStream(Duration.ofMillis(200)),
                    "open aborted by session close must not leak a raw QUIC stream onto the peer accept queue"
            );

            releaseEventLoop.countDown();
            closer.join(Duration.ofSeconds(2).toMillis());

            assertFalse(closer.isAlive(), "close should complete after the Netty event loop is released");
            assertNull(closeError.get(), "close should not fail once the transport close completes");
        }
    }

    @Test
    void localCloseRejectsQueuedAcceptsAndNewOpensBeforeTransportCloseCompletes() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession server = (NettyQuicSession) pair.server;
            byte[] payload = utf8("queued-close-payload");
            ZmuxStream queued = pair.client.openStream(new OpenOptions(4L, null, utf8("queued-close")));
            queued.write(payload);
            awaitStats(
                    server,
                    Duration.ofSeconds(5),
                    snapshot -> snapshot.acceptBacklog().count() == 1L
                            && snapshot.acceptBacklog().bytes() >= payload.length
            );
            NettyQuicStreamState queuedState = bidiAcceptSnapshot(server).get(0).state;
            assertTrue(
                    queuedState.bufferedInboundBytes() >= payload.length,
                    "test setup should have unread payload buffered in the accept backlog"
            );

            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawServer.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall server Netty event loop");

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            Thread closer = new Thread(() -> {
                try {
                    pair.server.close();
                } catch (Throwable failure) {
                    closeError.set(failure);
                }
            }, "adapter-close-pending");
            closer.start();

            SessionStats closingStats = awaitStats(
                    server,
                    Duration.ofSeconds(2),
                    snapshot -> snapshot.state() == SessionState.CLOSING && snapshot.acceptBacklog().count() == 0L
            );
            assertEquals(SessionState.CLOSING, closingStats.state(), "local close should enter CLOSING before terminal completion");
            assertEquals(0L, closingStats.acceptBacklog().count(), "queued accept backlog should be discarded when close starts");
            assertEquals(0L, queuedState.bufferedInboundBytes(), "discarded accept backlog stream should release unread payload");
            assertThrows(SessionClosedException.class, () -> pair.server.acceptStream(Duration.ofMillis(200)));
            assertThrows(SessionClosedException.class, () -> pair.server.openStream());

            releaseEventLoop.countDown();
            closer.join(Duration.ofSeconds(2).toMillis());

            assertFalse(closer.isAlive(), "close should complete after the Netty event loop is released");
            assertNull(closeError.get(), "close should not fail once the transport close completes");

            try {
                queued.close();
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    void blockingWriteFailsPromptlyWhenSessionCloseStartsBeforeTransportCloseCompletes() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream clientStream = pair.client.openStream(new OpenOptions(6L, null, utf8("blocking-write")));
            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));

            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall client Netty event loop");

            AtomicReference<Throwable> writeError = new AtomicReference<>();
            Thread writer = new Thread(() -> {
                try {
                    clientStream.write(utf8("blocked"));
                } catch (Throwable failure) {
                    writeError.set(failure);
                }
            }, "adapter-close-write");
            writer.start();

            Thread.sleep(100L);

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            Thread closer = new Thread(() -> {
                try {
                    pair.client.close();
                } catch (Throwable failure) {
                    closeError.set(failure);
                }
            }, "adapter-close-write-session");
            closer.start();

            writer.join(Duration.ofSeconds(2).toMillis());
            assertFalse(writer.isAlive(), "blocked write should unblock once close starts");
            assertInstanceOf(SessionClosedException.class, writeError.get(), "blocked write should fail with session close once shutdown begins");

            releaseEventLoop.countDown();
            closer.join(Duration.ofSeconds(2).toMillis());

            assertFalse(closer.isAlive(), "close should complete after the Netty event loop is released");
            assertNull(closeError.get(), "close should not fail once the transport close completes");

            try {
                accepted.close();
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    void closeWriteRespectsWriteDeadlineWhenTransportShutdownStalls() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("x"));
            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
            assertArrayEquals(utf8("x"), readExactly(accepted, 1));

            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall client Netty event loop");

            clientStream.setWriteDeadline(Instant.now().plusMillis(100));
            assertThrows(
                    io.zmux.WriteTimeoutException.class,
                    clientStream::closeWrite,
                    "closeWrite should honor the stream write deadline while waiting for transport shutdown"
            );

            releaseEventLoop.countDown();

            try {
                accepted.close();
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    void closeWriteDoesNotWaitForeverForTransportShutdownFuture() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("x"));
            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
            assertArrayEquals(utf8("x"), readExactly(accepted, 1));

            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall client Netty event loop");

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            CountDownLatch closeDone = new CountDownLatch(1);
            Thread closer = new Thread(() -> {
                try {
                    clientStream.closeWrite();
                } catch (Throwable failure) {
                    closeError.set(failure);
                } finally {
                    closeDone.countDown();
                }
            }, "adapter-closeWrite-nonblocking-terminal");
            closer.start();

            assertTrue(
                    closeDone.await(1L, TimeUnit.SECONDS),
                    "closeWrite must not wait forever for a transport shutdown future when no write deadline is set"
            );
            assertNull(closeError.get(), "closeWrite should locally commit the graceful write close");
            releaseEventLoop.countDown();

            closer.join(Duration.ofSeconds(2).toMillis());
            assertFalse(closer.isAlive(), "closeWrite thread should be finished after local terminal commit");

            try {
                accepted.close();
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    void closeWriteWaitsForInFlightDataWrite() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("x"));
            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
            assertArrayEquals(utf8("x"), readExactly(accepted, 1));

            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall client Netty event loop");

            AtomicReference<Throwable> writeError = new AtomicReference<>();
            CountDownLatch writeDone = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                try {
                    clientStream.write(utf8("y"));
                } catch (Throwable failure) {
                    writeError.set(failure);
                } finally {
                    writeDone.countDown();
                }
            }, "adapter-serialized-write");
            writer.start();
            assertFalse(writeDone.await(100L, TimeUnit.MILLISECONDS), "write should be waiting on the stalled Netty future");

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            CountDownLatch closeDone = new CountDownLatch(1);
            Thread closer = new Thread(() -> {
                try {
                    clientStream.closeWrite();
                } catch (Throwable failure) {
                    closeError.set(failure);
                } finally {
                    closeDone.countDown();
                }
            }, "adapter-serialized-closeWrite");
            closer.start();
            assertFalse(closeDone.await(100L, TimeUnit.MILLISECONDS), "closeWrite must not overtake an in-flight write");

            releaseEventLoop.countDown();

            writer.join(Duration.ofSeconds(2).toMillis());
            closer.join(Duration.ofSeconds(2).toMillis());
            assertFalse(writer.isAlive(), "write should finish after the event loop is released");
            assertFalse(closer.isAlive(), "closeWrite should finish after the in-flight write completes");
            assertNull(writeError.get(), "in-flight write should not be converted into a local write-close error");
            assertNull(closeError.get(), "closeWrite should succeed after serialized write completion");
            assertArrayEquals(utf8("y"), readExactly(accepted, 1));

            try {
                accepted.close();
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    void hugeWriteDeadlineDoesNotOverflowWhileTransportWriteWaits() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            ZmuxStream clientStream = pair.client.openStream();

            CountDownLatch eventLoopBlocked = new CountDownLatch(1);
            CountDownLatch releaseEventLoop = new CountDownLatch(1);
            pair.rawClient.eventLoop().execute(() -> stallEventLoopUntilReleased(eventLoopBlocked, releaseEventLoop));
            assertTrue(eventLoopBlocked.await(1L, TimeUnit.SECONDS), "failed to stall client Netty event loop");

            clientStream.setWriteTimeout(Duration.ofSeconds(Long.MAX_VALUE));
            AtomicReference<Throwable> writeError = new AtomicReference<>();
            CountDownLatch writeDone = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                try {
                    clientStream.write(utf8("x"));
                } catch (Throwable failure) {
                    writeError.set(failure);
                } finally {
                    writeDone.countDown();
                }
            }, "adapter-huge-write-deadline");
            writer.start();

            Thread.sleep(100L);
            assertFalse(
                    writeDone.await(100L, TimeUnit.MILLISECONDS),
                    "huge write deadline should not expire while the Netty write future is stalled"
            );

            releaseEventLoop.countDown();

            writer.join(Duration.ofSeconds(2).toMillis());
            assertFalse(writer.isAlive(), "write should complete once the Netty event loop is released");
            assertNull(writeError.get(), "huge write deadline should not overflow into a spurious timeout");

            ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
            assertArrayEquals(utf8("x"), readExactly(accepted, 1));

            try {
                accepted.close();
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    void closeWithErrorIsStickyLocally() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream();
            clientStream.write(utf8("x"));
            ZmuxStream accepted = await(acceptedFuture);
            assertArrayEquals(utf8("x"), readExactly(accepted, 1));

            clientStream.closeWithError(55L, "adapter-contract-abort");

            ApplicationError readError = assertInstanceOf(ApplicationError.class, assertThrows(IOException.class, () -> clientStream.read(new byte[1])));
            assertEquals(55L, readError.code());
            assertEquals("adapter-contract-abort", readError.reason());

            ApplicationError writeError = assertInstanceOf(
                    ApplicationError.class,
                    assertThrows(IOException.class, () -> clientStream.write(utf8("y")))
            );
            assertEquals(55L, writeError.code());
            assertEquals("adapter-contract-abort", writeError.reason());

            SessionStats clientStats = awaitStats(
                    pair.client,
                    Duration.ofSeconds(5),
                    snapshot -> Long.valueOf(1L).equals(snapshot.reasons().abort().get(55L))
            );
            assertEquals(Long.valueOf(1L), clientStats.reasons().abort().get(55L), "local abort reason count mismatch");

            IOException peerFailure = assertThrows(IOException.class, () -> accepted.read(new byte[1]));
            assertNotNull(peerFailure);
        }
    }

    @Test
    void streamReasonStatsDoNotCountLateLocalTerminalOpsAfterSessionClose() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> firstAcceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            CompletableFuture<ZmuxStream> secondAcceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));

            ZmuxStream first = pair.client.openStream();
            ZmuxStream second = pair.client.openStream();
            first.write(utf8("a"));
            second.write(utf8("b"));

            ZmuxStream acceptedFirst = await(firstAcceptedFuture);
            ZmuxStream acceptedSecond = await(secondAcceptedFuture);
            byte firstByte = readExactly(acceptedFirst, 1)[0];
            byte secondByte = readExactly(acceptedSecond, 1)[0];
            assertTrue(firstByte != secondByte, "concurrent accept waiters must surface both ready streams");
            assertTrue(
                    (firstByte == utf8("a")[0] && secondByte == utf8("b")[0])
                            || (firstByte == utf8("b")[0] && secondByte == utf8("a")[0]),
                    "concurrent accept waiters may complete in either order, but must surface both payloads"
            );

            pair.client.closeWithError(91L, "adapter-contract-session-close");
            assertTrue(pair.client.awaitTermination(Duration.ofSeconds(5)));

            assertThrows(IOException.class, () -> first.cancelWrite(41L));
            assertThrows(IOException.class, () -> second.closeWithError(51L, "late-stream-abort"));

            SessionStats snapshot = pair.client.stats();
            assertNull(snapshot.reasons().reset().get(41L), "late local reset after session close must not inflate reason stats");
            assertNull(snapshot.reasons().abort().get(51L), "late local abort after session close must not inflate reason stats");

            try {
                acceptedFirst.close();
            } catch (IOException ignored) {
            }
            try {
                acceptedSecond.close();
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    void sessionAbortRejectsTooWideApplicationCode() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            AdapterUnsupportedException error = assertThrows(
                    AdapterUnsupportedException.class,
                    () -> pair.client.closeWithError(0x1_0000_0000L, "too-wide")
            );
            assertTrue(error.getMessage().contains("32-bit"));
            assertEquals("close", error.operation());
            assertEquals(ZmuxErrorScope.SESSION, error.scope());
            assertEquals(ZmuxErrorDirection.BOTH, error.direction());
        }
    }

    @Test
    void gracefulSessionCloseTerminatesBothSidesAndRejectsFurtherOpens() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<Boolean> serverWait = async(() -> pair.server.awaitTermination(Duration.ofSeconds(5)));

            pair.client.close();

            assertTrue(pair.client.awaitTermination(Duration.ofSeconds(5)));
            assertTrue(await(serverWait));
            assertEquals(SessionState.CLOSED, pair.client.state());
            assertEquals(SessionState.CLOSED, pair.server.state());

            SessionClosedException serverOpenFailure = assertThrows(SessionClosedException.class, () -> pair.server.openStream());
            assertEquals(SessionClosedException.MESSAGE, serverOpenFailure.getMessage());
        }
    }

    @Test
    void preparedAcceptedStreamIsNotPublishedAfterSessionClose() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<ZmuxStream> acceptedFuture = async(() -> pair.server.acceptStream(Duration.ofSeconds(5)));
            ZmuxStream clientStream = pair.client.openStream(new OpenOptions(3L, null, utf8("late")));
            NettyQuicBidiStream accepted = (NettyQuicBidiStream) await(acceptedFuture);
            NettyQuicSession server = (NettyQuicSession) pair.server;

            server.close();
            assertTrue(server.awaitTermination(Duration.ofSeconds(5)), "server session should terminate");

            assertFalse(
                    server.publishPreparedAcceptedStream(accepted.state),
                    "closed session must reject late prepared accepted stream publication"
            );
            assertEquals(0L, server.stats().acceptBacklog().count(), "late publication must not re-populate the accept backlog");
            assertThrows(SessionClosedException.class, () -> server.acceptStream(Duration.ofMillis(50)));

            accepted.close();
            clientStream.close();
        }
    }

    @Test
    void stateReportsClosingBeforeTerminalCloseWhenLocalShutdownHasStarted() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession client = (NettyQuicSession) pair.client;

            setField(client, "closing", true);

            assertEquals(SessionState.CLOSING, client.state());
            assertEquals(SessionState.CLOSING, client.stats().state());
        }
    }

    @Test
    void stateReportsDrainingBeforeTerminalCloseWhenRemoteCloseEventArrives() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession server = (NettyQuicSession) pair.server;

            setField(server, "closeEvent", newCloseEvent(true, 0, new byte[0]));

            assertEquals(SessionState.DRAINING, server.state());
            assertEquals(SessionState.DRAINING, server.stats().state());
        }
    }

    @Test
    void transportCloseEventSurfacesStructuredTransportErrorBeforeTerminalClose() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession server = (NettyQuicSession) pair.server;

            setField(server, "closeEvent", newCloseEvent(false, 7, utf8("transport-close")));

            ZmuxException error = assertInstanceOf(
                    ZmuxException.class,
                    assertThrows(IOException.class, server::openStream)
            );
            assertEquals(7L, error.code());
            assertTrue(error.getMessage().contains("transport-close"));
            ZmuxErrorDetails details = ZmuxErrors.details(error);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.REMOTE, details.source());
            assertEquals(ZmuxErrorDirection.BOTH, details.direction());
            assertEquals(ZmuxErrorScope.SESSION, details.scope());
            assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
        }
    }

    @Test
    void plainTransportIoFailureIsStructuredAsSessionTermination() {
        IOException cause = new IOException("plain-transport-io");

        ZmuxException error = assertInstanceOf(
                ZmuxException.class,
                NettyQuicSupport.translateThrowable(cause)
        );

        assertEquals(ErrorCode.INTERNAL.code(), error.code());
        assertSame(cause, error.getCause());
        assertTrue(error.getMessage().contains("plain-transport-io"));
        ZmuxErrorDetails details = ZmuxErrors.details(error);
        assertNotNull(details);
        assertEquals(ZmuxErrorScope.SESSION, details.scope());
        assertEquals(ZmuxErrorSource.TRANSPORT, details.source());
        assertEquals(ZmuxErrorDirection.BOTH, details.direction());
        assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
    }

    @Test
    void structuredNestedIoFailureIsPreservedWhenTranslatingTransportFailure() {
        ApplicationError nested = NettyQuicSupport.streamApplicationError(
                37L,
                "nested",
                ZmuxErrorSource.REMOTE,
                ZmuxErrorDirection.READ,
                ZmuxTerminationKind.RESET
        );
        IOException wrapper = new IOException("wrapper", nested);

        assertSame(nested, NettyQuicSupport.translateThrowable(wrapper));
    }

    @Test
    void blockedAcceptSurfacesStructuredTransportErrorWhenCloseStartsLater() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession server = (NettyQuicSession) pair.server;
            CompletableFuture<Object> acceptedFuture = async(() -> {
                try {
                    return server.acceptStream(Duration.ofSeconds(5));
                } catch (Throwable failure) {
                    return failure;
                }
            });

            Thread.sleep(100L);

            QuicConnectionCloseEvent closeEvent = newCloseEvent(false, 11, utf8("blocked-accept-close"));
            setField(server, "closeEvent", closeEvent);
            invokePrivate(
                    server,
                    "beginClosing",
                    new Class<?>[]{IOException.class},
                    NettyQuicSupport.connectionCloseError(closeEvent, null)
            );

            Object outcome = await(acceptedFuture);
            ZmuxException error = assertInstanceOf(ZmuxException.class, outcome);
            assertEquals(11L, error.code());
            assertTrue(error.getMessage().contains("blocked-accept-close"));
            ZmuxErrorDetails details = ZmuxErrors.details(error);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.REMOTE, details.source());
            assertEquals(ZmuxErrorDirection.BOTH, details.direction());
            assertEquals(ZmuxErrorScope.SESSION, details.scope());
            assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
        }
    }

    @Test
    void failedOpenFuturePrefersStructuredSessionCloseErrorAfterCloseStarts() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession client = (NettyQuicSession) pair.client;
            DefaultPromise<QuicStreamChannel> failedOpen = new DefaultPromise<>(pair.rawClient.eventLoop());
            failedOpen.setFailure(new RuntimeException("failed-open-future"));

            QuicConnectionCloseEvent closeEvent = newCloseEvent(false, 17, utf8("open-close-race"));
            setField(client, "closeEvent", closeEvent);
            invokePrivate(
                    client,
                    "beginClosing",
                    new Class<?>[]{IOException.class},
                    NettyQuicSupport.connectionCloseError(closeEvent, null)
            );

            InvocationTargetException failure = assertThrows(
                    InvocationTargetException.class,
                    () -> invokePrivate(
                            client,
                            "awaitStreamFuture",
                            new Class<?>[]{Future.class, TimeoutBudget.class},
                            failedOpen,
                            TimeoutBudget.unbounded()
                    )
            );

            ZmuxException error = assertInstanceOf(
                    ZmuxException.class,
                    failure.getCause(),
                    "open wait should prefer the structured session close error once close has started"
            );
            assertEquals(17L, error.code());
            assertTrue(error.getMessage().contains("open-close-race"));
            ZmuxErrorDetails details = ZmuxErrors.details(error);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.REMOTE, details.source());
            assertEquals(ZmuxErrorDirection.BOTH, details.direction());
            assertEquals(ZmuxErrorScope.SESSION, details.scope());
            assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
        }
    }

    @Test
    void failedWriteFuturePrefersStructuredSessionCloseErrorAfterCloseStarts() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession client = (NettyQuicSession) pair.client;
            NettyQuicBidiStream stream = (NettyQuicBidiStream) pair.client.openStream();
            QuicStreamChannel rawStream = (QuicStreamChannel) getField(stream.state, "channel");
            io.netty.channel.DefaultChannelPromise failedWrite = new io.netty.channel.DefaultChannelPromise(rawStream);
            failedWrite.setFailure(new RuntimeException("failed-write-future"));

            QuicConnectionCloseEvent closeEvent = newCloseEvent(false, 19, utf8("write-close-race"));
            setField(client, "closeEvent", closeEvent);
            invokePrivate(
                    client,
                    "beginClosing",
                    new Class<?>[]{IOException.class},
                    NettyQuicSupport.connectionCloseError(closeEvent, null)
            );

            InvocationTargetException failure = assertThrows(
                    InvocationTargetException.class,
                    () -> invokePrivate(
                            stream.state,
                            "awaitWriteFuture",
                            new Class<?>[]{ChannelFuture.class, int.class, long.class},
                            failedWrite,
                            1,
                            System.nanoTime()
                    )
            );

            ZmuxException error = assertInstanceOf(
                    ZmuxException.class,
                    failure.getCause(),
                    "write wait should prefer the structured session close error once close has started"
            );
            assertEquals(19L, error.code());
            assertTrue(error.getMessage().contains("write-close-race"));
            ZmuxErrorDetails details = ZmuxErrors.details(error);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.REMOTE, details.source());
            assertEquals(ZmuxErrorDirection.BOTH, details.direction());
            assertEquals(ZmuxErrorScope.SESSION, details.scope());
            assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
        }
    }

    @Test
    void failedWriteSideFuturePrefersStructuredSessionCloseErrorAfterCloseStarts() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession client = (NettyQuicSession) pair.client;
            NettyQuicBidiStream stream = (NettyQuicBidiStream) pair.client.openStream();
            QuicStreamChannel rawStream = (QuicStreamChannel) getField(stream.state, "channel");
            io.netty.channel.DefaultChannelPromise failedWriteSide = new io.netty.channel.DefaultChannelPromise(rawStream);
            failedWriteSide.setFailure(new RuntimeException("failed-write-side-future"));

            QuicConnectionCloseEvent closeEvent = newCloseEvent(false, 23, utf8("write-side-close-race"));
            setField(client, "closeEvent", closeEvent);
            invokePrivate(
                    client,
                    "beginClosing",
                    new Class<?>[]{IOException.class},
                    NettyQuicSupport.connectionCloseError(closeEvent, null)
            );

            InvocationTargetException failure = assertThrows(
                    InvocationTargetException.class,
                    () -> invokePrivate(
                            stream.state,
                            "awaitWriteSideFuture",
                            new Class<?>[]{ChannelFuture.class, long.class},
                            failedWriteSide,
                            System.nanoTime()
                    )
            );

            ZmuxException error = assertInstanceOf(
                    ZmuxException.class,
                    failure.getCause(),
                    "write-side terminal wait should prefer the structured session close error once close has started"
            );
            assertEquals(23L, error.code());
            assertTrue(error.getMessage().contains("write-side-close-race"));
            ZmuxErrorDetails details = ZmuxErrors.details(error);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.REMOTE, details.source());
            assertEquals(ZmuxErrorDirection.BOTH, details.direction());
            assertEquals(ZmuxErrorScope.SESSION, details.scope());
            assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
        }
    }

    @Test
    void failedPreludeReadPrefersStructuredSessionCloseErrorAfterCloseStarts() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession client = (NettyQuicSession) pair.client;
            NettyQuicBidiStream stream = (NettyQuicBidiStream) pair.client.openStream();
            Object readHalf = getField(stream.state, "readHalf");
            ApplicationError remoteReadFailure = NettyQuicSupport.streamApplicationError(
                    29L,
                    "remote-prelude-race",
                    ZmuxErrorSource.REMOTE,
                    ZmuxErrorDirection.READ,
                    ZmuxTerminationKind.RESET
            );
            invokePrivate(readHalf, "failRemote", new Class<?>[]{IOException.class}, remoteReadFailure);

            QuicConnectionCloseEvent closeEvent = newCloseEvent(false, 31, utf8("prelude-close-race"));
            setField(client, "closeEvent", closeEvent);
            invokePrivate(
                    client,
                    "beginClosing",
                    new Class<?>[]{IOException.class},
                    NettyQuicSupport.connectionCloseError(closeEvent, null)
            );

            InvocationTargetException failure = assertThrows(
                    InvocationTargetException.class,
                    () -> invokePrivate(
                            stream.state,
                            "readExactly",
                            new Class<?>[]{int.class, Duration.class, String.class},
                            1,
                            Duration.ofSeconds(1),
                            "read stream prelude length"
                    )
            );

            ZmuxException error = assertInstanceOf(
                    ZmuxException.class,
                    failure.getCause(),
                    "prelude read should prefer the structured session close error once close has started"
            );
            assertEquals(31L, error.code());
            assertTrue(error.getMessage().contains("prelude-close-race"));
            ZmuxErrorDetails details = ZmuxErrors.details(error);
            assertNotNull(details);
            assertEquals(ZmuxErrorSource.REMOTE, details.source());
            assertEquals(ZmuxErrorDirection.BOTH, details.direction());
            assertEquals(ZmuxErrorScope.SESSION, details.scope());
            assertEquals(ZmuxTerminationKind.SESSION_TERMINATION, details.terminationKind());
        }
    }

    @Test
    void sessionAbortPropagatesApplicationErrorToPeer() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            CompletableFuture<Boolean> serverWait = async(() -> pair.server.awaitTermination(Duration.ofSeconds(5)));

            pair.client.closeWithError(91L, "adapter-contract-session-abort");

            assertTrue(pair.client.awaitTermination(Duration.ofSeconds(5)));
            assertTrue(await(serverWait));
            assertEquals(SessionState.FAILED, pair.client.state());
            assertEquals(SessionState.FAILED, pair.server.state());

            ApplicationError serverOpenFailure = assertInstanceOf(
                    ApplicationError.class,
                    assertThrows(IOException.class, () -> pair.server.openStream())
            );
            assertEquals(91L, serverOpenFailure.code());
            assertEquals("adapter-contract-session-abort", serverOpenFailure.reason());
        }
    }

    @Test
    void readyAcceptedStreamBypassesStalledPreludeAndQueueRecoversAfterTimeout() throws Exception {
        NettyQuicSessionOptions serverOptions = new NettyQuicSessionOptions(Duration.ofMillis(100), 0);
        try (NettyQuicTestSupport.SessionPair pair = openPair(NettyQuicSessionOptions.defaults(), serverOptions)) {
            QuicStreamChannel stalled = openRawBidiStream(pair);
            try {
                writeRaw(stalled, new byte[]{0x40});

                ZmuxStream ready = pair.client.openStream();
                ready.write(utf8("x"));

                ZmuxStream accepted = pair.server.acceptStream(Duration.ofSeconds(5));
                assertEquals(ready.streamId(), accepted.streamId());
                assertArrayEquals(utf8("x"), readExactly(accepted, 1));
                accepted.close();
                ready.close();

                Thread.sleep(250L);
                SessionStats refusedStats = awaitStats(
                        pair.server,
                        Duration.ofSeconds(5),
                        snapshot -> snapshot.hiddenState().refused() >= 1L
                );
                assertEquals(1L, refusedStats.hiddenState().refused(), "timed-out accepted prelude should count as hidden refused");

                ZmuxStream next = pair.client.openStream();
                next.write(utf8("y"));

                ZmuxStream acceptedNext = pair.server.acceptStream(Duration.ofSeconds(5));
                assertEquals(next.streamId(), acceptedNext.streamId());
                assertArrayEquals(utf8("y"), readExactly(acceptedNext, 1));
                acceptedNext.close();
                next.close();
            } finally {
                stalled.close().syncUninterruptibly();
            }
        }
    }

    @Test
    void statsTrackTransportIdleTimeoutAsKeepaliveTimeout() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession client = (NettyQuicSession) pair.client;
            ApplicationError timeout = NettyQuicSupport.sessionApplicationError(
                    ErrorCode.IDLE_TIMEOUT.code(),
                    "",
                    ZmuxErrorSource.TRANSPORT,
                    ZmuxTerminationKind.TIMEOUT
            );

            invokePrivate(client, "onSessionClosed", new Class<?>[]{IOException.class}, timeout);

            SessionStats snapshot = client.stats();
            assertEquals(SessionState.FAILED, snapshot.state());
            assertEquals(1L, snapshot.diagnostics().keepaliveTimeouts());
        }
    }

    @Test
    void statsTrackLateReadDiscardDiagnosticsByTerminationKind() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            NettyQuicSession client = (NettyQuicSession) pair.client;

            client.noteLateReadDiscard(
                    NettyQuicSupport.readClosedError(ZmuxErrorSource.LOCAL, ZmuxTerminationKind.STOPPED),
                    5
            );
            client.noteLateReadDiscard(
                    NettyQuicSupport.streamApplicationError(
                            41L,
                            "",
                            ZmuxErrorSource.REMOTE,
                            ZmuxErrorDirection.READ,
                            ZmuxTerminationKind.RESET
                    ),
                    7
            );
            client.noteLateReadDiscard(
                    NettyQuicSupport.streamApplicationError(
                            51L,
                            "",
                            ZmuxErrorSource.REMOTE,
                            ZmuxErrorDirection.BOTH,
                            ZmuxTerminationKind.ABORT
                    ),
                    11
            );

            SessionStats snapshot = client.stats();
            assertEquals(5L, snapshot.diagnostics().lateDataAfterCloseRead());
            assertEquals(7L, snapshot.diagnostics().lateDataAfterReset());
            assertEquals(11L, snapshot.diagnostics().lateDataAfterAbort());
        }
    }

    @Test
    void awaitTerminationTreatsZeroAndNegativeTimeoutAsImmediatePoll() throws Exception {
        try (NettyQuicTestSupport.SessionPair pair = openPair()) {
            assertFalse(pair.client.awaitTermination(Duration.ZERO));
            assertFalse(pair.client.awaitTermination(Duration.ofMillis(-1L)));
        }
    }
}
