package io.zmux.runtime;

import io.zmux.ApplicationError;
import io.zmux.ErrorCode;
import io.zmux.MetadataUpdate;
import io.zmux.Settings;
import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.FrameType;
import io.zmux.protocol.Protocol;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.*;

final class LocalOpenVisibilityTest {
    @Test
    void committedLocalOpenDefersPeerVisibleStateAndPendingPriorityUpdate() throws Exception {
        long capabilities = Protocol.CAPABILITY_PRIORITY_UPDATE | Protocol.CAPABILITY_PRIORITY_HINTS;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());

        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        stream.queueWrite("hello".getBytes(StandardCharsets.UTF_8), 0, "hello".length());

        synchronized (runtime.lock()) {
            assertTrue(stream.openedOnWire(), "opening commit should consume the stream id");
            assertFalse(stream.peerVisible(), "writer submission should be required before the stream becomes peer-visible");
            assertTrue(stream.awaitingPeerVisibilityLocked(), "committed local open should remain reclaimable until peer-visible");
        }

        stream.updateMetadata(new MetadataUpdate(9L, null));

        synchronized (runtime.lock()) {
            assertTrue(stream.hasPendingPriorityUpdateLocked(), "priority update should stay staged until the opener becomes peer-visible");
            assertFalse(stream.peerVisible(), "staging advisory metadata must not mark the stream peer-visible");
        }

        synchronized (runtime.lock()) {
            SessionRuntimeTestSupport.invokePrivate(runtime, "markPeerVisibleLocked", new Class<?>[]{StreamRuntime.class}, stream);
        }

        synchronized (runtime.lock()) {
            assertTrue(stream.peerVisible(), "markPeerVisibleLocked should transition the local stream");
            assertFalse(stream.awaitingPeerVisibilityLocked(), "peer-visible stream must leave the unseen-local state");
            assertFalse(stream.hasPendingPriorityUpdateLocked(), "staged priority update should flush once peer-visible");
        }
    }

    @Test
    void committedInvisibleOpenReusesOpeningMetadataWhenOpenCarriageRemainsAvailable() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA
                | Protocol.CAPABILITY_PRIORITY_UPDATE
                | Protocol.CAPABILITY_PRIORITY_HINTS;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            stream.initializeLocalOpenedLocked(
                    SessionRuntime.firstLocalStreamId(runtime.localRole(), true),
                    runtime.peerSettings().initialMaxStreamDataBidiPeerOpened(),
                    runtime.localSettings().initialMaxStreamDataBidiLocallyOpened()
            );
            stream.markOpenedOnWireLocked();
            assertEquals(LocalOpenPhase.NEEDS_EMIT, stream.localOpenPhaseLocked(), "test requires a committed local stream whose opener is not yet queued");
        }

        stream.updateMetadata(new MetadataUpdate(9L, null));

        synchronized (runtime.lock()) {
            assertEquals(9L, stream.metadata().priority(), "committed invisible open should update local metadata immediately");
            assertFalse(stream.hasPendingPriorityUpdateLocked(), "opening-metadata path must not stage a pending PRIORITY_UPDATE");
            assertArrayEquals(
                    FrameCodec.buildOpenMetadataPrefix(
                            capabilities,
                            9L,
                            null,
                            StreamRuntime.EMPTY_BYTES,
                            runtime.peerSettings().maxFramePayload()
                    ),
                    stream.metadataStateInternal().buildOpeningPrefixLocked(runtime),
                    "committed invisible open should re-encode the opener prefix instead of falling back to PRIORITY_UPDATE"
            );
        }
    }

    @Test
    void committedInvisibleOpenRequeuesControlOpenerWhenNeedsEmit() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream(
                new io.zmux.OpenOptions(0L, null, "tag".getBytes(StandardCharsets.UTF_8))
        );

        synchronized (runtime.lock()) {
            runtime.beginLocalOpenLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            stream.clearOpeningFramePendingLocked();
            assertEquals(LocalOpenPhase.NEEDS_EMIT, stream.localOpenPhaseLocked(), "test requires a committed local stream with no queued opener");

            stream.prepareLocalControlOpenerLocked(true, true);

            Deque<Object> urgentQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "urgentQueue");
            assertEquals(1, urgentQueue.size(), "control-path opener should be requeued on the urgent lane");
            Object opener = urgentQueue.peekFirst();
            assertTrue(SessionRuntimeTestSupport.outboundOpeningFrame(opener), "requeued control opener should still be tagged as an opening frame");
            assertTrue(
                    (SessionRuntimeTestSupport.outboundFrame(opener).flags() & Protocol.FRAME_FLAG_OPEN_METADATA) != 0,
                    "requeued opener should carry opening metadata when available"
            );
        }
    }

    @Test
    void closeWriteReemitsOpeningFrameWhenCommittedInvisibleOpenNeedsEmit() throws Exception {
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA;
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(capabilities, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream(
                new io.zmux.OpenOptions(0L, null, "tag".getBytes(StandardCharsets.UTF_8))
        );

        synchronized (runtime.lock()) {
            runtime.beginLocalOpenLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            stream.clearOpeningFramePendingLocked();
            assertEquals(LocalOpenPhase.NEEDS_EMIT, stream.localOpenPhaseLocked(), "test requires a committed local stream with no queued opener");
        }

        stream.closeWrite();

        synchronized (runtime.lock()) {
            Deque<Object> dataQueue = SessionRuntimeTestSupport.outboundQueue(runtime, "dataQueue");
            assertEquals(1, dataQueue.size(), "closeWrite should enqueue exactly one DATA|FIN frame in this setup");
            Object fin = dataQueue.peekFirst();
            assertTrue(SessionRuntimeTestSupport.outboundOpeningFrame(fin), "closeWrite should re-emit the opener when the stream still needs emission");
            assertTrue(
                    (SessionRuntimeTestSupport.outboundFrame(fin).flags() & Protocol.FRAME_FLAG_FIN) != 0,
                    "closeWrite should still carry FIN on the re-emitted opening frame"
            );
            assertTrue(
                    (SessionRuntimeTestSupport.outboundFrame(fin).flags() & Protocol.FRAME_FLAG_OPEN_METADATA) != 0,
                    "closeWrite should preserve opening metadata on the re-emitted frame"
            );
        }
    }

    @Test
    void peerGoAwayReclaimsCommittedButStillUnseenLocalStream() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();
        stream.queueWrite("body".getBytes(StandardCharsets.UTF_8), 0, "body".length());

        synchronized (runtime.lock()) {
            assertFalse(stream.peerVisible(), "test requires a committed stream that is still unseen by the peer");
            assertTrue(stream.unseenLocalTracked(), "committed unseen stream should remain in the reclaim queue");
        }

        FrameCodec.Frame goAway = new FrameCodec.Frame(
                FrameType.GOAWAY,
                0,
                0L,
                FrameCodec.buildGoAwayPayload(0L, Protocol.MAX_VARINT62, ErrorCode.NO_ERROR.code(), "", Settings.defaults().maxControlPayloadBytes())
        );
        SessionRuntimeTestSupport.invokePrivate(runtime, "handleGoAwayFrame", new Class<?>[]{FrameCodec.Frame.class}, goAway);

        IOException error = assertThrows(IOException.class, () -> stream.write("x".getBytes(StandardCharsets.UTF_8)));
        ApplicationError refused = assertInstanceOf(ApplicationError.class, error);
        assertEquals(ErrorCode.REFUSED_STREAM.code(), refused.code(), "peer GOAWAY should reclaim committed-but-unseen local streams above the watermark");

        synchronized (runtime.lock()) {
            assertFalse(stream.peerVisible(), "reclaimed stream should never become peer-visible");
            assertFalse(stream.unseenLocalTracked(), "reclaimed stream should leave the unseen-local queue");
            assertTrue(stream.fullyTerminalLocked(), "reclaimed stream should become terminal immediately");
        }
    }
}
