package io.zmux;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

final class SessionAcceptRegistryTest {
    private static SessionAcceptRegistry acceptRegistry(SessionRuntime runtime) throws Exception {
        Field field = SessionRuntime.class.getDeclaredField("acceptRegistry");
        field.setAccessible(true);
        return (SessionAcceptRegistry) field.get(runtime);
    }

    private static Object acceptBidiQueue(SessionAcceptRegistry registry) throws Exception {
        Field field = SessionAcceptRegistry.class.getDeclaredField("acceptBidi");
        field.setAccessible(true);
        return field.get(registry);
    }

    @Test
    void visibilitySequenceUsesUnsignedOrderingAcrossSignedWrap() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            SessionAcceptRegistry registry = acceptRegistry(runtime);
            SessionRuntimeTestSupport.setLongField(registry, "nextVisibilitySequence", Long.MAX_VALUE - 1L);
            StreamRuntime oldUni = runtime.createPeerOpenedStreamLocked(SessionRuntime.firstPeerStreamId(Role.RESPONDER, false));
            StreamRuntime newBidi = runtime.createPeerOpenedStreamLocked(SessionRuntime.firstPeerStreamId(Role.RESPONDER, true));

            registry.enqueueAcceptedLocked(oldUni);
            registry.enqueueAcceptedLocked(newBidi);

            assertEquals(Long.MAX_VALUE, oldUni.visibilitySequence(), "first stream should sit at the signed positive edge");
            assertEquals(Long.MIN_VALUE, newBidi.visibilitySequence(), "second stream should wrap into the unsigned upper half");

            StreamRuntime newest = (StreamRuntime) SessionRuntimeTestSupport.invokePrivate(
                    registry,
                    "pollNewestAcceptedLocked",
                    new Class<?>[0]
            );
            assertSame(newBidi, newest, "visible accept shedding must not reverse at the signed long boundary");
        }
    }

    @Test
    void terminalQueuedStreamPreservesOpenInfoUntilAccept() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                Protocol.CAPABILITY_OPEN_METADATA,
                Settings.defaults()
        );
        byte[] openInfo = "ssh".getBytes(StandardCharsets.UTF_8);
        StreamRuntime stream;

        synchronized (runtime.lock()) {
            stream = runtime.createPeerOpenedStreamLocked(SessionRuntime.firstPeerStreamId(Role.RESPONDER, true));
            stream.applyOpenMetadataLocked(0L, null, openInfo);
            runtime.enqueueAcceptedLocked(stream);
            stream.abortFromLocalLocked(ErrorCode.CANCELLED.code(), "");
            assertTrue(stream.fullyTerminalLocked(), "test requires a terminal stream");

            runtime.maybeCompactStreamLocked(stream);

            assertSame(stream, runtime.liveStreamLocked(stream.streamIdInternal()), "queued stream must stay live before accept");
            assertEquals(1, runtime.pendingAcceptedCountLocked(), "accept queue must retain the terminal stream");
            assertEquals(openInfo.length, runtime.retainedOpenInfoBytesLocked(), "open_info must stay charged before accept");
            assertArrayEquals(openInfo, stream.openInfo(), "open_info must stay visible before accept");
        }

        ZmuxNativeStream accepted = runtime.acceptStream();

        assertSame(stream, accepted, "accept should return the queued terminal stream");
        assertArrayEquals(openInfo, accepted.openInfo(), "open_info must stay visible to the accepting application");
        synchronized (runtime.lock()) {
            assertEquals(0, runtime.pendingAcceptedCountLocked(), "accept queue should be empty after accept");
            assertEquals(0L, runtime.retainedOpenInfoBytesLocked(), "accepted terminal stream should release session open_info budget");
        }
    }

    @Test
    void drainedLargeAcceptQueueReleasesDequeStorage() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());

        synchronized (runtime.lock()) {
            SessionAcceptRegistry registry = acceptRegistry(runtime);
            long streamId = SessionRuntime.firstPeerStreamId(Role.RESPONDER, true);
            for (int i = 0; i < 1100; i++) {
                StreamRuntime stream = runtime.createPeerOpenedStreamLocked(streamId + i * 4L);
                registry.enqueueAcceptedLocked(stream);
            }
            Object retainedQueue = acceptBidiQueue(registry);

            for (int i = 0; i < 1100; i++) {
                assertNotNull(registry.pollAcceptedHeadLocked(true), "accept queue should contain the seeded stream");
            }

            assertEquals(0, registry.pendingAcceptedCountLocked(), "accept queue should be empty after draining");
            assertNotSame(retainedQueue, acceptBidiQueue(registry),
                    "drained oversized accept queue should release ArrayDeque backing");
        }
    }
}
