package io.zmux.runtime;

import io.zmux.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

final class EventSurfaceRuntimeTest {
    private static void queueOpenedEvent(SessionRuntime runtime, StreamRuntime stream) throws IOException {
        synchronized (runtime.lock()) {
            runtime.beginLocalOpenLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            runtime.markPeerVisibleLocked(stream);
        }
    }

    @Test
    void openingCommitDefersOpenedEventUntilPeerVisible() throws Exception {
        List<ZmuxEvent> events = new ArrayList<>();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder().eventHandler(event -> events.add(event)).build(),
                0L,
                Settings.defaults()
        );
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            runtime.beginLocalOpenLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            assertTrue(stream.openedOnWire(), "opening-frame commit should consume the local stream ID");
            assertFalse(stream.peerVisibleLocked(), "opening-frame commit should not by itself make the stream peer-visible");
            assertFalse(stream.openedEventSentLocked(), "opened event should stay deferred until the stream becomes peer-visible");
        }

        runtime.emitPendingEvents();
        assertTrue(events.isEmpty(), "opening-frame commit alone should not emit stream_opened");

        synchronized (runtime.lock()) {
            runtime.markPeerVisibleLocked(stream);
            assertTrue(stream.peerVisibleLocked(), "markPeerVisibleLocked should advance local-open visibility");
            assertTrue(stream.openedEventSentLocked(), "opened-event flag should be set once the event is queued");
        }

        runtime.emitPendingEvents();
        assertEquals(1, events.size(), "peer-visible transition should queue exactly one opened event");
        assertEquals(ZmuxEventType.STREAM_OPENED, events.get(0).type(), "queued event type mismatch");
        assertEquals(stream.streamIdInternal(), events.get(0).streamId(), "queued event stream ID mismatch");
        assertFalse(
                events.get(0).stream() instanceof ZmuxNativeStream,
                "event stream info must not leak a live native stream control surface"
        );
    }

    @Test
    void openingCommitLeavesOpenedFlagUnsetWithoutHandler() throws Exception {
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(0L, Settings.defaults());
        StreamRuntime stream = (StreamRuntime) runtime.openStream();

        synchronized (runtime.lock()) {
            runtime.beginLocalOpenLocked(stream);
            runtime.markLocalStreamOpeningCommittedLocked(stream);
            assertTrue(stream.openedOnWire(), "opening-frame commit should still consume the local stream ID without a handler");
            assertFalse(stream.peerVisibleLocked(), "opening-frame commit should not advance peer visibility without a handler");
            assertFalse(stream.openedEventSentLocked(), "opened-event flag should remain unset when no handler is configured");
            runtime.markPeerVisibleLocked(stream);
            assertTrue(stream.peerVisibleLocked(), "peer-visible transition should still be tracked without a handler");
        }

        runtime.emitPendingEvents();
        assertFalse(stream.openedEventSentLocked(), "opened-event flag should remain unset after draining without a handler");
    }

    @Test
    void acceptedEventRequiresAcceptedState() throws Exception {
        List<ZmuxEvent> events = new ArrayList<>();
        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder().eventHandler(events::add).build(),
                0L,
                Settings.defaults()
        );

        synchronized (runtime.lock()) {
            StreamRuntime stream = runtime.createPeerOpenedStreamLocked(
                    SessionRuntime.firstPeerStreamId(Role.RESPONDER, true)
            );
            stream.setApplicationVisibleLocked(true);
            runtime.enqueueStreamEventLocked(stream, ZmuxEventType.STREAM_ACCEPTED, null);
            assertFalse(stream.acceptedEventSentLocked(), "accepted event must stay deferred until the stream is accepted");
        }

        runtime.emitPendingEvents();
        assertTrue(events.isEmpty(), "application-visible peer stream should not emit accepted before accept returns it");
    }

    @Test
    void eventDispatcherSuppressesHandlerRuntimeExceptionsButPropagatesJvmErrors() {
        AtomicInteger runtimeCalls = new AtomicInteger();
        Object runtimeLock = new Object();
        SessionEventDispatcher runtimeDispatcher = new SessionEventDispatcher(runtimeLock, event -> {
            runtimeCalls.incrementAndGet();
            throw new IllegalStateException("handler failed");
        });
        synchronized (runtimeLock) {
            runtimeDispatcher.enqueueSessionClosedEventLocked(SessionState.CLOSED, null);
        }

        runtimeDispatcher.emitPendingEvents();
        assertEquals(1, runtimeCalls.get(), "runtime handler failure should be observed once and suppressed");

        AssertionError error = new AssertionError("fatal handler failure");
        Object errorLock = new Object();
        SessionEventDispatcher errorDispatcher = new SessionEventDispatcher(errorLock, event -> {
            throw error;
        });
        synchronized (errorLock) {
            errorDispatcher.enqueueSessionClosedEventLocked(SessionState.CLOSED, null);
        }

        AssertionError thrown = assertThrows(
                AssertionError.class,
                errorDispatcher::emitPendingEvents,
                "JVM Error subclasses must not be swallowed by event delivery"
        );
        assertEquals(error, thrown, "event dispatcher should rethrow the original JVM Error instance");
    }

    @Test
    void eventDispatcherSerializesConcurrentEmittersWithoutDroppingQueuedEvents() throws Exception {
        CountDownLatch firstHandlerEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstHandler = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger activeHandlers = new AtomicInteger();
        AtomicInteger maxActiveHandlers = new AtomicInteger();
        AtomicReference<Throwable> emitterFailure = new AtomicReference<>();
        CountDownLatch secondEmitterReturned = new CountDownLatch(1);

        SessionRuntime runtime = SessionRuntimeTestSupport.newReadyRuntime(
                ZmuxConfig.builder().eventHandler(event -> {
                    int currentActive = activeHandlers.incrementAndGet();
                    maxActiveHandlers.accumulateAndGet(currentActive, Math::max);
                    int call = calls.incrementAndGet();
                    try {
                        if (call == 1) {
                            firstHandlerEntered.countDown();
                            assertTrue(
                                    releaseFirstHandler.await(2, TimeUnit.SECONDS),
                                    "first event handler should be released by the test"
                            );
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    } finally {
                        activeHandlers.decrementAndGet();
                    }
                }).build(),
                0L,
                Settings.defaults()
        );

        StreamRuntime first = (StreamRuntime) runtime.openStream();
        queueOpenedEvent(runtime, first);
        Thread firstEmitter = new Thread(() -> {
            try {
                runtime.emitPendingEvents();
            } catch (Throwable failure) {
                emitterFailure.compareAndSet(null, failure);
            }
        }, "event-serial-first");
        firstEmitter.start();
        assertTrue(firstHandlerEntered.await(2, TimeUnit.SECONDS), "first emitter should enter the handler");

        StreamRuntime second = (StreamRuntime) runtime.openStream();
        queueOpenedEvent(runtime, second);
        Thread secondEmitter = new Thread(() -> {
            try {
                runtime.emitPendingEvents();
            } catch (Throwable failure) {
                emitterFailure.compareAndSet(null, failure);
            } finally {
                secondEmitterReturned.countDown();
            }
        }, "event-serial-second");
        secondEmitter.start();
        assertTrue(
                secondEmitterReturned.await(500, TimeUnit.MILLISECONDS),
                "non-empty emitPendingEvents call should not wait behind an active handler"
        );

        releaseFirstHandler.countDown();
        firstEmitter.join(TimeUnit.SECONDS.toMillis(2));
        secondEmitter.join(TimeUnit.SECONDS.toMillis(2));

        assertFalse(firstEmitter.isAlive(), "first emitter should finish");
        assertFalse(secondEmitter.isAlive(), "second emitter should finish");
        assertNull(emitterFailure.get(), "emitters should not fail");
        assertEquals(2, calls.get(), "both queued events should be delivered");
        assertEquals(1, maxActiveHandlers.get(), "event handlers must not run concurrently for one session");
    }

    @Test
    void eventDispatcherDoesNotBlockEmptyEmitterBehindSlowHandler() throws Exception {
        Object lock = new Object();
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        CountDownLatch emptyEmitterReturned = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        SessionEventDispatcher dispatcher = new SessionEventDispatcher(lock, event -> {
            handlerEntered.countDown();
            try {
                assertTrue(
                        releaseHandler.await(2, TimeUnit.SECONDS),
                        "handler should be released by the test"
                );
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        });
        synchronized (lock) {
            dispatcher.enqueueSessionClosedEventLocked(SessionState.CLOSED, null);
        }

        Thread activeEmitter = new Thread(() -> {
            try {
                dispatcher.emitPendingEvents();
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
            }
        }, "event-empty-active");
        activeEmitter.start();
        assertTrue(handlerEntered.await(2, TimeUnit.SECONDS), "active emitter should enter handler");

        Thread emptyEmitter = new Thread(() -> {
            try {
                dispatcher.emitPendingEvents();
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
            } finally {
                emptyEmitterReturned.countDown();
            }
        }, "event-empty-fast-path");
        emptyEmitter.start();

        assertTrue(
                emptyEmitterReturned.await(500, TimeUnit.MILLISECONDS),
                "empty emitPendingEvents call should not wait behind an active handler"
        );
        releaseHandler.countDown();
        activeEmitter.join(TimeUnit.SECONDS.toMillis(2));
        emptyEmitter.join(TimeUnit.SECONDS.toMillis(2));

        assertFalse(activeEmitter.isAlive(), "active emitter should finish");
        assertFalse(emptyEmitter.isAlive(), "empty emitter should finish");
        assertNull(failure.get(), "event emitters should not fail");
    }
}
