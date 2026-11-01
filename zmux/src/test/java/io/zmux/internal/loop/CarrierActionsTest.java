package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.core.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class CarrierActionsTest {
    static final class Carrier extends AbstractTransport {
        final int direction;
        final TransportInfo info;
        boolean stoppedInput, stoppedOutput, closed;
        Carrier(EventLoop loop, int direction, Object connection, long bytes) {
            super(loop); this.direction = direction;
            info = new TransportInfo(new CarrierInfo(Framing.BYTES, false, false, false, false), "", connection, bytes, 0, 32);
        }
        @Override public TransportInfo info() { return info; }
        @Override public int directions() { return direction; }
        @Override public boolean writable() { return false; }
        @Override public boolean closed() { return closed; }
        @Override protected void begin() {}
        @Override protected boolean accepting() { return false; }
        @Override protected void inputFailed(java.io.IOException cause) {}
        @Override protected void send(io.zmux.carrier.OutboundBatch b) { fail("test carrier must not write"); }
        @Override public void shutdownInput() { stoppedInput = true; }
        @Override public void shutdownOutput() { stoppedOutput = true; }
        @Override public void close(Throwable cause) { closed = true; }
    }
    @Test void refusingAnOutboundLaneDoesNotResetTheSameInboundLaneNumber() throws Exception {
        EventLoop loop = new EventLoop("carrier-directions", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                var driver = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                Carrier inbound = new Carrier(loop, Action.DIR_IN, null, 0);
                driver.carriers().attach(inbound, 0, 1, null);
                driver.apply(new Action(Action.Kind.RESET_LANE, 0, 1, Action.DIR_OUT));
                assertFalse(inbound.closed);
                driver.apply(new Action(Action.Kind.CLOSE, 0, 0, Action.DIR_IN));
                assertTrue(inbound.closed);
            });
        } finally { loop.close(); }
    }
    @Test void closingInputLeavesTheBidirectionalOutputUsable() throws Exception {
        EventLoop loop = new EventLoop("carrier-half-close", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                var driver = new SessionRuntime(loop, new Session(CoreConfig.defaults(), Clock.now()), DriverHooks.NONE);
                Carrier carrier = new Carrier(loop, Action.DIR_BIDI, null, 0);
                driver.attachBootstrap(carrier);
                driver.apply(new Action(Action.Kind.CLOSE, 0, 0, Action.DIR_IN));
                assertTrue(carrier.stoppedInput);
                assertFalse(carrier.stoppedOutput); assertFalse(carrier.closed);
                driver.apply(new Action(Action.Kind.SHUTDOWN, 0, 0, Action.DIR_OUT));
                assertTrue(carrier.stoppedOutput);
            });
        } finally { loop.close(); }
    }
    @Test void connectionChargesTrackActualHoldersAndRaiseEstimates() throws Exception {
        EventLoop loop = new EventLoop("carrier-budget", true, BufferPool.DEFAULT_IDLE_BYTES);
        try {
            ExternalTransportTest.run(loop, () -> {
                for (boolean shared : new boolean[] {false, true}) {
                    MemoryBudget budget = shared ? new MemoryBudget(0, null) : null;
                    CoreConfig config = CoreConfig.builder().budget(budget).build();
                    Session core = new Session(config, Clock.now());
                    SessionRuntime driver = new SessionRuntime(loop, core, DriverHooks.NONE);
                    long base = core.memory().usage(); Object connection = new Object();
                    var set = driver.carriers();
                    Carrier a = new Carrier(loop, Action.DIR_IN, connection, 100);
                    Carrier zero = new Carrier(loop, Action.DIR_IN, connection, 0);
                    Carrier b = new Carrier(loop, Action.DIR_OUT, connection, 150);
                    var at = set.attach(a, 0, 0, null); var zt = set.attach(zero, 0, 1, null); set.remove(zt, null);
                    assertEquals(100, shared ? budget.used() : core.memory().usage()-base);
                    var bt = set.attach(b, 0, 0, null); assertSame(bt, set.attach(b, 0, 0, null));
                    assertEquals(150, shared ? budget.used() : core.memory().usage()-base);
                    set.remove(at, null);
                    assertEquals(150, shared ? budget.used() : core.memory().usage()-base);
                    set.remove(bt, null); set.remove(bt, null);
                    assertEquals(0, shared ? budget.used() : core.memory().usage()-base);
                    a = new Carrier(loop, Action.DIR_IN, connection, 100);
                    zero = new Carrier(loop, Action.DIR_IN, connection, 0);
                    at = set.attach(a, 0, 0, null); zt = set.attach(zero, 0, 1, null);
                    set.remove(at, null);
                    assertEquals(100, shared ? budget.used() : core.memory().usage()-base,
                            "a zero estimate does not erase the known connection cost");
                    set.remove(zt, null);
                    assertEquals(0, shared ? budget.used() : core.memory().usage()-base);
                    if (shared) {
                        CoreConfig secondConfig = CoreConfig.builder().budget(budget).build();
                        SessionRuntime second = new SessionRuntime(loop, new Session(secondConfig, Clock.now()), DriverHooks.NONE);
                        a = new Carrier(loop, Action.DIR_IN, connection, 100);
                        zero = new Carrier(loop, Action.DIR_IN, connection, 0);
                        at = set.attach(a, 0, 0, null); zt = second.carriers().attach(zero, 0, 0, null); set.remove(at, null);
                        assertEquals(100, budget.used(), "another session still owns an attachment");
                        second.carriers().remove(zt, null); assertEquals(0, budget.used());
                    }
                }
            });
        } finally { loop.close(); }
    }
    @Test void rejectedLaneImmediatelyDrivesTheProtocolClose() throws Exception {
        EventLoop loop = new EventLoop("lane-admission-close", true, BufferPool.DEFAULT_IDLE_BYTES);
        java.util.concurrent.CompletableFuture<Void> closing = new java.util.concurrent.CompletableFuture<>();
        try {
            ExternalTransportTest.run(loop, () -> {
                CoreConfig config = CoreConfig.defaults();
                Session core = new Session(config, Clock.now());
                SessionRuntime driver = new SessionRuntime(loop, core, DriverHooks.NONE);
                Object connection = new Object();
                Carrier bootstrap = new Carrier(loop, Action.DIR_BIDI, connection, 0);
                driver.attachBootstrap(bootstrap);
                CoreConfig remote = CoreConfig.builder().role(CoreConfig.Role.RESPONDER).transportRole(CoreConfig.Role.RESPONDER).build();
                Session peer = new Session(remote, Clock.now());
                peer.pipes().attachBootstrap(bootstrap.info().carrier(), bootstrap.info().carrier(), null);
                Batch batch = new Batch();
                assertTrue(peer.pollTransmit(0, 0, batch, Session.DEFAULT_BATCH, Clock.now()));
                java.nio.ByteBuffer bytes = java.nio.ByteBuffer.wrap(io.zmux.internal.core.BatchFixture.flat(batch));
                ReadBuf input = new ReadBuf(bytes, null);
                core.receive(0, 0, input, bytes, 0, bytes.limit(), Clock.now());
                input.unref(); batch.discard();
                assertTrue(core.established());
                driver.resourceListener(view -> { if (view.state() == SessionState.CLOSING) closing.complete(null); });
                // Let the attach/peer-parameter round settle before the invalid admission.
                loop.execute(() -> {
                    java.nio.ByteBuffer body = java.nio.ByteBuffer.allocate(64);
                    io.zmux.internal.wire.FrameEncoder.pipeLane(body, core.lanes().key(), 0, 0);
                    assertNull(driver.acceptLane(new Carrier(loop, Action.DIR_IN, connection, 0),
                            java.util.Arrays.copyOf(body.array(), body.position())));
                });
            });
            closing.get(5, java.util.concurrent.TimeUnit.SECONDS);
        } finally { loop.close(); }
    }

}
