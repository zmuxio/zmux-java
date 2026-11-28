package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.core.Action;
import io.zmux.internal.join.Mode;
import io.zmux.internal.join.Opener;
import io.zmux.internal.join.Result;
import io.zmux.internal.join.Sealer;
import io.zmux.internal.join.Suite;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class SealedInputPoolTest {
    private static final class Host extends SessionHost {
        Host() { super(new BufferPool(BufferPool.DEFAULT_IDLE_BYTES)); }
        @Override public boolean inLoop() { return true; }
        @Override public boolean tryExecute(Runnable task) { task.run(); return true; }
        @Override protected void requestRoundEnd() {}
        @Override protected void wakeAt(long deadline) {}
    }

    private static final class Incoming implements Inbound {
        private int records;
        private boolean ended;
        private ByteBuffer backing;
        private int expected;

        @Override public boolean session() { return true; }

        @Override public int bytes(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            throw new AssertionError("sealed records must reach an attached session as messages");
        }

        @Override public void message(Transport t, ReadBuf buf, ByteBuffer data, int pos, int lim) {
            assertSame(buf.data(), data);
            assertEquals(expected, lim - pos);
            assertEquals(0, pos);
            assertEquals(lim, data.limit());
            assertEquals(42, data.get(pos));
            if (backing != null) assertSame(backing, data);
            backing = data;
            records++;
        }

        @Override public void ended(Transport t, boolean graceful, boolean partial) {
            assertFalse(graceful);
            ended = true;
        }
    }

    @Test void authenticatedPlaintextReusesItsFinalBackingWithBothCiphers() throws Exception {
        for (Suite suite : List.of(Suite.AES, Suite.CHACHA)) {
            Host host = new Host();
            var setup = new CarrierSetup();
            var lower = setup.own(() -> new CarrierActionsTest.Carrier(host, Action.DIR_BIDI, null, 0));
            byte[] key = new byte[suite.keyLen()];
            Sealer sender = Sealer.create(suite, key, 0);
            var receiver = receiver(lower, suite, key);
            var in = new Incoming();
            receiver.start(in);
            ByteBuffer wire = ByteBuffer.allocate(2 + SealedTransport.MAX_RECORD + Suite.TAG_LEN);
            ReadBuf encrypted = new ReadBuf(wire, null);
            int sent = 0;
            for (int size : new int[] {1, 4095, 4097, SealedTransport.MAX_RECORD}) {
                in.expected = size;
                in.backing = null;
                ByteBuffer plaintext = ByteBuffer.allocate(size);
                plaintext.put(0, (byte) 42);
                for (int i = 0; i < 16; i++) {
                    wire.clear().putShort((short) (size + Suite.TAG_LEN));
                    sender.seal(plaintext.rewind(), wire);
                    wire.flip();
                    assertEquals(wire.limit(), receiver.bytes(lower, encrypted, wire, 0, wire.limit()));
                    assertEquals(0, host.pool.outstanding());
                    sent++;
                }
            }
            assertEquals(sent, in.records);
            assertEquals(4, host.pool.allocations());
            assertEquals(256 + 4096 + 8192 + 65536, host.pool.idleBytes());
            wire.clear().putShort((short) (1 + Suite.TAG_LEN)).put(new byte[1 + Suite.TAG_LEN]).flip();
            assertEquals(Inbound.GONE, receiver.bytes(lower, encrypted, wire, 0, wire.limit()));
            encrypted.unref();
            assertTrue(in.ended);
            assertEquals(sent, in.records);
            assertEquals(4, host.pool.allocations());
            assertEquals(0, host.pool.outstanding());
        }
    }

    @Test void aSplitSealedHeaderAndPayloadUseTheCiphertextLength() throws Exception {
        Host host = new Host();
        var setup = new CarrierSetup();
        var lower = setup.own(() -> new CarrierActionsTest.Carrier(host, Action.DIR_BIDI, null, 0) {
            @Override protected void begin() { openReader(); }
            @Override protected boolean accepting() { return !closed; }
        });
        Suite suite = Suite.AES;
        byte[] key = new byte[suite.keyLen()];
        var receiver = receiver(lower, suite, key);
        var in = new Incoming();
        in.expected = 1024;
        receiver.start(in);
        ByteBuffer plaintext = ByteBuffer.allocate(in.expected).put(0, (byte) 42);
        int ciphertextLength = in.expected + Suite.TAG_LEN;
        ByteBuffer wire = ByteBuffer.allocate(2 + ciphertextLength).putShort((short) ciphertextLength);
        Sealer.create(suite, key, 0).seal(plaintext, wire);
        wire.flip();
        var encrypted = new ReadBuf(wire, null);
        try {
            assertEquals(1, receiver.missingRecordBytes(wire, 0, 1));
            lower.input(wire.limit(1), encrypted);
            assertEquals(0, in.records);
            assertEquals(ciphertextLength - 9, receiver.missingRecordBytes(wire.limit(11), 0, 11));
            lower.input(wire, encrypted);
            assertEquals(0, in.records);
            lower.input(wire.limit(wire.capacity()), encrypted);
            assertEquals(1, in.records);
            assertEquals(wire.capacity(), host.copiedInputBytes());
            assertEquals(2, host.pool.allocations());
            assertEquals(0, host.pool.outstanding());
        } finally {
            encrypted.unref();
        }
    }

    @Test void ciphertextWorkspaceWaitsForTheLastProviderPin() throws Exception {
        Host host = new Host();
        var physicalClose = new java.util.concurrent.CompletableFuture<Void>();
        var held = new java.util.concurrent.atomic.AtomicReference<io.zmux.carrier.OutboundBatch.Retained>();
        var setup = new CarrierSetup();
        var lower = setup.own(() -> new CarrierActionsTest.Carrier(host, Action.DIR_BIDI, null, 0) {
            @Override public boolean writable() { return !closed && !writing(); }
            @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return physicalClose; }
            @Override protected void send(io.zmux.carrier.OutboundBatch batch) {
                held.set(batch.retain());
                writeDone(null);
            }
            @Override void closePhysical(CarrierSetup.Bound.Permit permit, Throwable cause) {
                super.closePhysical(permit, cause);
                physicalClose.complete(null);
            }
        });
        long cap = SealedTransport.workspace(Action.DIR_BIDI) + CarrierCapacity.OUTBOUND_WORKSPACE;
        var budget = new io.zmux.internal.memory.MemoryBudget(cap, null);
        // Join reserves the lower carrier before the Sealed layer is created.
        assertTrue(CarrierCapacity.reserve(lower, budget, true));
        var sealed = receiver(lower, Suite.AES, new byte[Suite.AES.keyLen()]);
        var outcomes = new java.util.ArrayList<Boolean>();
        sealed.start(new Inbound() {
            @Override public int bytes(Transport t, ReadBuf b, ByteBuffer data, int from, int end) { return end - from; }
            @Override public void ended(Transport t, boolean graceful, boolean partial) {}
            @Override public void writable(Transport t, boolean on) { outcomes.add(on); }
            @Override public void written(Transport t, io.zmux.internal.core.Batch batch, int confirmed, Throwable cause) {
                assertNull(cause);
                assertEquals(batch.bytes(), confirmed);
                batch.discard();
            }
        });
        var batch = new io.zmux.internal.core.Batch();
        batch.wrap(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap(new byte[] {42})));
        sealed.write(batch);
        assertEquals(java.util.List.of(false), outcomes);
        assertFalse(sealed.writable());
        held.get().close();
        assertEquals(java.util.List.of(false, true), outcomes);
        assertTrue(sealed.writable());

        batch.wrap(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap(new byte[] {43})));
        sealed.write(batch);
        CarrierSetup.connectionLost(sealed, null);
        assertTrue(physicalClose.isDone());
        assertEquals(cap, budget.used());
        held.get().close();
        assertEquals(0, budget.used());
    }

    @Test void shortWritesKeepTheSealedSuffixAndConsumeOneNoncePerRecord() throws Exception {
        var host = new Host();
        var setup = new CarrierSetup();
        var writes = new java.util.ArrayList<io.zmux.carrier.OutboundBatch>();
        var carrierClosed = new java.util.concurrent.CompletableFuture<Void>();
        var lower = setup.own(() -> new CarrierActionsTest.Carrier(host, Action.DIR_BIDI, null, 0) {
            @Override public boolean writable() { return !writing(); }
            @Override public java.util.concurrent.CompletionStage<Void> whenClosed() { return carrierClosed; }
            @Override protected void send(io.zmux.carrier.OutboundBatch batch) { writes.add(batch); }
        });
        long cap = SealedTransport.workspace(Action.DIR_BIDI) + CarrierCapacity.OUTBOUND_WORKSPACE;
        assertTrue(CarrierCapacity.reserve(lower, new io.zmux.internal.memory.MemoryBudget(cap, null), true));
        byte[] key = new byte[Suite.AES.keyLen()];
        var reference = Sealer.create(Suite.AES, key, 0);
        var sealed = receiver(lower, Suite.AES, key);
        var completions = new java.util.ArrayList<Integer>();
        sealed.start(new Inbound() {
            @Override public int bytes(Transport t, ReadBuf b, ByteBuffer data, int from, int end) { return end - from; }
            @Override public void ended(Transport t, boolean graceful, boolean partial) {}
            @Override public void written(Transport t, io.zmux.internal.core.Batch b, int confirmed, Throwable cause) {
                assertNull(cause);
                completions.add(confirmed);
                b.discard();
            }
        });
        for (int nonce = 0; nonce < 2; nonce++) {
            byte[] plain = new byte[60];
            plain[0] = (byte) nonce;
            byte[] cipher = reference.seal(plain, 0, plain.length);
            ByteBuffer expected = ByteBuffer.allocate(2 + cipher.length).putShort((short) cipher.length).put(cipher).flip();
            var b = new io.zmux.internal.core.Batch();
            b.wrap(io.zmux.carrier.OutboundBatch.of(ByteBuffer.wrap(plain)));
            sealed.write(b);
            java.util.Arrays.fill(plain, (byte) 91);
            var frozen = writes.get(nonce);
            for (int prefix : new int[] {0, 1, 22, 22, 63, expected.limit()}) {
                lower.writeProgress(prefix);
                assertEquals(expected.slice(prefix, expected.limit() - prefix),
                        frozen.segment(0).slice(prefix, expected.limit() - prefix));
                assertEquals(nonce + 1, writes.size());
            }
            lower.writeDone(null);
        }
        assertEquals(List.of(60, 60), completions);
        carrierClosed.complete(null);
    }

    @Test void defaultHostsShareOneIdleCapacityLimit() {
        var first = BufferPool.forHost(BufferPool.DEFAULT_IDLE_BYTES);
        var second = BufferPool.forHost(BufferPool.DEFAULT_IDLE_BYTES);
        var blocks = new ReadBuf[Math.toIntExact(BufferPool.DEFAULT_IDLE_BYTES / BufferPool.SIZE) + 1];
        for (int i = 0; i < blocks.length; i++) blocks[i] = (i % 2 == 0 ? first : second).take();
        for (ReadBuf block : blocks) block.unref();
        assertEquals(first.idleBytes(), second.idleBytes());
        assertTrue(first.idleBytes() <= BufferPool.DEFAULT_IDLE_BYTES);
        assertEquals(0, first.outstanding());
        assertEquals(0, second.outstanding());
    }

    @Test void aFullCacheOfBatchBlocksCanWarmItsSmallRecordClass() {
        var pool = new BufferPool(BufferPool.SIZE);
        pool.take().unref();
        assertEquals(BufferPool.SIZE, pool.idleBytes());
        var small = pool.takeRecord(256);
        ByteBuffer backing = small.data();
        small.unref();
        assertEquals(256, pool.idleBytes());
        small = pool.takeRecord(256);
        try {
            assertSame(backing, small.data());
            assertEquals(2, pool.allocations());
        } finally {
            small.unref();
        }
    }

    private static SealedTransport receiver(Transport lower, Suite suite, byte[] key) throws Exception {
        return new SealedTransport(lower, new Result(Mode.SEALED, suite,
                Sealer.create(suite, key, 0), Opener.create(suite, key, 0), key));
    }
}
