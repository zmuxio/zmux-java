package io.zmux.internal.core;

import io.zmux.internal.buffer.CarrierCharge;
import io.zmux.internal.buffer.ReadBuf;
import io.zmux.internal.memory.SharedMemory;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Physical backing for one admitted sending direction. Releases may run off the owner. */
public final class SendBacking {
    public static final int BLOCK_BYTES = 16 << 10;
    /** Buffer owner, byte view, bridge entry, send segment and their array headers. */
    static final int BLOCK_CHARGE = BLOCK_BYTES + 512;
    static final int VIEW_CHARGE = 512;
    private static final int FLOOR_BLOCKS = (int) (Stream.SMALL_HELD / BLOCK_BYTES) + 2;
    static final int PREAMBLE_CHARGE = StreamTable.MAX_PREAMBLE + VIEW_CHARGE;
    static final long FLOOR = (long) FLOOR_BLOCKS * BLOCK_CHARGE + PREAMBLE_CHARGE;

    /** The wait condition for either a prepaid payload block or its optional equivalent. */
    public static final long PAYLOAD_BLOCK = -1;
    private record BackingWait(long bytes, Runnable wake) {}

    private static final int CLOSED = Integer.MIN_VALUE;
    private static final int PREAMBLE = 1 << 8;
    private volatile ConnFlow conn;
    private final SharedMemory.Receive account;
    private final long floor;
    private final long id;
    private final AtomicInteger live = new AtomicInteger();
    private final AtomicReference<BackingWait> waiting = new AtomicReference<>();
    private volatile Runnable retired;
    private final ReadBuf.Account foreign = new ReadBuf.Account() {
        @Override public Object domain() { return account == null ? null : account.domain(); }
        @Override public boolean reserve(long bytes) { return reserveView(bytes); }
        @Override public void release(long bytes) { releaseView(bytes); }
    };

    private final ReadBuf.Account managed = new ReadBuf.Account() {
        @Override public Object domain() { return account.domain(); }
        @Override public boolean reserve(long bytes) { throw new AssertionError("managed backing was prepaid"); }
        @Override public void release(long bytes) {} // Its allocation callback returns the direction's slot.
    };

    SendBacking(ConnFlow conn, SharedMemory.Receive account, long floor, long id) {
        this.conn = conn;
        this.account = account;
        this.floor = floor;
        this.id = id;
        if (account != null) account.claimSendFloor(floor);
    }

    SendHistory newHistory() { return new SendHistory(conn); }

    public ReadBuf block() {
        boolean prepaid = takeFloor(1);
        if (!prepaid && !reserveView(BLOCK_CHARGE)) return null;
        try {
            var root = new ReadBuf(ByteBuffer.allocate(BLOCK_BYTES), ignored -> releaseBlock(prepaid));
            if (account != null) root.prepaid(managed, false);
            return root;
        } catch (RuntimeException | Error failure) {
            releaseBlock(prepaid);
            throw failure;
        }
    }

    ReadBuf preamble(byte[] data) {
        if (data.length + VIEW_CHARGE > PREAMBLE_CHARGE || !takeFloor(PREAMBLE)) {
            throw new IllegalStateException("preamble outside its reserved backing");
        }
        try {
            return new ReadBuf(ByteBuffer.wrap(data), ignored -> retiredBlock(PREAMBLE));
        } catch (RuntimeException | Error failure) {
            retiredBlock(PREAMBLE);
            throw failure;
        }
    }

    private boolean takeFloor(int kind) {
        for (int current = live.get(); current >= 0; current = live.get()) {
            if (!floorRoom(current, kind)) return false;
            if (live.compareAndSet(current, current + kind)) return true;
        }
        return false;
    }

    private static boolean floorRoom(int current, int kind) {
        return kind == PREAMBLE ? (current & PREAMBLE) == 0 : (current & (PREAMBLE - 1)) < FLOOR_BLOCKS;
    }

    private void releaseBlock(boolean prepaid) {
        if (prepaid) retiredBlock(1);
        else releaseView(BLOCK_CHARGE);
    }

    private void retiredBlock(int kind) {
        int left = live.addAndGet(-kind);
        if (left == CLOSED) notifyRetired();
        else if (left >= 0) ready();
    }

    private boolean reserveView(long charge) {
        return live.get() >= 0 && (account == null || account.reserveView(charge));
    }

    private void releaseView(long charge) {
        if (account != null) account.releaseView(charge);
        ready();
    }

    /** Roots are billed through their last reference; independent slices each pay for a descriptor. */
    public boolean reserveSlice(ReadBuf source) {
        if (!reserveView(VIEW_CHARGE)) return false;
        if (source.cover(foreign)) return true;
        releaseView(VIEW_CHARGE);
        return false;
    }

    public void releaseSlice() { releaseView(VIEW_CHARGE); }

    /** Prepay the grant in the target session before entering its send queue. */
    public boolean prepareReceipt(CarrierCharge charge) {
        return live.get() >= 0 && charge.prepare(account);
    }

    /** A return can race registration. The same predicate governs both the probe and eventual wake. */
    public void waitForBacking(long bytes, Runnable wake) {
        waiting.set(new BackingWait(bytes, wake));
        ConnFlow owner = conn;
        if (owner != null) owner.backingWaitChanged(this);
        ready();
    }

    long id() { return id; }

    long waitingBytes() {
        BackingWait wait = waiting.get();
        if (wait == null) return 0;
        return wait.bytes == PAYLOAD_BLOCK ? BLOCK_CHARGE : wait.bytes;
    }

    public boolean blockReady() { return room(PAYLOAD_BLOCK); }

    private boolean room(long bytes) {
        int current = live.get();
        if (current < 0 || account == null) return true;
        if (bytes == PAYLOAD_BLOCK) {
            if (floorRoom(current, 1)) return true;
            bytes = BLOCK_CHARGE;
        }
        return account.viewRoom() >= bytes;
    }

    public void cancelWait() {
        if (waiting.getAndSet(null) == null) return;
        ConnFlow owner = conn;
        if (owner != null) owner.backingWaitChanged(this);
    }

    void ready() {
        BackingWait wait = waiting.get();
        if (wait == null || !room(wait.bytes) || !waiting.compareAndSet(wait, null)) return;
        ConnFlow owner = conn;
        if (owner != null) owner.backingWaitChanged(this);
        wait.wake.run();
    }

    void close(Runnable discarded) {
        if (live.get() < 0) return;
        retired = discarded;
        if (live.getAndUpdate(value -> value | CLOSED) == 0) notifyRetired();
        ready();
        conn = null;
    }

    private void notifyRetired() {
        if (account != null) account.returnSendFloor(floor);
        Runnable notify = retired;
        retired = null;
        notify.run();
    }
}
