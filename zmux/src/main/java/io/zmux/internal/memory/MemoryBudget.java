package io.zmux.internal.memory;

import io.zmux.internal.buffer.ReceiveSizes;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.TreeMap;

/**
 * A memory budget shared by the sessions of a listener, including the windows
 * reserved before publishing flow-control credit. Pressure starts at the limit and ends below
 * three quarters of it, and each transition runs {@link #onChange} (which must only
 * hand the work to a serial context).
 */
public final class MemoryBudget implements SharedMemory {
    private final Object lock = new Object();
    /** Changed by {@link #limit(long)}; every reader reads it here. */
    private volatile long limit;
    // Writes hold the budget lock; pressure and room estimates can read without taking it.
    private volatile long used;
    private volatile boolean over;
    private final Runnable onChange;
    private TreeMap<Long, LinkedHashSet<ReceiveAccount>> waiting;
    private final LinkedHashSet<ReceiveAccount> viewCaches = new LinkedHashSet<>();
    private volatile long firstWaiting = Long.MAX_VALUE;
    private final java.util.IdentityHashMap<Object, CarrierCharge> carriers = new java.util.IdentityHashMap<>();
    private static final class CarrierCharge { long bytes; int refs; }

    /** A larger declaration covers the connection until its last carrier is physically released. */
    @Override
    public Runnable acquireCarrier(Object connection, long bytes, long privateBytes) {
        if (bytes < 0 || privateBytes < 0) throw new IllegalArgumentException("negative carrier capacity");
        boolean changed;
        boolean admitted;
        synchronized (lock) {
            var held = carriers.get(connection);
            long extra = Math.max(0, bytes - (held == null ? 0 : held.bytes));
            admitted = extra <= Long.MAX_VALUE - privateBytes && privateBytes + extra <= roomFor(privateBytes + extra);
            if (admitted) {
                if (held == null) {
                    held = new CarrierCharge();
                    carriers.put(connection, held);
                }
                held.refs++;
                held.bytes += extra;
                used += extra + privateBytes;
            }
            changed = updatePressure();
        }
        signal(changed);
        if (!admitted) return null;
        var released = new AtomicBoolean();
        return () -> {
            if (!released.compareAndSet(false, true)) return;
            boolean returned;
            synchronized (lock) {
                var held = carriers.get(connection);
                used -= privateBytes;
                if (--held.refs == 0) {
                    carriers.remove(connection);
                    used -= held.bytes;
                }
                returned = updatePressure();
            }
            signal(returned);
        };
    }

    /** @param limit bytes, {@link SharedMemory#UNLIMITED} = unlimited */
    public MemoryBudget(long limit, Runnable onChange) {
        this.limit = limit;
        this.onChange = onChange;
    }

    @Override public long limit() { return limit; }

    /**
     * Replaces the limit ({@link SharedMemory#UNLIMITED} = unlimited) and
     * starts or ends pressure at once, as if the charged sum had crossed the new bounds.
     */
    public void limit(long limit) {
        boolean changed;
        boolean refused;
        synchronized (lock) {
            if (limit < used) reclaimViews();
            refused = limit < used;
            if (!refused) this.limit = limit;
            changed = updatePressure();
        }
        signal(changed);
        if (refused) throw new IllegalArgumentException("memory budget below committed backing capacity");
    }

    @Override public long used() { synchronized (lock) { return used; } }

    @Override public boolean pressure() { return over; }

    @Override public boolean tryCharge(long bytes) { return reserve(bytes, bytes, 1) >= 0; }

    @Override
    public void release(long bytes) {
        if (bytes == 0) return;
        if (bytes < 0) throw new IllegalArgumentException("negative release");
        boolean changed;
        synchronized (lock) {
            used -= bytes;
            changed = updatePressure();
        }
        signal(changed);
    }

    private long reserve(long minimum, long wanted, long cost) {
        if (minimum < 0 || wanted < minimum || cost <= 0) throw new IllegalArgumentException("invalid reservation");
        if (wanted == 0) return 0;
        boolean changed;
        long granted;
        synchronized (lock) {
            granted = Math.min(wanted, roomFor(unitBytes(wanted, cost)) / cost);
            if (granted < minimum) granted = -1;
            else used += granted * cost;
            changed = updatePressure();
        }
        signal(changed);
        return granted;
    }

    @Override
    public Receive reserveReceive(long minimum, long wanted, long cost, long working, long loanFloor, long receive, long send) {
        long granted;
        boolean changed;
        synchronized (lock) {
            long fixed = working + receive + send;
            long room = roomFor(fixed + unitBytes(wanted, cost, Long.MAX_VALUE - fixed));
            granted = fixed > room ? -1 : Math.min(wanted, (room - fixed) / cost);
            if (granted < minimum) granted = -1;
            else used += fixed + granted * cost;
            changed = updatePressure();
        }
        signal(changed);
        return granted < 0 ? null : new ReceiveAccount(granted, cost, working, loanFloor, receive, send);
    }

    private long room() { long bound = limit; return bound - Math.min(bound, used); }

    private static long unitBytes(long units, long unit) {
        return unitBytes(units, unit, Long.MAX_VALUE);
    }

    private static long unitBytes(long units, long unit, long cap) {
        return units > cap / unit ? cap : units * unit;
    }

    /** With the budget locked, makes idle view credit available to every account. */
    private long roomFor(long bytes) {
        long available = room();
        if (available >= bytes || viewCaches.isEmpty()) return available;
        reclaimViews();
        return room();
    }

    @SuppressWarnings("NonAtomicVolatileUpdate") // roomFor and limit changes hold the budget monitor
    private void reclaimViews() {
        for (var account : viewCaches) {
            long spare = account.spareViews.getAndSet(ReceiveAccount.RECLAIMING_VIEWS);
            used -= spare;
            account.optionalPaid -= spare;
        }
        for (var account : viewCaches) account.spareViews.set(0);
    }

    private final class ReceiveAccount implements Receive {
        private static final long CLOSED_VIEWS = -1, RECLAIMING_VIEWS = -2;
        private static final long VIEW_CACHE = 2L * ReceiveSizes.MAX_BLOCK;
        private final long cost;
        private final long loanFloor;
        /** Paid spare space. Returns on application threads need no budget lock in the common case. */
        private final AtomicLong spareViews = new AtomicLong();
        private long working;
        private long credit;
        private long receiveStreams;
        private long sendPromised;
        private long sendLive;
        private long retained;
        private long optionalPaid;
        private long loaned;
        private boolean closed;
        private volatile boolean creditWithheld;
        private long waitingFor = Long.MAX_VALUE;
        private long commitmentWait = Long.MAX_VALUE;
        private long backingWait = Long.MAX_VALUE;
        private Runnable roomReady;

        ReceiveAccount(long credit, long cost, long working, long loanFloor, long receive, long send) {
            this.credit = credit;
            this.cost = cost;
            this.working = working;
            this.loanFloor = loanFloor;
            this.receiveStreams = receive;
            this.sendPromised = send;
        }

        @Override public Object domain() { return MemoryBudget.this; }

        @Override public long window() {
            synchronized (lock) { return credit; }
        }

        @Override public long commit(long floor, long wanted) {
            long granted;
            boolean changed;
            synchronized (lock) {
                if (closed) return 0;
                long previous = charged();
                long excess = Math.max(0, retained - receiveStreams - working - Math.max(0, loaned - loanFloor));
                long retainedCredit = excess == 0 ? 0 : (excess - 1) / cost + 1;
                wanted = Math.max(Math.max(floor, retainedCredit), wanted);
                credit = wanted <= credit ? wanted : credit + payable(wanted - credit, cost);
                granted = credit;
                changed = book(previous);
                creditWithheld = granted < wanted;
                if (creditWithheld) waitFor(cost);
            }
            signal(changed);
            return granted;
        }

        @Override public long reserveStreams(long wanted, long receive, long send) {
            long granted;
            boolean changed;
            synchronized (lock) {
                if (closed) return 0;
                long previous = charged();
                long unit = receive + send;
                granted = payable(wanted, unit);
                receiveStreams += granted * receive;
                sendPromised += granted * send;
                changed = book(previous);
                if (granted < wanted) waitFor(unit);
            }
            signal(changed);
            return granted;
        }

        @Override public void releaseStream(long receive, long send) {
            boolean changed;
            synchronized (lock) {
                if (closed) return;
                long previous = charged();
                receiveStreams -= receive;
                sendPromised -= send;
                changed = book(previous);
            }
            signal(changed);
        }

        @Override public void claimSendFloor(long bytes) {
            synchronized (lock) {
                if (closed || bytes > sendPromised - sendLive) {
                    throw new IllegalStateException("send direction has no prepaid floor");
                }
                sendLive += bytes;
            }
        }

        @Override public void returnSendFloor(long bytes) {
            boolean changed;
            synchronized (lock) {
                long previous = charged();
                sendLive -= bytes;
                changed = book(previous);
            }
            signal(changed);
        }

        @Override public void onRoom(Runnable ready) {
            synchronized (lock) {
                roomReady = ready;
                if (waitingFor != Long.MAX_VALUE) insertWaiting(this);
            }
            signal(false);
        }

        private void waitFor(long bytes) {
            if (closed || commitmentWait <= bytes) return;
            commitmentWait = bytes;
            updateWaiting();
        }

        private void updateWaiting() {
            long threshold = Math.min(commitmentWait, backingWait);
            if (waitingFor == threshold) return;
            removeWaiting(this);
            waitingFor = threshold;
            if (threshold != Long.MAX_VALUE && roomReady != null) insertWaiting(this);
        }

        @Override public void waitForBacking(java.util.function.LongSupplier minimum) {
            boolean changed;
            synchronized (lock) {
                if (closed) return;
                backingWait = minimum.getAsLong();
                updateWaiting();
                if (backingWait != Long.MAX_VALUE) roomFor(backingWait);
                changed = updatePressure();
            }
            signal(changed);
        }

        @Override public boolean pressure() {
            return MemoryBudget.this.pressure() || (creditWithheld && room() < cost);
        }

        @Override public long reservedBytes() {
            synchronized (lock) { return charged() + optionalPaid; }
        }

        @Override public long viewRoom() {
            long spare = spareViews.get();
            return spare < 0 ? 0 : spare + Math.min(room(), Long.MAX_VALUE - spare);
        }

        @Override public boolean reserveView(long bytes) {
            for (;;) {
                long spare = spareViews.get();
                if (spare == CLOSED_VIEWS) return false;
                if (spare < bytes) {
                    if (!refillViews(bytes)) return false;
                } else if (spareViews.compareAndSet(spare, spare - bytes)) {
                    return true;
                }
            }
        }

        private boolean refillViews(long bytes) {
            boolean changed;
            boolean granted = false;
            synchronized (lock) {
                long spare = spareViews.get();
                if (spare == CLOSED_VIEWS) return false;
                if (spare >= bytes) return true;
                long room = roomFor(bytes);
                // The reclamation above can take our own spare as well.
                spare = spareViews.get();
                long need = bytes - spare;
                if (need <= 0) {
                    granted = true;
                } else if (room >= need) {
                    long want = room >= 4 * VIEW_CACHE && firstWaiting == Long.MAX_VALUE ? Math.max(bytes, VIEW_CACHE) : bytes;
                    long grant = Math.min(want - spare, room);
                    viewCaches.add(this);
                    used += grant;
                    optionalPaid += grant;
                    spareViews.addAndGet(grant);
                    granted = true;
                }
                changed = updatePressure();
            }
            signal(changed);
            return granted;
        }

        @Override public void releaseView(long bytes) {
            while (true) {
                long spare = spareViews.get();
                if (spare < 0 || over || firstWaiting != Long.MAX_VALUE) {
                    releaseOptional(bytes, true);
                    return;
                }
                long keep = Math.min(spare + bytes, VIEW_CACHE);
                if (spareViews.compareAndSet(spare, keep)) {
                    // A waiter may have registered after this return decided to cache the bytes.
                    long giveBack = spare + bytes - keep;
                    if (giveBack != 0 || over || firstWaiting != Long.MAX_VALUE) releaseOptional(giveBack, over || firstWaiting != Long.MAX_VALUE);
                    return;
                }
            }
        }

        private void releaseOptional(long bytes, boolean reclaim) {
            boolean changed;
            synchronized (lock) {
                if (reclaim && !closed) bytes += spareViews.getAndSet(0);
                optionalPaid -= bytes;
                used -= bytes;
                changed = updatePressure();
            }
            signal(changed);
        }

        @Override public boolean reserveLoan(long bytes, long descriptor) {
            boolean changed;
            boolean granted;
            synchronized (lock) {
                if (closed) return false;
                long previous = charged();
                long requested = bytes + descriptor;
                long needed = Math.max(0, loaned + requested - loanFloor) - Math.max(0, loaned - loanFloor);
                long chargedReceive = chargedReceive();
                long extra = Math.max(needed - (chargedReceive - receiveCommitted()),
                        descriptor - (chargedReceive - retained));
                granted = extra <= roomFor(extra);
                if (granted) {
                    loaned += requested;
                    retained += descriptor;
                    changed = book(previous);
                } else {
                    waitFor(extra);
                    changed = updatePressure();
                }
            }
            signal(changed);
            return granted;
        }

        @Override public void releaseLoan(long bytes, long descriptor) {
            boolean changed;
            synchronized (lock) {
                long previous = charged();
                if (!closed) loaned -= bytes + descriptor;
                retained -= descriptor;
                changed = book(previous);
            }
            signal(changed);
        }

        private long receiveCommitted() { return credit * cost + receiveStreams + working + Math.max(0, loaned - loanFloor); }

        private long chargedReceive() { return Math.max(receiveCommitted(), retained); }

        private long charged() { return chargedReceive() + Math.max(sendPromised, sendLive); }

        private long payable(long wanted, long unit) {
            return Math.min(wanted, roomFor(unitBytes(wanted, unit)) / unit);
        }

        @SuppressWarnings("NonAtomicVolatileUpdate") // callers hold the enclosing budget monitor
        private boolean book(long previous) {
            used += charged() - previous;
            return updatePressure();
        }

        @Override public boolean tryRetainReceive(long bytes) {
            synchronized (lock) {
                if (closed || bytes > receiveCommitted() - retained) return false;
                retained += bytes;
                return true;
            }
        }

        @Override public void releaseRetainedReceive(long bytes) {
            boolean changed;
            synchronized (lock) {
                long previous = charged();
                retained -= bytes;
                changed = book(previous);
            }
            signal(changed);
        }

        @Override public void close() {
            boolean changed;
            synchronized (lock) {
                if (closed) return;
                long previous = charged();
                closed = true;
                removeWaiting(this);
                waitingFor = Long.MAX_VALUE;
                roomReady = null;
                viewCaches.remove(this);
                long spare = spareViews.getAndSet(CLOSED_VIEWS);
                optionalPaid -= spare;
                used -= spare;
                credit = receiveStreams = sendPromised = working = loaned = 0;
                changed = book(previous);
            }
            signal(changed);
        }
    }

    private boolean updatePressure() {
        boolean previous = over;
        boolean pressure = limit != SharedMemory.UNLIMITED
                && (used >= limit || (previous && used >= limit - limit / 4));
        over = pressure;
        return previous != pressure;
    }

    private void signal(boolean changed) {
        if (changed && onChange != null) onChange.run();
        if (firstWaiting == Long.MAX_VALUE || room() < firstWaiting) return;
        ArrayList<Runnable> ready = new ArrayList<>();
        synchronized (lock) {
            long available = room();
            while (waiting != null && !waiting.isEmpty() && waiting.firstKey() <= available) {
                for (ReceiveAccount account : waiting.pollFirstEntry().getValue()) {
                    account.waitingFor = Long.MAX_VALUE;
                    if (account.commitmentWait <= available) account.commitmentWait = Long.MAX_VALUE;
                    if (account.backingWait <= available) account.backingWait = Long.MAX_VALUE;
                    account.updateWaiting();
                    ready.add(account.roomReady);
                }
            }
            updateFirstWaiting();
        }
        ready.forEach(Runnable::run);
    }

    private void insertWaiting(ReceiveAccount account) {
        if (waiting == null) waiting = new TreeMap<>();
        waiting.computeIfAbsent(account.waitingFor, _ -> new LinkedHashSet<>()).add(account);
        updateFirstWaiting();
    }

    private void removeWaiting(ReceiveAccount account) {
        if (waiting == null) return;
        var entries = waiting.get(account.waitingFor);
        if (entries == null) return;
        entries.remove(account);
        if (entries.isEmpty()) waiting.remove(account.waitingFor);
        updateFirstWaiting();
    }

    private void updateFirstWaiting() {
        firstWaiting = waiting == null || waiting.isEmpty() ? Long.MAX_VALUE : waiting.firstKey();
    }
}
