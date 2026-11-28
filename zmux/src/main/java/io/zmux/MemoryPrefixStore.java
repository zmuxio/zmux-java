package io.zmux;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A process-local prefix authority. Nonce claims and ticket IDs have separate bounded
 * capacities. Ticket ID tombstones survive expiry, revocation and single use and are never
 * freed while this authority is open. The ticket capacity bounds the total number of
 * distinct IDs the authority can install or revoke over its lifetime.
 *
 * <p>The nonce capacity is shared. A holder of a reusable ticket can fill it with valid
 * presentations inside the skew window, preventing further admissions until claims
 * expire. Use new keys and tickets after losing this store, or restore the full domain
 * state with a durable authority.
 */
public final class MemoryPrefixStore implements PrefixStore {
    public static final int DEFAULT_CAPACITY = 1 << 16;

    private record Nonce(Id id, Id nonce) {}
    private record Expiry(Nonce nonce, long lastSlot) {}
    private record Receipt(Id nonce, UUID commitId) {}
    private record TicketState(Ticket ticket, Receipt receipt) {}
    private static final TicketState RETIRED = new TicketState(null, null);

    private final Window window;
    private final int maxTickets, maxNonces;
    private final Map<Id, TicketState> tickets = new HashMap<>();
    private final Map<Nonce, UUID> claims = new HashMap<>();
    private final PriorityQueue<Expiry> expiries =
            new PriorityQueue<>((a, b) -> Long.compareUnsigned(a.lastSlot(), b.lastSlot()));
    private volatile Schedule schedule;

    public MemoryPrefixStore(Window window, List<Generation> generations) {
        this(window, generations, DEFAULT_CAPACITY, DEFAULT_CAPACITY);
    }

    public MemoryPrefixStore(Window window, List<Generation> generations, int maxTickets, int maxNonces) {
        this.window = Objects.requireNonNull(window);
        if (maxTickets <= 0 || maxNonces <= 0) throw new IllegalArgumentException("prefix capacities must be positive");
        this.maxTickets = maxTickets;
        this.maxNonces = maxNonces;
        schedule = new Schedule(0, PrefixStore.copyGenerations(generations), 0);
    }

    @Override public Window window() { return window; }

    @Override public Schedule localSchedule() { return schedule; }

    @Override public CompletionStage<Long> floor(Attempt attempt) {
        Schedule current = schedule;
        return current == null ? CompletableFuture.failedFuture(new IllegalStateException("prefix authority closed"))
                : CompletableFuture.completedFuture(current.floorHint());
    }

    /** Publish the complete installed schedule. Removing a generation revokes it at once. */
    public synchronized void setGenerations(List<Generation> generations) {
        Schedule current = requireOpen();
        if (current.version() == -1L) throw new IllegalStateException("prefix schedule versions exhausted");
        schedule = new Schedule(current.version() + 1, PrefixStore.copyGenerations(generations), current.floorHint());
        current.generations().forEach(Generation::clearKey);
    }

    /** Installs a fresh, previously unused ticket ID. */
    public synchronized void addTicket(Ticket ticket) {
        requireOpen();
        if (tickets.containsKey(ticket.id()) || tickets.size() >= maxTickets
                || Long.compareUnsigned(ticket.lastSlot(), schedule.floorHint()) < 0) {
            throw new IllegalArgumentException("duplicate, expired or over-capacity prefix ticket");
        }
        tickets.put(ticket.id(), new TicketState(ticket.ownedCopy(), null));
    }

    /**
     * Revokes a ticket immediately, or reserves its ID permanently if it has yet to
     * be installed. At ID capacity, reserving an unknown ID throws.
     */
    public synchronized void revokeTicket(Id id) {
        requireOpen();
        Objects.requireNonNull(id);
        var state = tickets.get(id);
        if (state == null) {
            if (tickets.size() >= maxTickets) throw new IllegalArgumentException("over-capacity prefix ticket");
            tickets.put(id, RETIRED);
        } else if (state.ticket() != null) {
            state.ticket().clearKey();
            tickets.put(id, RETIRED);
        }
    }

    @Override public synchronized CompletionStage<Ticket> lookup(Id id, Attempt attempt) {
        var state = tickets.get(id);
        return CompletableFuture.completedFuture(state != null && attempt.active() ? state.ticket() : null);
    }

    @Override public synchronized CompletionStage<Boolean> claim(Claim claim, Attempt attempt) {
        return CompletableFuture.completedFuture(redeem(claim, attempt));
    }

    private boolean redeem(Claim claim, Attempt attempt) {
        if (schedule == null) return false;
        var local = attempt.localSlot();
        if (local.isEmpty() || !attempt.active()) return false;
        long current = PrefixStore.later(schedule.floorHint(), local.getAsLong());
        var state = tickets.get(claim.id());
        Ticket ticket = state == null ? null : state.ticket();
        if (ticket == null || ticket.version() != claim.ticketVersion() || !ticket.liveAt(current)
                || schedule.version() != claim.scheduleVersion() || !schedule.sameEligible(window, claim.checkedSlot(), current)
                || !PrefixStore.containsSlot(window.first(current), window.last(current), claim.issueSlot())
                || schedule.generations().stream().noneMatch(g -> g.firstSlot() == claim.generation() && g.contains(claim.issueSlot()))) {
            return false;
        }
        expireClaims(current);
        var nonce = new Nonce(claim.id(), claim.nonce());
        if (claims.containsKey(nonce) || claims.size() >= maxNonces || !attempt.active()) return false;
        claims.put(nonce, claim.commitId());
        expiries.add(new Expiry(nonce, window.last(claim.issueSlot())));
        if (ticket.singleUse()) {
            ticket.clearKey();
            tickets.put(ticket.id(), new TicketState(null, new Receipt(claim.nonce(), claim.commitId())));
        }
        advanceFloor(current);
        return true;
    }

    @Override public synchronized CompletionStage<Boolean> resolveClaim(Claim claim, Admission.Deadline deadline) {
        if (deadline.expired()) return CompletableFuture.completedFuture(false);
        UUID committed = claims.get(new Nonce(claim.id(), claim.nonce()));
        var state = tickets.get(claim.id());
        Receipt receipt = state == null ? null : state.receipt();
        return CompletableFuture.completedFuture(claim.commitId().equals(committed)
                || (receipt != null && receipt.nonce().equals(claim.nonce()) && receipt.commitId().equals(claim.commitId())));
    }

    /** Retires expired nonce claims under the same floor and replaces expired ticket keys with tombstones. */
    public synchronized void expire(Instant now) {
        requireOpen();
        if (now.getEpochSecond() < 0) return;
        long current = PrefixStore.later(window.slot(now.getEpochSecond()), schedule.floorHint());
        expireClaims(current);
        for (var entry : tickets.entrySet()) {
            Ticket ticket = entry.getValue().ticket();
            if (ticket != null && Long.compareUnsigned(ticket.lastSlot(), current) < 0) {
                ticket.clearKey();
                entry.setValue(RETIRED);
            }
        }
    }

    /** Closes the authority and clears all of its installed key copies. */
    @Override public synchronized void close() {
        Schedule current = schedule;
        if (current == null) return;
        schedule = null;
        current.generations().forEach(Generation::clearKey);
        for (var state : tickets.values()) {
            if (state.ticket() != null) state.ticket().clearKey();
        }
        tickets.clear();
        claims.clear();
        expiries.clear();
    }

    private Schedule requireOpen() {
        if (schedule == null) throw new IllegalStateException("prefix authority closed");
        return schedule;
    }

    private void expireClaims(long current) {
        if (!expiries.isEmpty() && Long.compareUnsigned(expiries.peek().lastSlot(), current) < 0) {
            do {
                claims.remove(expiries.remove().nonce());
            } while (!expiries.isEmpty() && Long.compareUnsigned(expiries.peek().lastSlot(), current) < 0);
            advanceFloor(current);
        }
    }

    private void advanceFloor(long current) {
        // Both claim and expire hold the store monitor, as do all other writers.
        Schedule previous = schedule;
        if (current != previous.floorHint()) {
            schedule = new Schedule(previous.version(), previous.generations(), current);
        }
    }
}
