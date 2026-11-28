package io.zmux;

import io.zmux.PrefixStore.Attempt;
import io.zmux.PrefixStore.Claim;
import io.zmux.PrefixStore.Generation;
import io.zmux.PrefixStore.Schedule;
import io.zmux.PrefixStore.Window;
import io.zmux.carrier.Framing;
import io.zmux.internal.random.OsRandom;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.crypto.Mac;

/**
 * Presents a fresh ticket on each independently admitted carrier.
 * A single-use ticket may be consumed even when the carrier is never admitted, for example
 * if cancellation or a lost commit receipt prevents acceptance from reaching the caller.
 * The {@link Verifier} allows up to one second to resolve an uncertain commit receipt.
 * Obtain a new ticket for a retry after an uncertain outcome.
 */
public final class PrefixAuth implements CarrierCredential, AutoCloseable {
    public static final int TOKEN_LENGTH = 64;
    public static final int HEADER_LENGTH = 86;
    public static final String HEADER_FIELD = "Zmux-Prefix-Auth";
    public static final String EXPORTER_LABEL = "EXPORTER-zmux-prefix-auth";

    private static final int TAG_LENGTH = 16;
    private static final byte[] UNBOUND = new byte[0];
    private static final Base64.Encoder HEADER_ENCODER = Base64.getUrlEncoder().withoutPadding();

    /**
     * The agreed carrier form. On an unencrypted stream, allowCleartextBearer opts into
     * the risk of another presenter spending the token first. A header always needs encryption.
     */
    public record CarrierOptions(boolean bound, boolean allowCleartextBearer, String headerField) {
        public CarrierOptions(boolean bound, boolean allowCleartextBearer) {
            this(bound, allowCleartextBearer, HEADER_FIELD);
        }

        public CarrierOptions {
            Objects.requireNonNull(headerField);
            if (headerField.isEmpty() || !headerField.chars().allMatch(PrefixAuth::tokenChar)) {
                throw new IllegalArgumentException("invalid prefix header field");
            }
        }

        private byte[] binding(Admission.CarrierInfo info, boolean head) {
            if (info == null || info.earlyData()
                    || ((head || !allowCleartextBearer) && !info.outerSecurity().encrypted())) return null;
            if (!bound) return UNBOUND;
            if (!info.outerSecurity().permitsBinding()) return null;
            byte[] exporter = info.exportKeyingMaterial(EXPORTER_LABEL, UNBOUND, 32);
            return exporter != null && exporter.length == 32 ? exporter.clone() : null;
        }
    }

    private final CarrierOptions options;
    private final Window window;
    private final PrefixStore.Id id;
    private final PrefixKey ticketKey;
    private boolean closed;
    private final List<Generation> generations;
    private final Clock clock;
    private final SecureRandom random;

    /** The generations select the outer key by the presenter's UTC issue slot. */
    public PrefixAuth(CarrierOptions options, Duration slotLength, PrefixStore.Id id, byte[] ticketKey, List<Generation> generations) {
        this(options, slotLength, id, ticketKey, generations, Clock.systemUTC(), null);
    }

    /**
     * With a host-provided UTC clock and cryptographically secure randomness. A null source
     * uses the OS entropy source. A host source must be reseeded after snapshot restore.
     */
    public PrefixAuth(CarrierOptions options, Duration slotLength, PrefixStore.Id id, byte[] ticketKey, List<Generation> generations,
                      Clock clock, SecureRandom random) {
        this.options = Objects.requireNonNull(options);
        this.window = new Window(slotLength, 0);
        this.id = Objects.requireNonNull(id);
        this.clock = Objects.requireNonNull(clock);
        this.random = random;
        this.ticketKey = new PrefixKey(ticketKey);
        try {
            this.generations = PrefixStore.copyGenerations(generations);
        } catch (RuntimeException invalid) {
            this.ticketKey.clear();
            throw invalid;
        }
    }

    @Override public byte[] prefix(Admission.CarrierInfo info) throws IOException { return present(info, false); }

    @Override public HeaderField header(Admission.CarrierInfo info) throws IOException {
        return new HeaderField(options.headerField(), HEADER_ENCODER.encodeToString(present(info, true)));
    }

    private synchronized byte[] present(Admission.CarrierInfo info, boolean head) throws IOException {
        if (closed) throw new IOException("prefix presenter closed");
        byte[] binding = options.binding(info, head);
        if (binding == null) throw new IOException("prefix authentication unavailable on this carrier");
        long now = clock.instant().getEpochSecond();
        if (now < 0) throw new IOException("prefix authentication needs a nonnegative UTC time");
        long slot = window.slot(now);
        for (Generation generation : generations) {
            if (generation.contains(slot)) {
                byte[] nonce = new byte[TAG_LENGTH];
                if (random == null) OsRandom.fill(nonce);
                else random.nextBytes(nonce);
                return token(generation, id, ticketKey.mac(), slot, binding, nonce);
            }
        }
        throw new IOException("no outer key for the prefix issue slot");
    }

    /** Stops presentation and clears the presenter's copies of its ticket key and outer keys. */
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        ticketKey.clear();
        generations.forEach(Generation::clearKey);
    }

    private static boolean tokenChar(int c) {
        return c > 0x20 && c < 0x7f && "\"(),/:;<=>?@[\\]{}".indexOf(c) < 0;
    }

    /** Decodes a canonical unpadded base64url header, or returns null. */
    public static byte[] decodeHeader(String value) {
        if (value == null || value.length() != HEADER_LENGTH) return null;
        try {
            byte[] token = Base64.getUrlDecoder().decode(value);
            return token.length == TOKEN_LENGTH && HEADER_ENCODER.encodeToString(token).equals(value) ? token : null;
        } catch (IllegalArgumentException malformed) {
            return null;
        }
    }

    static byte[] token(Generation generation, PrefixStore.Id id, byte[] key, long slot, byte[] binding, byte[] nonce) {
        return token(generation, id, PrefixKey.mac(key), slot, binding, nonce);
    }

    private static byte[] token(Generation generation, PrefixStore.Id id, Mac ticket, long slot, byte[] binding, byte[] nonce) {
        if (nonce.length != TAG_LENGTH || (binding.length != 0 && binding.length != 32)) {
            throw new IllegalArgumentException("invalid prefix nonce or binding length");
        }
        byte[] token = Arrays.copyOf(nonce, TOKEN_LENGTH);
        Mac outer = generation.mac();
        byte[] masked = xor(id.bytes(), outerTag(outer, 0, token, TAG_LENGTH));
        System.arraycopy(masked, 0, token, 16, TAG_LENGTH);
        System.arraycopy(outerTag(outer, 1, token, 32), 0, token, 32, TAG_LENGTH);
        System.arraycopy(ticketTag(ticket, id.bytes(), token, slot, binding), 0, token, 48, TAG_LENGTH);
        return token;
    }

    private static byte[] xor(byte[] value, byte[] mask) {
        for (int i = 0; i < TAG_LENGTH; i++) value[i] ^= mask[i];
        return value;
    }

    private static byte[] outerTag(Mac mac, int domain, byte[] token, int length) {
        mac.update((byte) domain);
        mac.update(token, 0, length);
        return Arrays.copyOf(mac.doFinal(), TAG_LENGTH);
    }

    private static byte[] ticketTag(Mac mac, byte[] id, byte[] token, long slot, byte[] binding) {
        mac.update((byte) 2);
        mac.update(id);
        mac.update(token, 0, 48);
        mac.update(ByteBuffer.allocate(9).putLong(slot).put((byte) binding.length).array());
        mac.update(binding);
        return Arrays.copyOf(mac.doFinal(), TAG_LENGTH);
    }

    // This value also passes through admission plumbing before Establish has delivered a peer.
    private record BoundPrincipal(PrefixStore.Id id) {}

    /** The ID proven on the accepted bootstrap carrier, after any inner handshake completes. */
    public static Optional<PrefixStore.Id> boundPrincipal(ZmuxSession session) {
        Object value = session.bootstrapAdmission();
        return value instanceof BoundPrincipal(var id) ? Optional.of(id) : Optional.empty();
    }

    /**
     * A verifier of the specified carrier form and shared domain.
     * A committed single-use ticket stays consumed even if cancellation, carrier loss or an
     * unconfirmed receipt prevents admission. If a commit fails without a known outcome,
     * the verifier queries its original commit ID for up to one second. A late confirmation
     * can settle accounting but cannot admit a carrier beyond the original deadline.
     */
    public static final class Verifier implements Admission.Decider {
        private final CarrierOptions options;
        private final PrefixStore store;
        private final Window window;
        private final Clock clock;

        public Verifier(CarrierOptions options, PrefixStore store) {
            this(options, store, 2, Clock.systemUTC());
        }

        public Verifier(CarrierOptions options, PrefixStore store, int maxSkew, Clock clock) {
            this.options = Objects.requireNonNull(options);
            this.store = Objects.requireNonNull(store);
            this.window = Objects.requireNonNull(store.window());
            this.clock = Objects.requireNonNull(clock);
            if (maxSkew < window.skew()) throw new IllegalArgumentException("prefix skew exceeds the local limit");
        }

        /** Sets the stream peek and first-message read limit for protected admission. */
        public Admission.Builder admission() {
            return Admission.builder(TOKEN_LENGTH, this).maxMessage(TOKEN_LENGTH).refusal(AdmissionVerdict.CLOSE);
        }

        @Override public CompletionStage<Admission.Decision> decide(Admission.CarrierInfo info, ByteBuffer prefix,
                                                                    Admission.Deadline deadline) {
            var decision = new CompletableFuture<Admission.Decision>();
            var attempt = new Attempt(deadline, decision::isCancelled, () -> {
                long seconds = clock.instant().getEpochSecond();
                return seconds < 0 ? OptionalLong.empty() : OptionalLong.of(window.slot(seconds));
            });
            try {
                boolean head = info.stage() == Admission.Stage.HEAD;
                if (head && info.authenticatedEnvelope()) {
                    return CompletableFuture.completedFuture(attempt.active()
                            && !info.earlyData()
                            ? Admission.Decision.accept(0) : Admission.Decision.CLOSE);
                }
                byte[] token = decode(info, prefix, head);
                byte[] binding = token == null ? null : options.binding(info, head);
                if (binding == null || !attempt.active()) return CompletableFuture.completedFuture(Admission.Decision.CLOSE);
                redeem(token, binding, attempt).whenComplete((id, error) -> decision.complete(
                        error == null && id != null && attempt.active()
                                ? Admission.Decision.accept(head ? 0 : TOKEN_LENGTH, options.bound() ? new BoundPrincipal(id) : null)
                                : Admission.Decision.CLOSE));
            } catch (RuntimeException unavailable) {
                decision.complete(Admission.Decision.CLOSE);
            }
            return decision;
        }

        private byte[] decode(Admission.CarrierInfo info, ByteBuffer prefix, boolean head) {
            if (head) {
                var fields = info.header(options.headerField());
                return fields.size() == 1 ? decodeHeader(fields.getFirst()) : null;
            }
            if (prefix.remaining() < TOKEN_LENGTH || (info.framing() == Framing.MESSAGES && prefix.remaining() != TOKEN_LENGTH)) {
                return null;
            }
            byte[] token = new byte[TOKEN_LENGTH];
            prefix.duplicate().get(token);
            return token;
        }

        private CompletionStage<PrefixStore.Id> redeem(byte[] token, byte[] binding, Attempt attempt) {
            var local = attempt.localSlot();
            Schedule schedule = store.localSchedule();
            if (local.isEmpty() || schedule == null) return CompletableFuture.completedFuture(null);
            long prechecked = PrefixStore.later(local.getAsLong(), schedule.floorHint());
            Generation first = uniqueOuter(schedule, token, prechecked);
            if (first == null || !attempt.active()) return CompletableFuture.completedFuture(null);
            return store.floor(attempt).thenCompose(floor -> {
                if (!attempt.active()) return CompletableFuture.completedFuture(null);
                long current = PrefixStore.later(local.getAsLong(), floor);
                Generation selected = schedule.sameEligible(window, prechecked, current) ? first : uniqueOuter(schedule, token, current);
                if (selected == null) return CompletableFuture.completedFuture(null);
                PrefixStore.Id id = PrefixStore.Id.of(xor(Arrays.copyOfRange(token, 16, 32), outerTag(selected.mac(), 0, token, TAG_LENGTH)));
                return store.lookup(id, attempt).thenCompose(ticket -> {
                    if (ticket == null || !id.equals(ticket.id()) || !ticket.liveAt(current) || !attempt.active()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    var issue = matchSlot(ticket.mac(), id.bytes(), token, binding, selected, current, window);
                    if (issue.isEmpty() || !attempt.active()) return CompletableFuture.completedFuture(null);
                    try {
                        var claim = new Claim(id, PrefixStore.Id.read(ByteBuffer.wrap(token)), commitId(), ticket.version(),
                                schedule.version(), selected.firstSlot(), issue.getAsLong(), current);
                        return commit(claim, attempt).thenApply(ok -> Boolean.TRUE.equals(ok) ? id : null);
                    } catch (IOException unavailable) {
                        return CompletableFuture.completedFuture(null);
                    }
                });
            });
        }

        private CompletionStage<Boolean> commit(Claim claim, Attempt attempt) {
            if (!attempt.active()) return CompletableFuture.completedFuture(false);
            CompletionStage<Boolean> pending;
            try {
                pending = store.claim(claim, attempt);
            } catch (RuntimeException uncertain) {
                return resolve(claim);
            }
            return pending.exceptionallyCompose(uncertain -> resolve(claim));
        }

        private CompletionStage<Boolean> resolve(Claim claim) {
            // Resolve under a separate bound even when the original admission has ended.
            var timeout = java.util.concurrent.TimeUnit.SECONDS;
            var deadline = new Admission.Deadline(System.nanoTime() + timeout.toNanos(1));
            var result = new CompletableFuture<Boolean>().completeOnTimeout(false, 1, timeout);
            store.resolveClaim(claim, deadline).whenComplete((ok, failure) ->
                    result.complete(failure == null && !deadline.expired() && Boolean.TRUE.equals(ok)));
            return result;
        }

        private Generation uniqueOuter(Schedule schedule, byte[] token, long current) {
            Generation selected = null;
            int matches = 0;
            byte[] received = Arrays.copyOfRange(token, 32, 48);
            for (Generation generation : schedule.generations()) {
                if (generation.eligible(window, current)
                        && MessageDigest.isEqual(outerTag(generation.mac(), 1, token, 32), received)) {
                    selected = generation;
                    matches++;
                }
            }
            return matches == 1 ? selected : null;
        }
    }

    private static UUID commitId() throws IOException {
        byte[] entropy = new byte[16];
        OsRandom.fill(entropy);
        ByteBuffer bytes = ByteBuffer.wrap(entropy);
        return new UUID(bytes.getLong(), bytes.getLong());
    }

    static OptionalLong matchSlot(Mac mac, byte[] id, byte[] token, byte[] binding, Generation generation,
                                            long current, Window window) {
        byte[] received = Arrays.copyOfRange(token, 48, 64);
        long first = window.first(current), last = window.last(current);
        var issue = OptionalLong.empty();
        for (int offset = -window.skew(); offset <= window.skew(); offset++) {
            long candidate = current + offset;
            boolean valid = PrefixStore.containsSlot(first, last, candidate);
            // When the window spans the uint64 boundary, wrapped candidates lie outside the clamped interval.
            if (MessageDigest.isEqual(ticketTag(mac, id, token, valid ? candidate : 0, binding), received)
                    && valid && generation.contains(candidate)) {
                issue = OptionalLong.of(candidate);
            }
        }
        return issue;
    }
}
