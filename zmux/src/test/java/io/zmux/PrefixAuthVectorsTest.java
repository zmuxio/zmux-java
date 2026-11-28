package io.zmux;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.Admission.CarrierInfo;
import io.zmux.Admission.OuterSecurity;
import io.zmux.PrefixStore.Attempt;
import io.zmux.PrefixStore.Claim;
import io.zmux.PrefixStore.Generation;
import io.zmux.PrefixStore.Schedule;
import io.zmux.PrefixStore.Ticket;
import io.zmux.PrefixStore.Window;
import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeIo;
import io.zmux.testutil.Json;
import io.zmux.testutil.SpecFixtures;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.security.Key;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;
import javax.crypto.MacSpi;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class PrefixAuthVectorsTest {
    private static final Duration SLOT = Duration.ofSeconds(30);
    private static final List<String> HEADER_NAMES = List.of(PrefixAuth.HEADER_FIELD);
    private static final byte[] EMPTY = new byte[0];

    private record Vector(String name, long utc, long slot, byte[] outer, PrefixStore.Id id, byte[] key,
                          byte[] nonce, byte[] binding, byte[] token, String header) {
        Vector(Map<?, ?> row) {
            this((String) row.get("name"), number(row, "unix_seconds", 0), number(row, "slot", 0),
                    hex(row.get("outer")), PrefixStore.Id.of(hex(row.get("id"))), hex(row.get("ticket_key")),
                    hex(row.get("nonce")), hex(row.get("binding")), hex(row.get("token")), (String) row.get("header"));
        }

        boolean bound() { return binding.length != 0; }

        Generation generation() { return new Generation(outer, 0, -1); }

        Ticket ticket(boolean singleUse) { return new Ticket(id, key, 1, 0, -1, singleUse); }
    }

    private static final class TestClock extends Clock {
        private final long seconds;
        TestClock(long seconds) { this.seconds = seconds; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(instant(), zone); }
        @Override public Instant instant() { return Instant.ofEpochSecond(seconds); }
    }

    private static final class FixedRandom extends SecureRandom {
        private static final long serialVersionUID = 1L;
        private final byte[] nonce;
        FixedRandom(byte[] nonce) { this.nonce = nonce.clone(); }
        @Override public void nextBytes(byte[] bytes) {
            assertEquals(nonce.length, bytes.length);
            System.arraycopy(nonce, 0, bytes, 0, bytes.length);
        }
    }

    private static Map<String, Object> fixture(String file) throws IOException {
        return Json.object(Files.readString(SpecFixtures.root().resolve("secure-carrier/vectors").resolve(file)));
    }

    private static Map<String, Vector> vectors() throws IOException {
        var vectors = new java.util.HashMap<String, Vector>();
        for (Object row : (List<?>) fixture("first_message.json").get("prefix")) {
            var vector = new Vector((Map<?, ?>) row);
            vectors.put(vector.name, vector);
        }
        return vectors;
    }

    private static long number(Map<?, ?> row, String key, long defaultValue) {
        return row.get(key) instanceof Number number ? number.longValue() : defaultValue;
    }

    private static String str(Map<?, ?> row, String key, String defaultValue) {
        return row.get(key) instanceof String string ? string : defaultValue;
    }

    private static byte[] hex(Object value) {
        return value instanceof String string ? HexFormat.of().parseHex(string) : EMPTY;
    }

    private static Admission.Deadline deadline() {
        return new Admission.Deadline(System.nanoTime() + TimeUnit.SECONDS.toNanos(10));
    }

    private static CarrierInfo carrier(String form, OuterSecurity security, byte[] binding, List<String> fields) {
        PipeIo.Exporter exporter = binding.length == 0 ? null : (label, context, length) -> {
            assertEquals(PrefixAuth.EXPORTER_LABEL, label);
            assertArrayEquals(EMPTY, context);
            assertEquals(32, length);
            return binding.clone();
        };
        Map<String, List<String>> headers = fields.isEmpty() ? Map.of() : Map.of(HEADER_NAMES.getFirst(), fields);
        return new CarrierInfo(null, null, "", "", exporter, RequestHead.of(headers)).withOuterSecurity(security)
                .at(form.equals("header") ? Admission.Stage.HEAD : Admission.Stage.CARRIER,
                        form.equals("message") ? Framing.MESSAGES : Framing.BYTE_STREAM, false);
    }

    @Test void producesEveryCanonicalTokenAndHeader() throws Exception {
        for (Vector vector : vectors().values()) {
            var auth = new PrefixAuth(new PrefixAuth.CarrierOptions(vector.bound(), false), SLOT, vector.id, vector.key,
                    List.of(vector.generation()), new TestClock(vector.utc), new FixedRandom(vector.nonce));
            var info = carrier("header", OuterSecurity.TLS_1_3, vector.binding, List.of());
            assertArrayEquals(vector.token, auth.prefix(info), vector.name);
            assertEquals(vector.header, auth.header(info).value(), vector.name);
            assertArrayEquals(vector.token, PrefixAuth.decodeHeader(vector.header), vector.name);
        }
    }

    @Test void presenterRequiresActualEncryptionAndACompletedBinding() throws Exception {
        Vector vector = vectors().get("bound");
        var auth = new PrefixAuth(new PrefixAuth.CarrierOptions(true, true), SLOT, vector.id, vector.key,
                List.of(vector.generation()), new TestClock(vector.utc), new FixedRandom(vector.nonce));
        for (var security : List.of(OuterSecurity.CLEAR, OuterSecurity.ENCRYPTED, OuterSecurity.EARLY_DATA)) {
            assertThrows(IOException.class, () -> auth.prefix(carrier("stream", security, vector.binding, List.of())));
        }
        assertThrows(IOException.class, () -> auth.header(carrier("header", OuterSecurity.TLS_1_3, EMPTY, List.of())));
        assertArrayEquals(vector.token, auth.prefix(carrier("stream", OuterSecurity.QUIC, vector.binding, List.of())));
        var unbound = new PrefixAuth(new PrefixAuth.CarrierOptions(false, true, "X-Ticket"), SLOT, vector.id, vector.key,
                List.of(vector.generation()), new TestClock(vector.utc), new FixedRandom(vector.nonce));
        assertThrows(IOException.class, () -> unbound.header(carrier("header", OuterSecurity.CLEAR, EMPTY, List.of())));
        assertEquals("X-Ticket", unbound.header(carrier("header", OuterSecurity.ENCRYPTED, EMPTY, List.of())).name());
    }

    @Test void runsAllPrefixScenariosOnTheSharedAuthority() throws Exception {
        var vectors = vectors();
        Map<?, ?> prefix = (Map<?, ?>) fixture("first_message_cases.json").get("prefix");
        Vector initial = vectors.get(prefix.get("default_input"));
        long base = number(prefix, "base_unix_seconds", 0);
        int skew = Math.toIntExact(number(prefix, "skew_slots", 0));
        assertEquals(SLOT.toSeconds(), number(prefix, "slot_seconds", 0));
        for (Object test : (List<?>) prefix.get("cases")) {
            Map<?, ?> testCase = (Map<?, ?>) test;
            long cutover = number(testCase, "cutover_slot", 0);
            List<Generation> generations = cutover == 0 ? List.of(initial.generation()) : List.of(
                    new Generation(initial.outer, 0, cutover - 1),
                    new Generation(vectors.get(str(testCase, "incoming_outer_from", "rotated")).outer, cutover, -1));
            var store = new MemoryPrefixStore(new Window(SLOT, skew), generations, 4,
                    Math.toIntExact(number(testCase, "claim_capacity", 10)));
            store.addTicket(initial.ticket(Boolean.TRUE.equals(testCase.get("single_use"))));
            for (Object step : (List<?>) testCase.get("attempts")) {
                Map<?, ?> attempt = (Map<?, ?>) step;
                if (attempt.get("expire_at_offset_seconds") instanceof Number expires) {
                    store.expire(Instant.ofEpochSecond(base + expires.longValue()));
                } else if (attempt.get("concurrent_inputs") instanceof List<?> inputs) {
                    runConcurrent(vectors, testCase, attempt, inputs, base, store);
                } else {
                    var vector = vectors.get(str(attempt, "input", initial.name));
                    assertEquals(attempt.get("expect"), verify(vector, testCase, attempt, base, store), testCase.toString());
                }
                if ("revoke_ticket".equals(attempt.get("then"))) store.revokeTicket(initial.id);
                // Ending the carrier after admission leaves the same authority and claims in place.
            }
        }
    }

    private static void runConcurrent(Map<String, Vector> vectors, Map<?, ?> testCase, Map<?, ?> attempt,
                                      List<?> inputs, long base, MemoryPrefixStore store) throws Exception {
        var barrier = new CyclicBarrier(inputs.size());
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var results = new ArrayList<java.util.concurrent.Future<String>>();
            for (Object input : inputs) {
                results.add(executor.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return verify(vectors.get(input), testCase, attempt, base, store);
                }));
            }
            var got = new ArrayList<String>();
            for (var result : results) got.add(result.get(5, TimeUnit.SECONDS));
            assertEquals(attempt.get("expect_multiset"), got.stream().sorted().toList(), testCase.toString());
        }
    }

    private static String verify(Vector vector, Map<?, ?> testCase, Map<?, ?> attempt, long base, PrefixStore store)
            throws Exception {
        var clock = new TestClock(base + number(attempt, "offset_seconds", 0));
        String form = str(testCase, "form", "stream");
        boolean clear = Boolean.TRUE.equals(testCase.get("accept_first_use_dos"))
                || Boolean.TRUE.equals(attempt.get("accept_first_use_dos"));
        var security = switch (str(attempt, "outer_transport", "tls13")) {
            case "tls12" -> OuterSecurity.ENCRYPTED;
            case "quic" -> OuterSecurity.QUIC;
            case "cleartext" -> OuterSecurity.CLEAR;
            case "tls13" -> OuterSecurity.TLS_1_3;
            default -> throw new AssertionError(attempt);
        };
        if (Boolean.TRUE.equals(attempt.get("early_data"))) security = OuterSecurity.EARLY_DATA;
        byte[] binding = attempt.containsKey("binding_hex") ? hex(attempt.get("binding_hex")) : vector.binding;
        String field = vector.header + str(attempt, "append_text", "");
        if (Boolean.TRUE.equals(attempt.get("xor_unused_bit"))) {
            String digits = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
            int last = field.length() - 1;
            field = field.substring(0, last) + digits.charAt(digits.indexOf(field.charAt(last)) ^ 1);
        }
        var info = carrier(form, security, binding,
                java.util.Collections.nCopies(Math.toIntExact(number(attempt, "count", 1)), field));
        byte[] extra = hex(attempt.get("append_hex"));
        byte[] candidate = Arrays.copyOf(vector.token, vector.token.length + extra.length);
        System.arraycopy(extra, 0, candidate, vector.token.length, extra.length);
        if (attempt.get("xor_byte") instanceof Number index) candidate[index.intValue()] ^= 1;
        byte[] input = form.equals("header") ? EMPTY : Arrays.copyOf(candidate,
                Math.toIntExact(number(attempt, "take_bytes", candidate.length)));
        var verifier = new PrefixAuth.Verifier(new PrefixAuth.CarrierOptions(vector.bound(), clear), store, 2, clock);
        var decision = verifier.decide(info, ByteBuffer.wrap(input), deadline()).toCompletableFuture().get(5, TimeUnit.SECONDS);
        if (decision.verdict() != AdmissionVerdict.ACCEPT) {
            assertEquals(Admission.Decision.CLOSE, decision);
            return "refused";
        }
        assertEquals(form.equals("header") ? 0 : PrefixAuth.TOKEN_LENGTH, decision.skip());
        assertEquals(vector.bound(), decision.value() != null);
        if (attempt.containsKey("expect_remaining_hex")) {
            assertArrayEquals(hex(attempt.get("expect_remaining_hex")), Arrays.copyOfRange(input, decision.skip(), input.length));
        }
        return "accepted";
    }

    private static final class StoreProbe implements PrefixStore {
        final MemoryPrefixStore store;
        int floors, lookups, claims, resolutions;
        java.util.function.Function<Attempt, CompletionStage<Ticket>> delayedLookup;
        java.util.function.BiFunction<Claim, Attempt, CompletionStage<Boolean>> delayedClaim;
        Claim lastClaim;
        boolean loseReceipt;

        StoreProbe(MemoryPrefixStore store) { this.store = store; }

        @Override public void close() { store.close(); }
        @Override public Window window() { return store.window(); }
        @Override public Schedule localSchedule() { return store.localSchedule(); }
        @Override public CompletionStage<Long> floor(Attempt attempt) { floors++; return store.floor(attempt); }
        @Override public CompletionStage<Ticket> lookup(PrefixStore.Id id, Attempt attempt) {
            lookups++;
            return delayedLookup == null ? store.lookup(id, attempt) : delayedLookup.apply(attempt);
        }
        @Override public CompletionStage<Boolean> claim(Claim claim, Attempt attempt) {
            claims++;
            lastClaim = claim;
            if (delayedClaim != null) return delayedClaim.apply(claim, attempt);
            var result = store.claim(claim, attempt);
            return loseReceipt ? CompletableFuture.failedFuture(new IOException("lost receipt")) : result;
        }
        @Override public CompletionStage<Boolean> resolveClaim(Claim claim, Admission.Deadline deadline) {
            resolutions++;
            assertEquals(lastClaim, claim);
            return store.resolveClaim(claim, deadline);
        }
    }

    @Test void retiringKeysClearsTheOwnedCopiesAndLeavesTheCallerCredentialsAlone() throws Exception {
        Vector vector = vectors().get("unbound");
        var generation = vector.generation();
        var ticket = vector.ticket(false);
        var options = new PrefixAuth.CarrierOptions(false, false);
        var clock = new TestClock(vector.utc);
        var info = carrier("stream", OuterSecurity.TLS_1_3, EMPTY, List.of());
        var a = new PrefixAuth(options, SLOT, vector.id, vector.key, List.of(generation), clock, new FixedRandom(vector.nonce));
        var store = new MemoryPrefixStore(new Window(SLOT, 1), List.of(generation));
        try {
            var verifier = new PrefixAuth.Verifier(options, store, 2, clock);
            store.addTicket(ticket);
            var originalSchedule = store.localSchedule();
            store.setGenerations(List.of(generation));
            assertThrows(IllegalStateException.class, originalSchedule.generations().getFirst()::key);

            var attempt = new Attempt(deadline(), () -> false, () -> java.util.OptionalLong.of(vector.slot));
            var installedTicket = store.lookup(ticket.id(), attempt).toCompletableFuture().join();
            var installedGeneration = store.localSchedule().generations().getFirst();
            assertArrayEquals(vector.token, a.prefix(info));
            store.close();
            assertThrows(IllegalStateException.class, installedTicket::key);
            assertThrows(IllegalStateException.class, installedGeneration::key);
            assertEquals(Admission.Decision.CLOSE, decide(verifier, vector.token).toCompletableFuture().join());
            assertArrayEquals(vector.token, a.prefix(info));
            a.close();
            assertThrows(IOException.class, () -> a.prefix(info));
            assertArrayEquals(vector.key, ticket.key());
            assertArrayEquals(vector.outer, generation.key());
        } finally {
            a.close();
            store.close();
        }
    }

    @Test void unknownRevocationFailsAtLifetimeCapacityWithoutInvalidatingOtherTombstones() throws Exception {
        Vector vector = vectors().get("unbound");
        try (var store = new MemoryPrefixStore(new Window(SLOT, 1), List.of(vector.generation()), 1, 1)) {
            store.revokeTicket(vector.id);
            store.revokeTicket(vector.id);
            var stranger = new PrefixStore.Id(17, 32);
            assertThrows(IllegalArgumentException.class, () -> store.revokeTicket(stranger));
            assertThrows(IllegalArgumentException.class, () -> store.addTicket(vector.ticket(false)));
        }
    }

    private static CompletionStage<Admission.Decision> decide(PrefixAuth.Verifier verifier, byte[] token) {
        return verifier.decide(carrier("stream", OuterSecurity.TLS_1_3, EMPTY, List.of()), ByteBuffer.wrap(token), deadline());
    }

    @Test void authenticatesOuterTagBeforeFloorOrLookupAndCommitsOnlyValidTicketTags() throws Exception {
        Vector input = vectors().get("unbound");
        var store = new MemoryPrefixStore(new Window(SLOT, 1), List.of(
                new Generation(input.outer, 0, input.slot), new Generation(input.outer, input.slot + 1, -1)));
        store.addTicket(input.ticket(true));
        var probe = new StoreProbe(store);
        var verifier = new PrefixAuth.Verifier(new PrefixAuth.CarrierOptions(false, false), probe, 2, new TestClock(input.utc));
        assertEquals(Admission.Decision.CLOSE, decide(verifier, input.token).toCompletableFuture().join());
        assertEquals(0, probe.floors);
        assertEquals(0, probe.lookups);
        store.setGenerations(List.of(input.generation()));
        byte[] bad = input.token.clone();
        bad[32] ^= 1;
        assertEquals(Admission.Decision.CLOSE, decide(verifier, bad).toCompletableFuture().join());
        assertEquals(0, probe.floors);
        assertEquals(0, probe.lookups);

        bad = input.token.clone();
        bad[48] ^= 1;
        assertEquals(Admission.Decision.CLOSE, decide(verifier, bad).toCompletableFuture().join());
        assertEquals(1, probe.floors);
        assertEquals(1, probe.lookups);
        assertEquals(0, probe.claims);

        probe.loseReceipt = true;
        assertEquals(AdmissionVerdict.ACCEPT, decide(verifier, input.token).toCompletableFuture().join().verdict());
        assertEquals(1, probe.claims);
        assertEquals(1, probe.resolutions);
        assertThrows(IllegalArgumentException.class, () -> store.addTicket(input.ticket(true)));
    }

    @Test void revalidatesRevocationAndCancellationBeforeCommit() throws Exception {
        Vector vector = vectors().get("unbound");
        for (boolean revoke : List.of(false, true)) {
            var store = new MemoryPrefixStore(new Window(SLOT, 1), List.of(vector.generation()));
            store.addTicket(vector.ticket(false));
            var probe = new StoreProbe(store);
            var lookup = new CompletableFuture<Ticket>();
            probe.delayedLookup = ignored -> lookup;
            var verifier = new PrefixAuth.Verifier(new PrefixAuth.CarrierOptions(false, false), probe, 2, new TestClock(vector.utc));
            var result = decide(verifier, vector.token).toCompletableFuture();
            assertFalse(result.isDone());
            if (revoke) store.setGenerations(List.of());
            else assertTrue(result.cancel(false));
            lookup.complete(vector.ticket(false));
            if (revoke) {
                assertEquals(Admission.Decision.CLOSE, result.join());
                assertEquals(1, probe.claims);
            } else {
                assertEquals(0, probe.claims);
                assertTrue(result.isCancelled());
            }
        }
    }

    @Test void doesNotDeliverACommitConfirmedAfterCancellation() throws Exception {
        Vector vector = vectors().get("unbound");
        var store = new MemoryPrefixStore(new Window(SLOT, 1), List.of(vector.generation()));
        store.addTicket(vector.ticket(true));
        var probe = new StoreProbe(store);
        var receipt = new CompletableFuture<Boolean>();
        probe.delayedClaim = (claim, attempt) -> {
            assertTrue(store.claim(claim, attempt).toCompletableFuture().join());
            return receipt;
        };
        var verifier = new PrefixAuth.Verifier(new PrefixAuth.CarrierOptions(false, false), probe, 2, new TestClock(vector.utc));
        var result = decide(verifier, vector.token).toCompletableFuture();
        assertTrue(result.cancel(false));
        receipt.completeExceptionally(new IOException("lost receipt"));
        assertTrue(result.isCancelled());
        assertEquals(1, probe.resolutions);
        assertEquals(Admission.Decision.CLOSE, decide(verifier, vector.token).toCompletableFuture().join());
    }

    @Test void comparesEverySlotEvenAtUnsignedBoundaries() throws Exception {
        Vector vector = vectors().get("unbound");
        for (long current : new long[] {0, 12, -1L}) {
            for (int skew : new int[] {0, 1, 2}) {
                var window = new Window(SLOT, skew);
                for (long issue : new long[] {window.first(current), window.last(current), current + skew + 1L}) {
                    var spi = new CountingMac();
                    var mac = new TestMac(spi);
                    mac.init(new SecretKeySpec(vector.key, "HmacSHA256"));
                    byte[] token = PrefixAuth.token(vector.generation(), vector.id, vector.key, issue, EMPTY, vector.nonce);
                    var matched = PrefixAuth.matchSlot(mac, vector.id.bytes(), token, EMPTY, vector.generation(), current, window);
                    assertEquals(PrefixStore.containsSlot(window.first(current), window.last(current), issue), matched.isPresent());
                    assertEquals(2 * skew + 1, spi.calls);
                }
            }
        }
        var store = new MemoryPrefixStore(new Window(SLOT, 2), List.of(vector.generation()));
        assertThrows(IllegalArgumentException.class,
                () -> new PrefixAuth.Verifier(new PrefixAuth.CarrierOptions(false, false), store, 1, new TestClock(vector.utc)));
    }

    private static final class TestMac extends Mac {
        TestMac(CountingMac spi) { super(spi, null, "HmacSHA256"); }
    }

    private static final class CountingMac extends MacSpi {
        private Mac delegate;
        int calls;
        @Override protected int engineGetMacLength() { return 32; }
        @Override protected void engineInit(Key key, AlgorithmParameterSpec params) throws java.security.InvalidKeyException {
            try {
                delegate = Mac.getInstance("HmacSHA256");
                delegate.init(key);
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new AssertionError(e);
            }
        }
        @Override protected void engineUpdate(byte b) { delegate.update(b); }
        @Override protected void engineUpdate(byte[] b, int from, int size) { delegate.update(b, from, size); }
        @Override protected byte[] engineDoFinal() { calls++; return delegate.doFinal(); }
        @Override protected void engineReset() { delegate.reset(); }
    }
}
