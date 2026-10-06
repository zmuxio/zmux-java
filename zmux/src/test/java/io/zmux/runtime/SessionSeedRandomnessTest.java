package io.zmux.runtime;

import io.zmux.Role;
import io.zmux.Settings;
import io.zmux.ZmuxConfig;
import io.zmux.protocol.Negotiated;
import io.zmux.protocol.Preface;
import io.zmux.protocol.Protocol;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

final class SessionSeedRandomnessTest {
    private static final long GAMMA = -7046029254386353131L;

    private static AtomicLong staticCounter(Class<?> owner, String name) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return (AtomicLong) field.get(null);
    }

    private static long longField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(target);
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static SessionRuntime readyExplicitRoleRuntime() throws Exception {
        // Explicit roles force both tie_breaker_nonce values to 0 (SPEC 2), the case that used to fall back to the
        // process-global counter.
        // Keepalive stays off so these writer-less runtimes do not arm the shared keepalive timer.
        ZmuxConfig config = ZmuxConfig.builder()
                .role(Role.RESPONDER)
                .keepaliveInterval(java.time.Duration.ZERO)
                .build();
        SessionRuntime runtime = SessionRuntimeTestSupport.newRuntime(config);
        Preface peerPreface = new Preface(
                Protocol.PREFACE_VERSION,
                Role.INITIATOR,
                0L,
                Protocol.PROTO_VERSION,
                Protocol.PROTO_VERSION,
                0L,
                Settings.defaults()
        );
        Negotiated negotiated = new Negotiated(
                Protocol.PROTO_VERSION,
                0L,
                Role.RESPONDER,
                Role.INITIATOR,
                Settings.defaults()
        );
        assertEquals(0L, config.localPreface().tieBreakerNonce(), "explicit role should advertise a zero nonce");
        synchronized (runtime.lock()) {
            runtime.markReadyLocked(peerPreface, negotiated, System.nanoTime());
        }
        return runtime;
    }

    private static boolean smallGammaMultiple(long state) {
        for (long k = 1L; k <= 4096L; k++) {
            if (state == k * GAMMA) {
                return true;
            }
        }
        return false;
    }

    @Test
    void explicitRoleSessionsSeedPingAndJitterStateFromCsprngNotProcessCounter() throws Exception {
        AtomicLong nonceCounter = staticCounter(SessionRuntime.class, "SESSION_NONCE_SEED_COUNTER");
        AtomicLong jitterCounter = staticCounter(SessionTelemetryState.class, "KEEPALIVE_JITTER_COUNTER");
        Set<Long> pingStates = new HashSet<>();
        Set<Long> jitterStates = new HashSet<>();
        for (int i = 0; i < 8; i++) {
            // Simulate a fresh process: the old fallback counters started at zero in every JVM.
            nonceCounter.set(0L);
            jitterCounter.set(0L);
            SessionRuntime runtime = readyExplicitRoleRuntime();
            long pingState = longField(runtime, "pingNonceState");
            long jitterState = longField(field(runtime, "telemetry"), "keepaliveJitterState");

            assertFalse(smallGammaMultiple(pingState), "PING nonce state must not come from the process counter");
            assertFalse(smallGammaMultiple(jitterState), "keepalive jitter state must not come from the process counter");
            assertNotEquals(pingState, jitterState, "PING and jitter generators should be seeded independently");
            pingStates.add(pingState);
            jitterStates.add(jitterState);
        }
        assertEquals(8, pingStates.size(), "every session should draw a distinct PING nonce seed");
        assertEquals(8, jitterStates.size(), "every session should draw a distinct keepalive jitter seed");
    }

    @Test
    void randomSessionSeedsAreDistinctAndNonZero() {
        Set<Long> seeds = new HashSet<>();
        for (int i = 0; i < 16; i++) {
            seeds.add(SessionRuntime.randomSessionSeed());
        }
        assertEquals(16, seeds.size(), "random session seeds should not repeat");
        assertFalse(seeds.contains(0L), "random session seed must be non-zero");
    }
}
