package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.internal.core.CarrierAdmission;
import java.net.InetSocketAddress;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Source aggregation: one prefix per address family for everything the listener keeps per source. */
@Timeout(30)
class SourcePrefixTest {
    static InetSocketAddress at(String address, int port) throws Exception {
        return new InetSocketAddress(java.net.InetAddress.getByName(address), port);
    }

    /** IPv4-mapped IPv6, as an IPv6 address object (getByName would give an IPv4 one). */
    static InetSocketAddress mapped(int a, int b, int c, int d) throws Exception {
        byte[] x = new byte[16];
        x[10] = (byte) 0xff; x[11] = (byte) 0xff;
        x[12] = (byte) a; x[13] = (byte) b; x[14] = (byte) c; x[15] = (byte) d;
        return new InetSocketAddress(java.net.Inet6Address.getByAddress(null, x, -1), 9);
    }

    @Test void prefixesOutsideTheirRangeAreConfigurationErrors() {
        var b = ListenerConfig.builder();
        for (int bad : new int[] {0, -1, 33}) assertThrows(IllegalArgumentException.class, () -> b.sourcePrefixV4(bad), "v4 " + bad);
        for (int bad : new int[] {0, -1, 129}) assertThrows(IllegalArgumentException.class, () -> b.sourcePrefixV6(bad), "v6 " + bad);
        var d = ListenerConfig.defaults();
        assertEquals(32, d.sourcePrefixV4());
        assertEquals(64, d.sourcePrefixV6());
        var set = b.sourcePrefixV4(1).sourcePrefixV6(128).build();
        assertEquals(1, set.sourcePrefixV4());
        assertEquals(128, set.sourcePrefixV6());
        assertEquals(set.sourcePrefixV6(), set.toBuilder().build().sourcePrefixV6());
    }

    @Test void slotsAndStallDurationsAreSharedWithinAPrefix() throws Exception {
        var adm = Admission.of(1, Admission.Decider.immediate((info, prefix) -> Admission.Decision.CLOSE))
                .withStall(Duration.ofSeconds(1), Duration.ofDays(1));
        var lc = ListenerConfig.builder().maxPendingPerSource(2).sourcePrefixV4(24).sourcePrefixV6(56).admission(adm).build();
        try (var l = ZmuxListener.of(lc)) {
            // Two IPv4 sources of one /24 hold its two slots; a third, mapped or not, has none.
            assertNotNull(l.reserve(at("10.0.0.1", 1), null));
            assertNotNull(l.reserve(at("10.0.0.2", 2), null));
            assertNull(l.reserve(at("10.0.0.200", 3), null));
            assertNull(l.reserve(mapped(10, 0, 0, 7), null), "IPv4-mapped IPv6 counts as IPv4");
            assertNotNull(l.reserve(at("10.0.1.1", 4), null), "another /24 has its own share");
            // IPv6: /56 is the first seven bytes.
            assertNotNull(l.reserve(at("2001:db8:0:1::1", 5), null));
            assertNotNull(l.reserve(at("2001:db8:0:ff::2", 6), null));
            assertNull(l.reserve(at("2001:db8:0:42::3", 7), null));
            assertNotNull(l.reserve(at("2001:db8:0:100::1", 8), null), "another /56 has its own share");

            assertEquals(l.stallDuration(at("10.0.0.1", 1)), l.stallDuration(at("10.0.0.99", 2)));
            assertEquals(l.stallDuration(at("10.0.0.1", 1)), l.stallDuration(mapped(10, 0, 0, 3)));
            assertNotEquals(l.stallDuration(at("10.0.0.1", 1)), l.stallDuration(at("10.0.1.1", 1)));
            assertEquals(l.stallDuration(at("2001:db8:0:1::1", 1)), l.stallDuration(at("2001:db8:0:ff::9", 1)));
            assertNotEquals(l.stallDuration(at("2001:db8:0:1::1", 1)), l.stallDuration(at("2001:db8:0:100::1", 1)));
        }
    }

    @Test void theDefaultsCountAnIpv4AddressAloneAndAnIpv6OneByItsSlash64() throws Exception {
        var a = new CarrierAdmission(2, 0, 0, 0, 0);
        assertNotEquals(a.sourceKey(at("10.0.0.1", 1)), a.sourceKey(at("10.0.0.2", 1)));
        assertEquals(a.sourceKey(at("10.0.0.1", 1)), a.sourceKey(at("10.0.0.1", 2)), "ports are ignored");
        assertEquals(a.sourceKey(at("2001:db8::1", 1)), a.sourceKey(at("2001:db8::ffff:2", 1)));
        assertNotEquals(a.sourceKey(at("2001:db8::1", 1)), a.sourceKey(at("2001:db8:0:1::1", 1)));
        assertNotEquals(a.sourceKey(at("0.0.0.1", 1)), a.sourceKey(at("::1", 1)), "the families never meet");
    }

    @Test void stalledCarriersAreCountedPerPrefix() throws Exception {
        var a = new CarrierAdmission(3, 0, 0, 24, 56);
        for (String s : new String[] {"10.0.0.1", "10.0.0.2", "10.0.0.3"}) assertEquals(CarrierAdmission.TAKEN, a.take(at(s, 1)));
        assertTrue(a.stall(at("10.0.0.1", 1)));
        assertTrue(a.stall(at("10.0.0.2", 1)));
        assertFalse(a.stall(at("10.0.0.3", 1)), "the /24 holds maxPerSource - 1 stalled carriers");
        assertEquals(CarrierAdmission.TAKEN, a.take(at("10.0.1.1", 1)));
        assertTrue(a.stall(at("10.0.1.1", 1)), "another /24 has its own count");
        assertEquals(CarrierAdmission.TAKEN, a.take(at("2001:db8:0:1::1", 1)));
        assertEquals(CarrierAdmission.TAKEN, a.take(at("2001:db8:0:2::1", 1)));
        assertTrue(a.stall(at("2001:db8:0:1::1", 1)));
        assertTrue(a.stall(at("2001:db8:0:2::1", 1)));
        assertEquals(CarrierAdmission.TAKEN, a.take(at("2001:db8:0:3::1", 1)));
        assertFalse(a.stall(at("2001:db8:0:3::1", 1)), "the /56 holds maxPerSource - 1 stalled carriers");
    }
}
