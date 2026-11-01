package io.zmux;

import java.net.InetSocketAddress;

/** Test addresses: a label stands for one client. */
public final class TestAddr {
    private TestAddr() {}

    /** An unresolved address named label: one source per label. */
    public static InetSocketAddress peer(String label) {
        return InetSocketAddress.createUnresolved(label, 0);
    }

    /** An IP literal as an address, other text as {@link #peer}. */
    public static InetSocketAddress literal(String text) {
        try {
            return new InetSocketAddress(java.net.InetAddress.ofLiteral(text), 0);
        } catch (IllegalArgumentException notLiteral) {
            return peer(text);
        }
    }
}
