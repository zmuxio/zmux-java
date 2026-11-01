package io.zmux.http;

import io.zmux.RequestHead;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * The intermediaries in front of a server (reverse proxies, load balancers, CDNs) whose report of
 * a carrier's client address is believed (MULTIPATH.md Section 5.6). The source of a carrier,
 * which the admission sees as {@code CarrierInfo.source()} and the listener counts in its
 * per-source limits, is then the client address the intermediary reports; the same report from
 * any other peer is ignored, so that a client cannot choose its own source.
 *
 * <p>Two mechanisms carry the report, each enabled on its own: the PROXY protocol (version 1 or
 * 2) at the start of every connection from a trusted address ({@link #proxyProtocol()}), and a
 * forwarding header field on HTTP requests from a trusted address ({@link #header()}). Immutable.
 */
public final class TrustedProxies {
    private final List<String> prefixes;
    private final List<Prefix> parsed;
    private final String header;
    private final boolean proxyProtocol;

    private TrustedProxies(List<String> prefixes, String header, boolean proxyProtocol) {
        this.prefixes = List.copyOf(prefixes);
        var p = new ArrayList<Prefix>(prefixes.size());
        for (String s : this.prefixes) p.add(Prefix.parse(s));
        this.parsed = List.copyOf(p);
        this.header = Objects.requireNonNull(header);
        if (!header.isEmpty() && !header.chars().allMatch(TrustedProxies::tokenChar)) {
            throw new IllegalArgumentException("not a header field name: " + header);
        }
        this.proxyProtocol = proxyProtocol;
    }

    /**
     * The intermediaries at these addresses and networks, in CIDR notation ("192.0.2.1",
     * "10.0.0.0/8", "2001:db8::/32"); no report is read until {@link #withHeader} or
     * {@link #withProxyProtocol} enables one.
     */
    public static TrustedProxies of(List<String> prefixes) {
        return new TrustedProxies(prefixes, "", false);
    }

    /**
     * The forwarding header field the trusted intermediaries set on HTTP requests:
     * "X-Forwarded-For", "Forwarded" (RFC 7239), "X-Real-IP", or the name of another field that
     * holds the one client address as X-Real-IP does. Empty: HTTP requests are not read.
     */
    public TrustedProxies withHeader(String name) { return new TrustedProxies(prefixes, name, proxyProtocol); }

    /** Expects a PROXY protocol header on every connection from a trusted address. */
    public TrustedProxies withProxyProtocol(boolean on) { return new TrustedProxies(prefixes, header, on); }

    public List<String> prefixes() { return prefixes; }

    public String header() { return header; }

    public boolean proxyProtocol() { return proxyProtocol; }

    /** Whether address is one of the trusted intermediaries. */
    public boolean trusts(InetAddress address) {
        byte[] a = unmap(address.getAddress());
        for (Prefix p : parsed) if (p.contains(a)) return true;
        return false;
    }

    /**
     * The source of an HTTP request that arrived from peer with these header fields, the one resolver of the
     * trusted-intermediary configuration (API.md Section 4.7): the client's address and port that the trusted
     * intermediary reports in the configured header field (port 0 when it reports none), or peer itself when peer
     * is not trusted or the fields name no client.
     */
    public InetSocketAddress source(InetSocketAddress peer, RequestHead headers) {
        Objects.requireNonNull(peer);
        if (header.isEmpty() || peer.getAddress() == null || !trusts(peer.getAddress())) return peer;
        InetSocketAddress reported = resolve(headers);
        return reported != null ? reported : peer;
    }

    /** The source of a peer without a report: its own address. */
    static String text(InetAddress a) {
        try {
            return InetAddress.getByAddress(unmap(a.getAddress())).getHostAddress();
        } catch (java.net.UnknownHostException impossible) {
            throw new AssertionError(impossible);
        }
    }

    /** The reported client, walking the entries from the right past the trusted ones; null: none. */
    private InetSocketAddress resolve(RequestHead headers) {
        List<String> lines = headers.header(header);
        String name = header.toLowerCase(Locale.ROOT);
        InetSocketAddress found = null;
        if (name.equals("forwarded")) {
            for (int i = lines.size() - 1; i >= 0; i--) {
                List<String> elements = splitQuoted(lines.get(i), ',');
                for (int j = elements.size() - 1; j >= 0; j--) {
                    String e = elements.get(j).strip();
                    if (e.isEmpty()) continue;
                    String node = forwardedFor(e);
                    InetSocketAddress a = node == null ? null : parseNode(node);
                    if (a == null) return found;
                    found = a;
                    if (!trusts(a.getAddress())) return found;
                }
            }
            return found;
        }
        if (name.equals("x-forwarded-for")) {
            for (int i = lines.size() - 1; i >= 0; i--) {
                String[] entries = lines.get(i).split(",", -1);
                for (int j = entries.length - 1; j >= 0; j--) {
                    String e = entries[j].strip();
                    if (e.isEmpty()) continue;
                    InetSocketAddress a = parseNode(e);
                    if (a == null) return found;
                    found = a;
                    if (!trusts(a.getAddress())) return found;
                }
            }
            return found;
        }
        // X-Real-IP and other single-address fields: the last line.
        if (lines.isEmpty()) return null;
        String v = lines.getLast().strip();
        return v.isEmpty() ? null : parseNode(v);
    }

    /** The node of the for= parameter of a Forwarded element; null without one. */
    private static String forwardedFor(String element) {
        for (String pair : splitQuoted(element, ';')) {
            int eq = pair.indexOf('=');
            if (eq < 0) continue;
            if (!pair.substring(0, eq).strip().equalsIgnoreCase("for")) continue;
            String v = pair.substring(eq + 1).strip();
            if (v.length() >= 2 && v.charAt(0) == '"' && v.charAt(v.length() - 1) == '"') {
                v = v.substring(1, v.length() - 1).replaceAll("\\\\(.)", "$1");
            }
            return v;
        }
        return null;
    }

    /** Splits s at sep outside quoted strings. */
    private static List<String> splitQuoted(String s, char sep) {
        var out = new ArrayList<String>();
        boolean quoted = false;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quoted && c == '\\') { i++; continue; }
            if (c == '"') quoted = !quoted;
            else if (c == sep && !quoted) { out.add(s.substring(start, i)); start = i + 1; }
        }
        out.add(s.substring(start));
        return out;
    }

    /**
     * An address node: "ip", "ip:port", "[ipv6]", "[ipv6]:port" or a bare IPv6 address, port 0 without one; null
     * otherwise.
     */
    static InetSocketAddress parseNode(String v) {
        String host = v;
        int portNumber = 0;
        if (v.startsWith("[")) {
            int end = v.indexOf(']');
            if (end < 0) return null;
            String rest = v.substring(end + 1);
            if (!rest.isEmpty() && !(rest.startsWith(":") && port(rest.substring(1)))) return null;
            if (!rest.isEmpty()) portNumber = Integer.parseInt(rest.substring(1));
            host = v.substring(1, end);
            if (host.indexOf(':') < 0) return null;
        } else {
            int colon = v.indexOf(':');
            if (colon >= 0 && colon == v.lastIndexOf(':')) {
                if (!port(v.substring(colon + 1))) return null;
                portNumber = Integer.parseInt(v.substring(colon + 1));
                host = v.substring(0, colon);
                if (host.indexOf('.') < 0) return null;
            } else if (colon < 0 && v.indexOf('.') < 0) {
                return null;
            }
        }
        if (host.indexOf('%') >= 0) return null;
        try {
            return new InetSocketAddress(InetAddress.ofLiteral(host), portNumber);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static boolean port(String s) {
        if (s.isEmpty() || s.length() > 5) return false;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) < '0' || s.charAt(i) > '9') return false;
        return Integer.parseInt(s) <= 65535;
    }

    /** An IPv4-mapped IPv6 address as its IPv4 address. */
    static byte[] unmap(byte[] a) {
        if (a.length != 16) return a;
        for (int i = 0; i < 10; i++) if (a[i] != 0) return a;
        if (a[10] != (byte) 0xff || a[11] != (byte) 0xff) return a;
        return java.util.Arrays.copyOfRange(a, 12, 16);
    }

    private static boolean tokenChar(int c) {
        return c > 0x20 && c < 0x7f && "\"(),/:;<=>?@[\\]{}".indexOf(c) < 0;
    }

    /** An address prefix. */
    private record Prefix(byte[] addr, int bits) {
        static Prefix parse(String s) {
            int slash = s.indexOf('/');
            String host = slash < 0 ? s : s.substring(0, slash);
            byte[] a;
            try {
                a = unmap(InetAddress.ofLiteral(host).getAddress());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("not an address prefix: " + s, e);
            }
            int max = a.length * 8, bits = max;
            if (slash >= 0) {
                try { bits = Integer.parseInt(s.substring(slash + 1)); }
                catch (NumberFormatException e) { throw new IllegalArgumentException("not an address prefix: " + s, e); }
                // An IPv4-mapped prefix counts its bits within the IPv4 address.
                if (a.length == 4 && host.indexOf(':') >= 0) bits -= 96;
                if (bits < 0 || bits > max) throw new IllegalArgumentException("not an address prefix: " + s);
            }
            return new Prefix(a, bits);
        }

        boolean contains(byte[] b) {
            if (b.length != addr.length) return false;
            int full = bits / 8, rest = bits % 8;
            for (int i = 0; i < full; i++) if (b[i] != addr[i]) return false;
            if (rest == 0) return true;
            int mask = 0xff << (8 - rest) & 0xff;
            return (b[full] & mask) == (addr[full] & mask);
        }
    }

    @Override public String toString() {
        return "TrustedProxies" + prefixes + (header.isEmpty() ? "" : " header " + header) + (proxyProtocol ? " PROXY" : "");
    }
}
