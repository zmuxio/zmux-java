package io.zmux.http;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * The PROXY protocol header (version 1 or 2) that a trusted intermediary writes at the start of a
 * connection to report the client's address ({@link TrustedProxies#proxyProtocol()}).
 *
 * @param length the header's length in bytes: the connection's own bytes follow it
 * @param source the client's address; null when the header reports none (a LOCAL command, an
 *     UNKNOWN or unspecified family): the connection is then its own source
 */
public record ProxyHeader(int length, InetSocketAddress source) {
    private static final byte[] V2 = {0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a};
    private static final byte[] V1 = "PROXY ".getBytes(StandardCharsets.US_ASCII);
    /** The longest version 1 header, CRLF included. */
    private static final int V1_MAX = 107;

    /**
     * Parses the header at the start of b (from its position, which is left unchanged). Null when
     * b holds only part of one.
     *
     * @throws ProtocolException when b does not start with a valid header
     */
    public static ProxyHeader parse(ByteBuffer b) throws ProtocolException {
        ByteBuffer d = b.duplicate();
        int n = d.remaining(), p = d.position();
        if (n == 0) return null;
        if (d.get(p) == 0x0d) {
            for (int i = 0; i < Math.min(n, V2.length); i++) if (d.get(p + i) != V2[i]) throw new ProtocolException("not a PROXY header");
            if (n < 16) return null;
            int verCmd = d.get(p + 12) & 0xff, fam = d.get(p + 13) & 0xff, len = d.getShort(p + 14) & 0xffff;
            if (verCmd >> 4 != 2 || (verCmd & 0xf) > 1) throw new ProtocolException("PROXY header version or command");
            if (n < 16 + len) return null;
            if ((verCmd & 0xf) == 0) return new ProxyHeader(16 + len, null); // LOCAL
            return switch (fam >> 4) {
                case 1 -> {
                    if (len < 12) throw new ProtocolException("PROXY header address length");
                    yield new ProxyHeader(16 + len, address(d, p + 16, 4, p + 24));
                }
                case 2 -> {
                    if (len < 36) throw new ProtocolException("PROXY header address length");
                    yield new ProxyHeader(16 + len, address(d, p + 16, 16, p + 48));
                }
                default -> new ProxyHeader(16 + len, null);
            };
        }
        for (int i = 0; i < Math.min(n, V1.length); i++) if (d.get(p + i) != V1[i]) throw new ProtocolException("not a PROXY header");
        int end = -1;
        for (int i = V1.length; i < Math.min(n, V1_MAX); i++) {
            if (d.get(p + i) == '\n') { end = i; break; }
        }
        if (end < 0) {
            if (n >= V1_MAX) throw new ProtocolException("PROXY header too long");
            return null;
        }
        if (d.get(p + end - 1) != '\r') throw new ProtocolException("PROXY header line end");
        byte[] line = new byte[end - 1];
        d.get(p, line);
        String[] f = new String(line, StandardCharsets.US_ASCII).split(" ", -1);
        if (f.length >= 2 && f[1].equals("UNKNOWN")) return new ProxyHeader(end + 1, null);
        if (f.length != 6 || !f[1].equals("TCP4") && !f[1].equals("TCP6")) throw new ProtocolException("PROXY header fields");
        InetAddress src;
        try {
            src = InetAddress.ofLiteral(f[2]);
        } catch (IllegalArgumentException e) {
            throw new ProtocolException("PROXY header address");
        }
        if ((src.getAddress().length == 4) != f[1].equals("TCP4") || f[2].indexOf('%') >= 0) throw new ProtocolException("PROXY header address");
        return new ProxyHeader(end + 1, new InetSocketAddress(src, port(f[4])));
    }

    private static InetSocketAddress address(ByteBuffer d, int at, int len, int portAt) throws ProtocolException {
        byte[] a = new byte[len];
        d.get(at, a);
        try {
            return new InetSocketAddress(InetAddress.getByAddress(a), d.getShort(portAt) & 0xffff);
        } catch (java.net.UnknownHostException e) {
            throw new ProtocolException("PROXY header address");
        }
    }

    private static int port(String s) throws ProtocolException {
        if (s.isEmpty() || s.length() > 5 || !s.chars().allMatch(c -> c >= '0' && c <= '9') || s.length() > 1 && s.charAt(0) == '0') {
            throw new ProtocolException("PROXY header port");
        }
        int v = Integer.parseInt(s);
        if (v > 65535) throw new ProtocolException("PROXY header port");
        return v;
    }
}
