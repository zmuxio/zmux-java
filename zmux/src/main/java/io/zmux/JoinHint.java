package io.zmux;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

/**
 * The metadata of a carrier given to {@link io.zmux.adapter.ListenerAdapter#handle}, supplied by an
 * HTTP or other adapter, or by a host: its pairing header, its addresses, and the
 * HTTP request it arrived with.
 *
 * @param pairing the value of the carrier's pairing field ({@link #PAIR_HEADER} unless configured; "": none). A half
 *     waits for its partner in one of 64 pairing slots of the listener, at most 8 per source;
 *     without an {@link Admission} in front of it, a few sources can hold them all, so a split
 *     bootstrap that the open Internet reaches is served with one
 * @param uplink with a pairing: the carrier is the initiator's sending half
 * @param source the carrier's source ({@link Admission.CarrierInfo#source()}): what the host
 *     or a trusted intermediary resolved; null (not resolved) is the peer
 * @param peer the address the carrier arrived from directly; null when unknown
 * @param request the HTTP request of the carrier, which the admission sees
 *     ({@link Admission.CarrierInfo#head()}, {@link Admission.CarrierInfo#header}); {@link
 *     RequestHead#NONE} for a carrier that is not an HTTP request
 */
public record JoinHint(String pairing, boolean uplink, InetSocketAddress source, InetSocketAddress peer,
                       RequestHead request) {
    /** The default request field that pairs the halves of a split bootstrap. */
    public static final String PAIR_HEADER = "Zmux-Pipe-Pair";

    private static final java.util.Set<String> RESERVED = java.util.Set.of(
            "content-length", "content-type", "content-encoding", "transfer-encoding", "host", "connection", "upgrade",
            "keep-alive", "te", "trailer", "authorization", "cookie", "expect", "date", "early-data", "forwarded", "via",
            "location", "origin");
    private static final List<String> RESERVED_PREFIXES = List.of("proxy-", "sec-websocket-", "x-forwarded-");

    /**
     * The pairing field to use for a configured name: null or empty is {@link #PAIR_HEADER}. Another name
     * must be an HTTP field name (a token) that no protocol field of HTTP, WebSocket or the adapters uses,
     * and is for paths where an intermediary strips unknown fields; both ends must use the same one.
     *
     * @throws IllegalArgumentException when field is not a token or is reserved
     */
    public static String pairField(String field) {
        if (field == null || field.isEmpty()) return PAIR_HEADER;
        for (int i = 0; i < field.length(); i++) {
            char c = field.charAt(i);
            boolean token = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || "!#$%&'*+-.^_`|~".indexOf(c) >= 0;
            if (!token) throw new IllegalArgumentException("pairing field \"" + field + "\" is not a header field name");
        }
        String l = field.toLowerCase(java.util.Locale.ROOT);
        boolean reserved = RESERVED.contains(l) || PrefixAuth.HEADER_FIELD.equalsIgnoreCase(l);
        for (String p : RESERVED_PREFIXES) reserved |= l.startsWith(p);
        if (reserved) throw new IllegalArgumentException("pairing field \"" + field + "\" is reserved");
        return field;
    }

    public JoinHint {
        pairing = pairing == null ? "" : pairing;
        if (source == null) source = peer;
        request = request == null ? RequestHead.NONE : request;
    }

    /** A carrier classified by its in-band first record, from peer, its source the peer. */
    public static JoinHint inBand(InetSocketAddress peer) { return new JoinHint("", false, null, peer, RequestHead.NONE); }
        public static JoinHint pair(String token, boolean uplink, InetSocketAddress peer) { return new JoinHint(token, uplink, null, peer, RequestHead.NONE); }

    /**
     * The hint of an HTTP request carrier from peer: its pairing is the request's
     * {@link #PAIR_HEADER} field (a field given more than once is
     * malformed, and the carrier is closed after its admission), uplink applies to a pairing.
     */
    public static JoinHint of(RequestHead request, boolean uplink, InetSocketAddress peer) {
        return of(request, PAIR_HEADER, uplink, peer);
    }

    /** {@link #of(RequestHead, boolean, InetSocketAddress)} with the pairing in another request field (see {@link #pairField}). */
    public static JoinHint of(RequestHead request, String pairField, boolean uplink, InetSocketAddress peer) {
        Objects.requireNonNull(request, "request");
        return new JoinHint(single(request.header(pairField(pairField))), uplink, null, peer, request);
    }

    /** The one value of a field, "" without one; several values join into one that does not decode. */
    private static String single(List<String> values) {
        return values.isEmpty() ? "" : values.size() == 1 ? values.getFirst() : String.join(",", values);
    }

    /** This hint with the carrier's HTTP request. */
    public JoinHint withRequest(RequestHead r) { return new JoinHint(pairing, uplink, source, peer, r); }

    /** This hint with the source the host resolved (null: the peer). */
    public JoinHint withSource(InetSocketAddress s) { return new JoinHint(pairing, uplink, s, peer, request); }
}
