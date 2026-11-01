package io.zmux;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;

/**
 * The out-of-band metadata of a carrier given to {@link ZmuxListener#handle}, supplied by an
 * HTTP or other adapter, or by a host: its join or pairing header, its addresses, and the
 * HTTP request it arrived with.
 *
 * @param join the value of the carrier's {@link #JOIN_HEADER} field ("": none)
 * @param pairing the value of the carrier's {@link #PAIR_HEADER} field ("": none). A half
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
public record JoinHint(String join, String pairing, boolean uplink, InetSocketAddress source, InetSocketAddress peer,
                       RequestHead request) {
    public static final String JOIN_HEADER = "Zmux-Pipe-Join";
    public static final String PAIR_HEADER = "Zmux-Pipe-Pair";

    public JoinHint {
        join = join == null ? "" : join;
        pairing = pairing == null ? "" : pairing;
        if (source == null) source = peer;
        request = request == null ? RequestHead.NONE : request;
    }

    /** A carrier classified by its in-band first record, from peer, its source the peer. */
    public static JoinHint inBand(InetSocketAddress peer) { return new JoinHint("", "", false, null, peer, RequestHead.NONE); }
    public static JoinHint join(String value, InetSocketAddress peer) { return new JoinHint(value, "", false, null, peer, RequestHead.NONE); }
    public static JoinHint pair(String token, boolean uplink, InetSocketAddress peer) { return new JoinHint("", token, uplink, null, peer, RequestHead.NONE); }

    /**
     * The hint of an HTTP request carrier from peer: its join and pairing are the request's
     * {@link #JOIN_HEADER} and {@link #PAIR_HEADER} fields (a field given more than once is
     * malformed, and the carrier is closed after its admission), uplink applies to a pairing.
     */
    public static JoinHint of(RequestHead request, boolean uplink, InetSocketAddress peer) {
        Objects.requireNonNull(request, "request");
        return new JoinHint(single(request.header(JOIN_HEADER)), single(request.header(PAIR_HEADER)), uplink, null, peer, request);
    }

    /** The one value of a field, "" without one; several values join into one that does not decode. */
    private static String single(List<String> values) {
        return values.isEmpty() ? "" : values.size() == 1 ? values.getFirst() : String.join(",", values);
    }

    /** This hint with the carrier's HTTP request. */
    public JoinHint withRequest(RequestHead r) { return new JoinHint(join, pairing, uplink, source, peer, r); }

    /** This hint with the source the host resolved (null: the peer). */
    public JoinHint withSource(InetSocketAddress s) { return new JoinHint(join, pairing, uplink, s, peer, request); }
}
