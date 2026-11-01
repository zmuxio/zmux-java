package io.zmux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The HTTP request of a carrier, as the application's HTTP server received it: its header
 * fields and whether the carrier is decided on this head.
 * An HTTP adapter gives it to {@link ZmuxListener#handle} with the carrier ({@link JoinHint#request()});
 * the admission sees it through {@link Admission.CarrierInfo}.
 *
 * @param headers the request's header fields, names compared without regard to case; fields
 *     whose names differ only in case are one field with the values of both, in the given order
 * @param head the carrier is decided on this head, without a prefix: an HTTP upgrade
 *     (WebSocket, WebTransport) or an HTTP request carrier whose request has no body (such as
 *     the downlink GET of a split bootstrap); a credential then travels in the header field
 *     {@link PrefixAuth#HEADER_FIELD}
 */
public record RequestHead(Map<String, List<String>> headers, boolean head) {
    /** No request: a carrier that is not an HTTP request. */
    public static final RequestHead NONE = new RequestHead(Map.of(), false);

    public RequestHead {
        Objects.requireNonNull(headers, "headers");
        TreeMap<String, List<String>> fields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        headers.forEach((name, values) -> {
            Objects.requireNonNull(name, "header name");
            List<String> copy = List.copyOf(values);
            fields.merge(name, copy, (a, b) -> {
                var merged = new ArrayList<String>(a.size() + b.size());
                merged.addAll(a);
                merged.addAll(b);
                return List.copyOf(merged);
            });
        });
        headers = Collections.unmodifiableMap(fields);
    }

    /** An HTTP request carrier with these header fields; head as {@link #head()}. */
    public static RequestHead of(Map<String, List<String>> headers, boolean head) {
        return new RequestHead(headers, head);
    }

    /** The values of the header field name, in order, its name compared without regard to case; empty when absent. */
    public List<String> header(String name) {
        return headers.getOrDefault(Objects.requireNonNull(name, "name"), List.of());
    }
}
