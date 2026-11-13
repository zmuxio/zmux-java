package io.zmux.http;

import io.zmux.HttpRedirectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * Redirects of HTTP carriers: a carrier follows a redirect only to its own host name, as another
 * path or query of the same origin or as a scheme upgrade (http to https, ws to wss, on any
 * port), never to another host and never from https to http, so that no credential, pairing
 * token, prefix or pipe byte reaches another origin.
 */
public final class Redirects {
    private Redirects() {}

    /** The number of redirects a carrier request follows. */
    public static final int DEFAULT_MAX = 5;

    /**
     * The uplink bytes a streaming carrier keeps until its first response that is not a
     * redirect, to send them again at the new URL; past them it keeps no more and refuses a
     * later redirect.
     */
    public static final int MAX_REPLAY = 64 << 10;

    /** Whether status is a redirect a carrier considers (301, 302, 303, 307, 308). */
    public static boolean isRedirect(int status) {
        return status == 301 || status == 302 || status == 303 || status == 307 || status == 308;
    }

    /**
     * The URL at which a carrier request with method to from, answered with the redirect status
     * and location, is repeated with the same method, fields and body. It refuses, with an
     * {@link HttpRedirectException}, a redirect to another host name, from a secure scheme
     * (https, wss) to an insecure one, to another port without a scheme upgrade, a Location with
     * userinfo, and a 301, 302 or 303 of a request other than GET or HEAD unless it is a pure
     * scheme upgrade (the same host, path and query over https). The schemes ws and wss count as
     * http and https; the result keeps the family of from and has no fragment.
     *
     * @param status a redirect status ({@link #isRedirect})
     * @param location the response's Location field; null or empty: none
     */
    public static URI check(URI from, String method, int status, String location) throws HttpRedirectException {
        if (location == null || location.isEmpty()) throw new HttpRedirectException(status, "", "no Location");
        URI to;
        try {
            to = from.resolve(new URI(location));
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new HttpRedirectException(status, location, "invalid Location");
        }
        Boolean fromTls = secure(from.getScheme()), toTls = secure(to.getScheme());
        if (fromTls == null) throw new HttpRedirectException(status, location, "unknown scheme " + from.getScheme());
        if (toTls == null) throw new HttpRedirectException(status, location, "unknown scheme " + to.getScheme());
        if (to.getRawUserInfo() != null) throw new HttpRedirectException(status, location, "credentials in the Location");
        if (to.getHost() == null || !to.getHost().equalsIgnoreCase(from.getHost())) {
            throw new HttpRedirectException(status, location, "another host");
        }
        if (fromTls && !toTls) throw new HttpRedirectException(status, location, "downgrade to " + to.getScheme());
        if (fromTls.equals(toTls) && port(to, toTls) != port(from, fromTls)) throw new HttpRedirectException(status, location, "another port");
        boolean upgrade = !fromTls && toTls;
        boolean ws = isWs(from.getScheme());
        String scheme = ws ? toTls ? "wss" : "ws" : toTls ? "https" : "http";
        if (!method.equals("GET") && !method.equals("HEAD") && status != 307 && status != 308) {
            // A 301, 302 or 303 turns a POST into a GET; only a pure scheme upgrade is taken as meant for the same request.
            if (!upgrade || !same(to.getRawPath(), from.getRawPath()) || !same(to.getRawQuery(), from.getRawQuery())) {
                throw new HttpRedirectException(status, location, "would change " + method + " to GET");
            }
        }
        try {
            return new URI(scheme + "://" + to.getRawAuthority() + HttpCarrierClient.target(to));
        } catch (URISyntaxException e) {
            throw new HttpRedirectException(status, location, "invalid Location");
        }
    }

    /** Whether to is from over a secure scheme: the same host, path and query, http to https (or ws to wss). */
    public static boolean schemeUpgradeOf(URI from, URI to) {
        return Boolean.FALSE.equals(secure(from.getScheme())) && Boolean.TRUE.equals(secure(to.getScheme()))
                && from.getHost() != null && from.getHost().equalsIgnoreCase(to.getHost())
                && same(from.getRawPath(), to.getRawPath()) && same(from.getRawQuery(), to.getRawQuery());
    }

    private static boolean same(String a, String b) {
        return (a == null || a.isEmpty() ? "" : a).equals(b == null || b.isEmpty() ? "" : b);
    }

    private static boolean isWs(String s) {
        return s != null && (s.equalsIgnoreCase("ws") || s.equalsIgnoreCase("wss"));
    }

    /** True for https and wss, false for http and ws, null for any other scheme. */
    private static Boolean secure(String s) {
        if (s == null) return null;
        return switch (s.toLowerCase(Locale.ROOT)) {
            case "https", "wss" -> true;
            case "http", "ws" -> false;
            default -> null;
        };
    }

    private static int port(URI u, boolean secure) {
        return u.getPort() > 0 ? u.getPort() : secure ? 443 : 80;
    }
}
