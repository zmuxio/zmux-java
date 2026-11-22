package io.zmux;

import java.net.InetSocketAddress;

/**
 * Why a listener rejects a first message.
 *
 * @param profile the profile of the first message, 2 for a Join, 0 when it has none
 * @param recordMode the Hello's record mode, or null when it is malformed
 * @param reason the local reason: for a Join {@code malformed} (the first bytes or the Hello do not parse),
 *     {@code pending_limit} (the source is over its bound of carriers waiting to authenticate; the profile is then 0),
 *     {@code unknown_locator}, {@code unknown_suite} (not usable in the found session), {@code binding_unavailable},
 *     {@code memory_pressure} or {@code auth}; a transform's own for a carrier it refused ({@link CarrierRejectedException})
 * @param source the carrier's source, or null
 * @param error the transform's rejection, or null for a listener-generated rejection
 */
public record RejectInfo(int profile, RecordMode recordMode, String reason, InetSocketAddress source,
                         CarrierRejectedException error) {
    public RejectInfo(int profile, RecordMode recordMode, String reason, InetSocketAddress source) {
        this(profile, recordMode, reason, source, null);
    }

    /** What the listener does with a rejected carrier: nothing is written either way. */
    public enum Action { CLOSE, STALL }
}
