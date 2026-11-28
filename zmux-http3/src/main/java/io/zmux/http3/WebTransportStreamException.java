package io.zmux.http3;

import java.io.IOException;
import java.util.OptionalLong;

/** A peer terminated one direction of a WebTransport data stream. Sizes refer to QUIC wire offsets. */
public final class WebTransportStreamException extends IOException {
    private static final long FIRST = 0x52e4a40fa8dbL;
    private final long wireCode, finalSize, reliableSize;
    private final boolean stop;

    WebTransportStreamException(boolean stop, long wireCode, long finalSize, long reliableSize) {
        super((stop ? "STOP_SENDING " : "RESET_STREAM ") + Long.toHexString(wireCode));
        this.stop = stop;
        this.wireCode = wireCode;
        this.finalSize = finalSize;
        this.reliableSize = reliableSize;
    }

    static long wireCode(long application) {
        if (application < 0 || application > 0xffff_ffffL) throw new IllegalArgumentException("WT stream code outside uint32");
        return FIRST + application + application / 30;
    }

    public boolean stopSending() { return stop; }
    public long wireCode() { return wireCode; }
    public OptionalLong applicationCode() {
        long delta = wireCode - FIRST;
        return delta < 0 || wireCode > wireCode(0xffff_ffffL) || delta % 31 == 30
                ? OptionalLong.empty() : OptionalLong.of(delta - delta / 31);
    }
    public OptionalLong finalSize() { return finalSize < 0 || stop ? OptionalLong.empty() : OptionalLong.of(finalSize); }
    public OptionalLong reliableSize() { return reliableSize < 0 || stop ? OptionalLong.empty() : OptionalLong.of(reliableSize); }
}
