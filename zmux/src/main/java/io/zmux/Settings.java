package io.zmux;

import java.util.Objects;

public final class Settings {
    private static final Settings DEFAULTS = new Settings(
            65_536L,
            65_536L,
            65_536L,
            262_144L,
            256L,
            256L,
            16_384L,
            0L,
            0L,
            4_096L,
            4_096L,
            SchedulerHint.UNSPECIFIED_OR_BALANCED
    );

    private final long initialMaxStreamDataBidiLocallyOpened;
    private final long initialMaxStreamDataBidiPeerOpened;
    private final long initialMaxStreamDataUni;
    private final long initialMaxData;
    private final long maxIncomingStreamsBidi;
    private final long maxIncomingStreamsUni;
    private final long maxFramePayload;
    private final long idleTimeoutMillis;
    private final long keepaliveHintMillis;
    private final long maxControlPayloadBytes;
    private final long maxExtensionPayloadBytes;
    private final SchedulerHint schedulerHints;

    public Settings(long initialMaxStreamDataBidiLocallyOpened,
                    long initialMaxStreamDataBidiPeerOpened,
                    long initialMaxStreamDataUni,
                    long initialMaxData,
                    long maxIncomingStreamsBidi,
                    long maxIncomingStreamsUni,
                    long maxFramePayload,
                    long idleTimeoutMillis,
                    long keepaliveHintMillis,
                    long maxControlPayloadBytes,
                    long maxExtensionPayloadBytes,
                    SchedulerHint schedulerHints) {
        requireVarint62(initialMaxStreamDataBidiLocallyOpened, "initialMaxStreamDataBidiLocallyOpened");
        requireVarint62(initialMaxStreamDataBidiPeerOpened, "initialMaxStreamDataBidiPeerOpened");
        requireVarint62(initialMaxStreamDataUni, "initialMaxStreamDataUni");
        requireVarint62(initialMaxData, "initialMaxData");
        requireVarint62(maxIncomingStreamsBidi, "maxIncomingStreamsBidi");
        requireVarint62(maxIncomingStreamsUni, "maxIncomingStreamsUni");
        requireVarint62(maxFramePayload, "maxFramePayload");
        requireVarint62(idleTimeoutMillis, "idleTimeoutMillis");
        requireVarint62(keepaliveHintMillis, "keepaliveHintMillis");
        requireVarint62(maxControlPayloadBytes, "maxControlPayloadBytes");
        requireVarint62(maxExtensionPayloadBytes, "maxExtensionPayloadBytes");
        this.initialMaxStreamDataBidiLocallyOpened = initialMaxStreamDataBidiLocallyOpened;
        this.initialMaxStreamDataBidiPeerOpened = initialMaxStreamDataBidiPeerOpened;
        this.initialMaxStreamDataUni = initialMaxStreamDataUni;
        this.initialMaxData = initialMaxData;
        this.maxIncomingStreamsBidi = maxIncomingStreamsBidi;
        this.maxIncomingStreamsUni = maxIncomingStreamsUni;
        this.maxFramePayload = maxFramePayload;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.keepaliveHintMillis = keepaliveHintMillis;
        this.maxControlPayloadBytes = maxControlPayloadBytes;
        this.maxExtensionPayloadBytes = maxExtensionPayloadBytes;
        this.schedulerHints = schedulerHints == null ? SchedulerHint.UNSPECIFIED_OR_BALANCED : schedulerHints;
    }

    public static Settings defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static void requireVarint62(long value, String field) {
        if (value < 0L) {
            throw new IllegalArgumentException("zmux settings " + field + " must be >= 0");
        }
        if (value > Protocol.MAX_VARINT62) {
            throw new IllegalArgumentException("zmux settings " + field + " must be within varint62 range");
        }
    }

    public Builder toBuilder() {
        return new Builder()
                .initialMaxStreamDataBidiLocallyOpened(initialMaxStreamDataBidiLocallyOpened)
                .initialMaxStreamDataBidiPeerOpened(initialMaxStreamDataBidiPeerOpened)
                .initialMaxStreamDataUni(initialMaxStreamDataUni)
                .initialMaxData(initialMaxData)
                .maxIncomingStreamsBidi(maxIncomingStreamsBidi)
                .maxIncomingStreamsUni(maxIncomingStreamsUni)
                .maxFramePayload(maxFramePayload)
                .idleTimeoutMillis(idleTimeoutMillis)
                .keepaliveHintMillis(keepaliveHintMillis)
                .maxControlPayloadBytes(maxControlPayloadBytes)
                .maxExtensionPayloadBytes(maxExtensionPayloadBytes)
                .schedulerHints(schedulerHints);
    }

    public Limits limits() {
        return new Limits(maxFramePayload, maxControlPayloadBytes, maxExtensionPayloadBytes);
    }

    public long initialMaxStreamDataBidiLocallyOpened() {
        return initialMaxStreamDataBidiLocallyOpened;
    }

    public long initialMaxStreamDataBidiPeerOpened() {
        return initialMaxStreamDataBidiPeerOpened;
    }

    public long initialMaxStreamDataUni() {
        return initialMaxStreamDataUni;
    }

    public long initialMaxData() {
        return initialMaxData;
    }

    public long maxIncomingStreamsBidi() {
        return maxIncomingStreamsBidi;
    }

    public long maxIncomingStreamsUni() {
        return maxIncomingStreamsUni;
    }

    public long maxFramePayload() {
        return maxFramePayload;
    }

    public long idleTimeoutMillis() {
        return idleTimeoutMillis;
    }

    public long keepaliveHintMillis() {
        return keepaliveHintMillis;
    }

    public long maxControlPayloadBytes() {
        return maxControlPayloadBytes;
    }

    public long maxExtensionPayloadBytes() {
        return maxExtensionPayloadBytes;
    }

    public SchedulerHint schedulerHints() {
        return schedulerHints;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Settings)) {
            return false;
        }
        Settings that = (Settings) other;
        return initialMaxStreamDataBidiLocallyOpened == that.initialMaxStreamDataBidiLocallyOpened
                && initialMaxStreamDataBidiPeerOpened == that.initialMaxStreamDataBidiPeerOpened
                && initialMaxStreamDataUni == that.initialMaxStreamDataUni
                && initialMaxData == that.initialMaxData
                && maxIncomingStreamsBidi == that.maxIncomingStreamsBidi
                && maxIncomingStreamsUni == that.maxIncomingStreamsUni
                && maxFramePayload == that.maxFramePayload
                && idleTimeoutMillis == that.idleTimeoutMillis
                && keepaliveHintMillis == that.keepaliveHintMillis
                && maxControlPayloadBytes == that.maxControlPayloadBytes
                && maxExtensionPayloadBytes == that.maxExtensionPayloadBytes
                && schedulerHints == that.schedulerHints;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                initialMaxStreamDataBidiLocallyOpened,
                initialMaxStreamDataBidiPeerOpened,
                initialMaxStreamDataUni,
                initialMaxData,
                maxIncomingStreamsBidi,
                maxIncomingStreamsUni,
                maxFramePayload,
                idleTimeoutMillis,
                keepaliveHintMillis,
                maxControlPayloadBytes,
                maxExtensionPayloadBytes,
                schedulerHints
        );
    }

    @Override
    public String toString() {
        return "Settings[initialMaxStreamDataBidiLocallyOpened=" + initialMaxStreamDataBidiLocallyOpened
                + ", initialMaxStreamDataBidiPeerOpened=" + initialMaxStreamDataBidiPeerOpened
                + ", initialMaxStreamDataUni=" + initialMaxStreamDataUni
                + ", initialMaxData=" + initialMaxData
                + ", maxIncomingStreamsBidi=" + maxIncomingStreamsBidi
                + ", maxIncomingStreamsUni=" + maxIncomingStreamsUni
                + ", maxFramePayload=" + maxFramePayload
                + ", idleTimeoutMillis=" + idleTimeoutMillis
                + ", keepaliveHintMillis=" + keepaliveHintMillis
                + ", maxControlPayloadBytes=" + maxControlPayloadBytes
                + ", maxExtensionPayloadBytes=" + maxExtensionPayloadBytes
                + ", schedulerHints=" + schedulerHints
                + "]";
    }

    public static final class Builder {
        private long initialMaxStreamDataBidiLocallyOpened = DEFAULTS.initialMaxStreamDataBidiLocallyOpened();
        private long initialMaxStreamDataBidiPeerOpened = DEFAULTS.initialMaxStreamDataBidiPeerOpened();
        private long initialMaxStreamDataUni = DEFAULTS.initialMaxStreamDataUni();
        private long initialMaxData = DEFAULTS.initialMaxData();
        private long maxIncomingStreamsBidi = DEFAULTS.maxIncomingStreamsBidi();
        private long maxIncomingStreamsUni = DEFAULTS.maxIncomingStreamsUni();
        private long maxFramePayload = DEFAULTS.maxFramePayload();
        private long idleTimeoutMillis = DEFAULTS.idleTimeoutMillis();
        private long keepaliveHintMillis = DEFAULTS.keepaliveHintMillis();
        private long maxControlPayloadBytes = DEFAULTS.maxControlPayloadBytes();
        private long maxExtensionPayloadBytes = DEFAULTS.maxExtensionPayloadBytes();
        private SchedulerHint schedulerHints = DEFAULTS.schedulerHints();

        public Builder initialMaxStreamDataBidiLocallyOpened(long value) {
            this.initialMaxStreamDataBidiLocallyOpened = value;
            return this;
        }

        public Builder initialMaxStreamDataBidiPeerOpened(long value) {
            this.initialMaxStreamDataBidiPeerOpened = value;
            return this;
        }

        public Builder initialMaxStreamDataUni(long value) {
            this.initialMaxStreamDataUni = value;
            return this;
        }

        public Builder initialMaxData(long value) {
            this.initialMaxData = value;
            return this;
        }

        public Builder maxIncomingStreamsBidi(long value) {
            this.maxIncomingStreamsBidi = value;
            return this;
        }

        public Builder maxIncomingStreamsUni(long value) {
            this.maxIncomingStreamsUni = value;
            return this;
        }

        public Builder maxFramePayload(long value) {
            this.maxFramePayload = value;
            return this;
        }

        public Builder idleTimeoutMillis(long value) {
            this.idleTimeoutMillis = value;
            return this;
        }

        public Builder keepaliveHintMillis(long value) {
            this.keepaliveHintMillis = value;
            return this;
        }

        public Builder maxControlPayloadBytes(long value) {
            this.maxControlPayloadBytes = value;
            return this;
        }

        public Builder maxExtensionPayloadBytes(long value) {
            this.maxExtensionPayloadBytes = value;
            return this;
        }

        public Builder schedulerHints(SchedulerHint value) {
            this.schedulerHints = value == null ? SchedulerHint.UNSPECIFIED_OR_BALANCED : value;
            return this;
        }

        public Settings build() {
            return new Settings(
                    initialMaxStreamDataBidiLocallyOpened,
                    initialMaxStreamDataBidiPeerOpened,
                    initialMaxStreamDataUni,
                    initialMaxData,
                    maxIncomingStreamsBidi,
                    maxIncomingStreamsUni,
                    maxFramePayload,
                    idleTimeoutMillis,
                    keepaliveHintMillis,
                    maxControlPayloadBytes,
                    maxExtensionPayloadBytes,
                    schedulerHints
            );
        }
    }
}
