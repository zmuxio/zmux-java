package io.zmux.protocol;

import io.zmux.Role;
import io.zmux.Settings;

import java.util.Objects;

public final class Preface {
    private final byte prefaceVersion;
    private final Role role;
    private final long tieBreakerNonce;
    private final long minProto;
    private final long maxProto;
    private final long capabilities;
    private final Settings settings;

    public Preface(byte prefaceVersion,
                   Role role,
                   long tieBreakerNonce,
                   long minProto,
                   long maxProto,
                   long capabilities,
                   Settings settings) {
        this.prefaceVersion = prefaceVersion;
        this.role = role;
        this.tieBreakerNonce = tieBreakerNonce;
        this.minProto = minProto;
        this.maxProto = maxProto;
        this.capabilities = capabilities;
        this.settings = settings;
    }

    public byte prefaceVersion() {
        return prefaceVersion;
    }

    public Role role() {
        return role;
    }

    public long tieBreakerNonce() {
        return tieBreakerNonce;
    }

    public long minProto() {
        return minProto;
    }

    public long maxProto() {
        return maxProto;
    }

    public long capabilities() {
        return capabilities;
    }

    public boolean hasCapability(long bit) {
        return Protocol.hasCapability(capabilities, bit);
    }

    public boolean supportsOpenMetadata() {
        return Protocol.supportsOpenMetadata(capabilities);
    }

    public boolean supportsPriorityUpdate() {
        return Protocol.supportsPriorityUpdate(capabilities);
    }

    public boolean canCarryOpenInfo() {
        return Protocol.canCarryOpenInfo(capabilities);
    }

    public boolean canCarryPriorityOnOpen() {
        return Protocol.canCarryPriorityOnOpen(capabilities);
    }

    public boolean canCarryGroupOnOpen() {
        return Protocol.canCarryGroupOnOpen(capabilities);
    }

    public boolean canCarryPriorityInUpdate() {
        return Protocol.canCarryPriorityInUpdate(capabilities);
    }

    public boolean canCarryGroupInUpdate() {
        return Protocol.canCarryGroupInUpdate(capabilities);
    }

    public boolean hasPeerVisiblePrioritySemantics() {
        return Protocol.hasPeerVisiblePrioritySemantics(capabilities);
    }

    public boolean hasPeerVisibleGroupSemantics() {
        return Protocol.hasPeerVisibleGroupSemantics(capabilities);
    }

    public Settings settings() {
        return settings;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Preface)) {
            return false;
        }
        Preface that = (Preface) other;
        return prefaceVersion == that.prefaceVersion
                && tieBreakerNonce == that.tieBreakerNonce
                && minProto == that.minProto
                && maxProto == that.maxProto
                && capabilities == that.capabilities
                && role == that.role
                && Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prefaceVersion, role, tieBreakerNonce, minProto, maxProto, capabilities, settings);
    }

    @Override
    public String toString() {
        return "Preface[prefaceVersion=" + prefaceVersion
                + ", role=" + role
                + ", tieBreakerNonce=" + tieBreakerNonce
                + ", minProto=" + minProto
                + ", maxProto=" + maxProto
                + ", capabilities=" + capabilities
                + ", settings=" + settings
                + "]";
    }
}
