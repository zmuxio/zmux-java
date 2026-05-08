package io.zmux.protocol;

import io.zmux.Role;
import io.zmux.Settings;
import java.util.Objects;

public final class Negotiated {
    private final long proto;
    private final long capabilities;
    private final Role localRole;
    private final Role peerRole;
    private final Settings peerSettings;

    public Negotiated(long proto, long capabilities, Role localRole, Role peerRole, Settings peerSettings) {
        this.proto = proto;
        this.capabilities = capabilities;
        this.localRole = localRole;
        this.peerRole = peerRole;
        this.peerSettings = peerSettings;
    }

    public long proto() {
        return proto;
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

    public Role localRole() {
        return localRole;
    }

    public Role peerRole() {
        return peerRole;
    }

    public Settings peerSettings() {
        return peerSettings;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Negotiated)) {
            return false;
        }
        Negotiated that = (Negotiated) other;
        return proto == that.proto
                && capabilities == that.capabilities
                && localRole == that.localRole
                && peerRole == that.peerRole
                && Objects.equals(peerSettings, that.peerSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(proto, capabilities, localRole, peerRole, peerSettings);
    }

    @Override
    public String toString() {
        return "Negotiated[proto=" + proto
                + ", capabilities=" + capabilities
                + ", localRole=" + localRole
                + ", peerRole=" + peerRole
                + ", peerSettings=" + peerSettings
                + "]";
    }
}
