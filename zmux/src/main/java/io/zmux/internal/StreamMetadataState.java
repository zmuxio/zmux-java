package io.zmux.internal;

import io.zmux.*;

import java.io.IOException;
import java.util.Objects;

final class StreamMetadataState {
    private static final byte[] EMPTY_BYTES = new byte[0];

    private StreamMetadata metadata = StreamMetadata.empty();
    private boolean openedOnWire;
    private boolean peerVisible;
    private boolean openingFramePending;
    private Long pendingPriorityUpdatePriority;
    private Long pendingPriorityUpdateGroup;
    private byte[] pendingPriorityUpdatePayload = EMPTY_BYTES;
    private boolean pendingPriorityUpdate;
    private boolean priorityUpdateQueued;

    StreamMetadataState(OpenOptions openOptions) {
        OpenOptions normalized = openOptions == null ? OpenOptions.empty() : openOptions;
        byte[] initialOpenInfo = normalized.openInfoLength() == 0 ? EMPTY_BYTES : normalized.openInfo();
        if (normalized.initialPriority() != null || normalized.initialGroup() != null || initialOpenInfo.length > 0) {
            metadata = new StreamMetadata(
                    normalized.initialPriority() == null ? 0L : normalized.initialPriority(),
                    normalizeEffectiveGroup(normalized.initialGroup()),
                    initialOpenInfo
            );
        }
    }

    private static Long normalizeEffectiveGroup(Long group) {
        return group == null || group == 0L ? null : group;
    }

    byte[] openInfo() {
        return metadata.openInfo();
    }

    StreamMetadata metadata() {
        return metadata;
    }

    boolean openedOnWire() {
        return openedOnWire;
    }

    LocalOpenPhase localOpenPhase(boolean openedLocally) {
        return LocalOpenPhase.from(openedLocally, openedOnWire, peerVisible, openingFramePending);
    }

    void markOpenedOnWire() {
        openedOnWire = true;
    }

    boolean peerVisible() {
        return peerVisible;
    }

    boolean shouldMarkPeerVisible(boolean openedLocally, boolean idAssigned) {
        return idAssigned && localOpenPhase(openedLocally).shouldMarkPeerVisible();
    }

    void markPeerVisible() {
        peerVisible = true;
    }

    boolean openingFramePending() {
        return openingFramePending;
    }

    void markOpeningFramePending() {
        openingFramePending = true;
    }

    void clearOpeningFramePending() {
        openingFramePending = false;
    }

    void applyMetadataUpdateLocked(SessionRuntime session,
                                   StreamRuntime streamRuntime,
                                   MetadataUpdate update) throws IOException {
        byte[] currentOpenInfo = metadata.openInfoLength() == 0 ? EMPTY_BYTES : metadata.openInfo();
        StreamMetadata nextMetadata = new StreamMetadata(
                update.priority() == null ? metadata.priority() : update.priority(),
                update.group() == null ? metadata.group() : normalizeEffectiveGroup(update.group()),
                currentOpenInfo
        );
        if (!openedOnWire) {
            validateOpeningMetadataUpdateLocked(session, nextMetadata);
        }
        replaceMetadataLocked(session, streamRuntime, nextMetadata);
    }

    void validateMetadataUpdateAsOpenLocked(SessionRuntime session, MetadataUpdate update) throws IOException {
        byte[] currentOpenInfo = metadata.openInfoLength() == 0 ? EMPTY_BYTES : metadata.openInfo();
        StreamMetadata nextMetadata = new StreamMetadata(
                update.priority() == null ? metadata.priority() : update.priority(),
                update.group() == null ? metadata.group() : normalizeEffectiveGroup(update.group()),
                currentOpenInfo
        );
        validateOpeningMetadataUpdateLocked(session, nextMetadata);
    }

    void applyOpenMetadataLocked(SessionRuntime session,
                                 StreamRuntime streamRuntime,
                                 long priority,
                                 Long group,
                                 byte[] openInfo) {
        long capabilities = session.capabilities();
        long nextPriority = Protocol.canCarryPriorityOnOpen(capabilities) ? priority : metadata.priority();
        Long nextGroup = Protocol.canCarryGroupOnOpen(capabilities) ? normalizeEffectiveGroup(group) : metadata.group();
        byte[] nextOpenInfo = Protocol.canCarryOpenInfo(capabilities)
                ? openInfo
                : (metadata.openInfoLength() == 0 ? EMPTY_BYTES : metadata.openInfo());
        replaceMetadataLocked(session, streamRuntime, new StreamMetadata(nextPriority, nextGroup, nextOpenInfo));
    }

    boolean applyPriorityUpdateLocked(SessionRuntime session,
                                      StreamRuntime streamRuntime,
                                      FrameCodec.ParsedPriorityUpdate update) {
        if (update == null || !update.valid()) {
            return false;
        }
        long capabilities = session.capabilities();
        long nextPriority = update.hasPriority() && Protocol.canCarryPriorityInUpdate(capabilities)
                ? update.priority()
                : metadata.priority();
        Long nextGroup = update.hasGroup() && Protocol.canCarryGroupInUpdate(capabilities)
                ? normalizeEffectiveGroup(update.group())
                : metadata.group();
        if (metadata.priority() == nextPriority && Objects.equals(metadata.group(), nextGroup)) {
            return false;
        }
        byte[] currentOpenInfo = metadata.openInfoLength() == 0 ? EMPTY_BYTES : metadata.openInfo();
        replaceMetadataLocked(session, streamRuntime, new StreamMetadata(nextPriority, nextGroup, currentOpenInfo));
        return true;
    }

    int openInfoLength() {
        return metadata.openInfoLength();
    }

    Long group() {
        return metadata.group();
    }

    long priority() {
        return metadata.priority();
    }

    void clearRetainedOpenInfoLocked(SessionRuntime session, StreamRuntime streamRuntime) {
        if (metadata.openInfoLength() == 0) {
            return;
        }
        replaceMetadataLocked(session, streamRuntime, new StreamMetadata(metadata.priority(), metadata.group(), EMPTY_BYTES));
    }

    byte[] buildOpeningPrefixLocked(SessionRuntime session) throws IOException {
        try {
            return FrameCodec.buildOpenMetadataPrefix(
                    session.capabilities(),
                    metadata.priority() == 0L ? null : metadata.priority(),
                    metadata.group(),
                    metadata.openInfo(),
                    session.peerSettings().maxFramePayload()
            );
        } catch (OpenInfoUnavailableException error) {
            throw new OpenInfoUnavailableException(
                    "write",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    error
            );
        } catch (OpenMetadataTooLargeException error) {
            throw new OpenMetadataTooLargeException(
                    "write",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    error
            );
        }
    }

    void stagePriorityUpdate(Long priority, Long group, byte[] payload) {
        if (priority != null) {
            pendingPriorityUpdatePriority = priority;
        }
        if (group != null || pendingPriorityUpdateGroup == null) {
            pendingPriorityUpdateGroup = group;
        }
        pendingPriorityUpdatePayload = payload == null || payload.length == 0 ? EMPTY_BYTES : payload;
        pendingPriorityUpdate = true;
    }

    boolean hasPendingPriorityUpdate() {
        return pendingPriorityUpdate;
    }

    boolean priorityUpdateQueued() {
        return priorityUpdateQueued;
    }

    Long pendingPriorityUpdatePriority() {
        return pendingPriorityUpdatePriority;
    }

    Long pendingPriorityUpdateGroup() {
        return pendingPriorityUpdateGroup;
    }

    byte[] pendingPriorityUpdatePayload() {
        return pendingPriorityUpdatePayload;
    }

    void markPriorityUpdateQueued() {
        priorityUpdateQueued = true;
    }

    void clearPriorityUpdateQueued() {
        priorityUpdateQueued = false;
    }

    void clearPendingPriorityUpdate() {
        pendingPriorityUpdate = false;
        pendingPriorityUpdatePriority = null;
        pendingPriorityUpdateGroup = null;
        pendingPriorityUpdatePayload = EMPTY_BYTES;
        priorityUpdateQueued = false;
    }

    boolean awaitingPeerVisibility(boolean openedLocally, boolean idAssigned, boolean fullyTerminal) {
        return idAssigned && !fullyTerminal && localOpenPhase(openedLocally).awaitingPeerVisibility();
    }

    boolean canTakePendingPriorityUpdate(boolean openedLocally) {
        return localOpenPhase(openedLocally).canTakePendingPriorityUpdate();
    }

    private void replaceMetadataLocked(SessionRuntime session,
                                       StreamRuntime streamRuntime,
                                       StreamMetadata nextMetadata) {
        int previousOpenInfoLength = metadata.openInfoLength();
        Long previousGroup = metadata.group();
        metadata = nextMetadata == null ? StreamMetadata.empty() : nextMetadata;
        if (previousOpenInfoLength != metadata.openInfoLength()) {
            session.onStreamOpenInfoUpdatedLocked(previousOpenInfoLength, metadata.openInfoLength());
        }
        if (!Objects.equals(previousGroup, metadata.group())) {
            session.onOutboundSchedulingGroupChangedLocked(streamRuntime, previousGroup);
        }
    }

    private void validateOpeningMetadataUpdateLocked(SessionRuntime session, StreamMetadata nextMetadata) throws IOException {
        try {
            FrameCodec.buildOpenMetadataPrefix(
                    session.capabilities(),
                    nextMetadata.priority() == 0L ? null : nextMetadata.priority(),
                    nextMetadata.group(),
                    nextMetadata.openInfo(),
                    session.peerSettings().maxFramePayload()
            );
        } catch (OpenInfoUnavailableException error) {
            throw new OpenInfoUnavailableException(
                    "write",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    error
            );
        } catch (OpenMetadataTooLargeException error) {
            throw new OpenMetadataTooLargeException(
                    "write",
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    error
            );
        } catch (ZmuxException error) {
            throw new ZmuxException(
                    error.code(),
                    "write",
                    error.getMessage(),
                    error,
                    ZmuxErrorScope.STREAM,
                    ZmuxErrorSource.LOCAL,
                    ZmuxErrorDirection.WRITE,
                    ZmuxTerminationKind.UNKNOWN
            );
        }
    }
}
