package io.zmux.internal;

import io.zmux.Negotiated;
import io.zmux.Preface;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Objects;

final class SessionEstablishmentCoordinatorOwner implements SessionEstablishmentCoordinator.Owner {
    private final SessionRuntime owner;

    SessionEstablishmentCoordinatorOwner(SessionRuntime owner) {
        this.owner = Objects.requireNonNull(owner, "owner");
    }

    @Override
    public FrameCodec.Decoder input() {
        return this.owner.inputInternal();
    }

    @Override
    public BufferedOutputStream output() {
        return this.owner.outputInternal();
    }

    @Override
    public Preface localPreface() {
        return this.owner.localPreface();
    }

    @Override
    public Object lock() {
        return this.owner.lock();
    }

    @Override
    public void markReadyLocked(Preface remotePreface, Negotiated negotiated, long readyAtNanos) {
        this.owner.markReadyLocked(remotePreface, negotiated, readyAtNanos);
    }

    @Override
    public void notifyLockWaiters() {
        this.owner.notifyLockWaitersLocked();
    }

    @Override
    public Runnable readerLoopTask() {
        return this.owner.readerLoopTaskInternal();
    }

    @Override
    public Runnable writerLoopTask() {
        return this.owner.writerLoopTaskInternal();
    }

    @Override
    public IOException sessionInternalError(String operation, String message) {
        return SessionRuntime.sessionInternalError(operation, message);
    }

    @Override
    public IOException sessionInternalError(String operation, String message, Throwable cause) {
        return SessionRuntime.sessionInternalError(operation, message, cause);
    }

    @Override
    public void closeTransport() {
        this.owner.closeTransport();
    }
}
