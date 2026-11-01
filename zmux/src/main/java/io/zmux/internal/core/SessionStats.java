package io.zmux.internal.core;

/** Exact counters of a session, updated on its event loop by the component that observes each count. */
public final class SessionStats {
    private long bytesSent;
    private long bytesReceived;
    private long recordsSent;
    private long recordsReceived;
    private long streamsOpenedLocal;
    private long streamsOpenedRemote;
    private long datagramsSent;
    private long datagramsReceived;
    private long datagramsDroppedSend;
    private long datagramsDroppedRecv;
    private long datagramsDroppedLimit;
    /** Of datagramsSent and datagramsReceived: those on carrier datagram channels (EXTENSIONS.md Section 6.8). */
    private long datagramsSentChannel;
    private long datagramsReceivedChannel;
    private long appBytesWritten;
    private long appBytesRead;
    private long bytesRedundant;
    private long bytesOpportunistic;
    private long datagramBytesSent;
    private long datagramBytesReceived;
    private long streamCreditWaits;
    private long bytesReinjected;
    private long penalties;
    private long reinjections;
    private long bytesDuplicateReceived;
    private long pipesAdded;
    private long pipeFailures;
    private long joinAuthFailures;
    private long joinReplays;
    private long joinRefusals;
    private long suspensions;
    /** Time spent suspended before the current suspension; suspendedSince is its start, or -1. */
    private long suspendedTotal;
    private long suspendedSince = -1;
    private long schedulerAnswers;
    private long schedulerFallbacks;
    /** Changes with every count: lets a reader publish only what changed. */
    private long version;

    /** A batch of records left on a pipe. */
    void sent(long bytes, long records) { version++; bytesSent += bytes; recordsSent += records; }

    /** One record arrived. */
    void received(long bytes) { version++; bytesReceived += bytes; recordsReceived++; }

    void streamOpened(boolean local) { version++; if (local) streamsOpenedLocal++; else streamsOpenedRemote++; }

    void datagramSent() { version++; datagramsSent++; }

    void datagramReceived() { version++; datagramsReceived++; }

    void datagramDroppedSend() { version++; datagramsDroppedSend++; }

    void datagramDroppedRecv() { version++; datagramsDroppedRecv++; }

    void datagramDroppedLimit() { version++; datagramsDroppedLimit++; }

    /** A datagram went on a channel (also counted as sent). */
    void datagramSentChannel() { version++; datagramsSent++; datagramsSentChannel++; }

    /** A datagram of payload bytes counted as sent on a channel was refused by it: dropped instead. */
    void channelSendFailed(int payload) {
        version++;
        datagramsSent--;
        datagramsSentChannel--;
        datagramsDroppedSend++;
        datagramBytesSent -= payload;
    }

    /** n of the datagrams received came on a channel. */
    void datagramsReceivedChannel(long n) { version++; datagramsReceivedChannel += n; }

    void appWritten(long n) { version++; appBytesWritten += n; }

    void appRead(long n) { version++; appBytesRead += n; }

    void redundant(long n) { version++; bytesRedundant += n; }

    /** n opportunistic copy bytes: also reinjected bytes (API.md Section 7.2). */
    void opportunistic(long n) { version++; bytesOpportunistic += n; bytesReinjected += n; }

    /** A datagram of n payload bytes went to a carrier. */
    void datagramBytesSent(long n) { version++; datagramBytesSent += n; }

    /** A datagram of n payload bytes was returned to the application. */
    void datagramBytesReceived(long n) { version++; datagramBytesReceived += n; }

    /** An open had to wait for stream-count credit. */
    public void streamCreditWait() { version++; streamCreditWaits++; }

    /** A reinjection episode of n stream bytes (a pipe failed or became suspect). */
    void reinjected(long n) { version++; reinjections++; bytesReinjected += n; }

    void penalty() { version++; penalties++; }

    void duplicateReceived(long n) { version++; bytesDuplicateReceived += n; }

    void pipeAdded() { version++; pipesAdded++; }

    void pipeFailed() { version++; pipeFailures++; }

    void joinAuthFailed() { version++; joinAuthFailures++; }

    void joinReplay() { version++; joinReplays++; }

    void joinRefused() { version++; joinRefusals++; }

    void suspended(long now) { version++; suspensions++; suspendedSince = now; }

    void resumed(long now) {
        if (suspendedSince < 0) return;
        version++;
        suspendedTotal += Math.max(0, now - suspendedSince);
        suspendedSince = -1;
    }

    void schedulerAnswer() { version++; schedulerAnswers++; }

    void schedulerFallback() { version++; schedulerFallbacks++; }

    public long bytesSent() { return bytesSent; }

    public long bytesReceived() { return bytesReceived; }

    public long recordsSent() { return recordsSent; }

    public long recordsReceived() { return recordsReceived; }

    public long streamsOpenedLocal() { return streamsOpenedLocal; }

    public long streamsOpenedRemote() { return streamsOpenedRemote; }

    public long datagramsSent() { return datagramsSent; }

    public long datagramsReceived() { return datagramsReceived; }

    public long datagramsDroppedSend() { return datagramsDroppedSend; }

    public long datagramsDroppedRecv() { return datagramsDroppedRecv; }

    public long datagramsDroppedLimit() { return datagramsDroppedLimit; }

    public long datagramsSentChannel() { return datagramsSentChannel; }

    public long datagramsReceivedChannel() { return datagramsReceivedChannel; }

    public long appBytesWritten() { return appBytesWritten; }

    public long appBytesRead() { return appBytesRead; }

    public long bytesRedundant() { return bytesRedundant; }

    public long bytesOpportunistic() { return bytesOpportunistic; }

    public long joinAuthFailures() { return joinAuthFailures; }

    public long datagramBytesSent() { return datagramBytesSent; }

    public long datagramBytesReceived() { return datagramBytesReceived; }

    public long version() { return version; }

    /** The gauges of a snapshot, read from the session's components when it is taken. */
    public record Gauges(long streamsActive, long sendBuffered, long recvBuffered, long retained, long pipesActive,
                         long smoothedRtt, long minRtt, long latestRtt, long meanDeviation) {}

    /**
     * An immutable copy of the counts an application reads; RTTs are nanoseconds, -1 when
     * absent; suspendedSince is the start of the current suspension on the monotonic clock, or -1.
     */
    public record Snapshot(long bytesSent, long bytesReceived, long recordsSent, long recordsReceived,
                           long appBytesWritten, long appBytesRead, long datagramBytesSent, long datagramBytesReceived,
                           long streamsOpenedLocal, long streamsOpenedRemote, long streamCreditWaits,
                           long datagramsSent, long datagramsReceived, long datagramsSentChannel, long datagramsReceivedChannel,
                           long datagramsDroppedSend, long datagramsDroppedRecv, long datagramsDroppedLimit,
                           long budgetTokens, long bytesReinjected, long bytesOpportunistic, long penalties, long reinjections,
                           long bytesRedundant, long bytesDuplicateReceived, long pipesAdded, long pipeFailures,
                           long joinAuthFailures, long joinReplays, long joinRefusals, long suspensions,
                           long suspendedTotal, long suspendedSince, long schedulerAnswers, long schedulerFallbacks,
                           Gauges gauges) {}

    public Snapshot snapshot(long budgetTokens, Gauges g) {
        return new Snapshot(bytesSent, bytesReceived, recordsSent, recordsReceived, appBytesWritten, appBytesRead,
                datagramBytesSent, datagramBytesReceived, streamsOpenedLocal, streamsOpenedRemote, streamCreditWaits,
                datagramsSent, datagramsReceived, datagramsSentChannel, datagramsReceivedChannel, datagramsDroppedSend,
                datagramsDroppedRecv, datagramsDroppedLimit, budgetTokens, bytesReinjected, bytesOpportunistic, penalties,
                reinjections, bytesRedundant, bytesDuplicateReceived, pipesAdded, pipeFailures, joinAuthFailures, joinReplays,
                joinRefusals, suspensions, suspendedTotal, suspendedSince, schedulerAnswers, schedulerFallbacks, g);
    }
}
