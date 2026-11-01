package io.zmux.internal.core;

import io.zmux.internal.wire.Frame;
import io.zmux.internal.wire.WireException;
import java.util.List;

/**
 * The path layer of a single-pipe session: no retention, no path frames, no path state. What
 * differs from multipath is delegated to the session's single-pipe handling through {@link Port}.
 */
final class SinglePath implements PathLayer {
    /** The session's single-pipe handling. */
    interface Port {
        boolean wantsWrite(Pipe p, Pipe.OutLane ol);

        void write(RecordWriter w, Pipe p, Pipe.OutLane ol);

        void onPipeAck(Frame.AckEntries it) throws WireException;

        void inboundEnded(boolean failed, boolean allEnded, Throwable cause);

        void outboundFailed(Throwable cause);

        long srttMax();
    }

    private final Port port;

    SinglePath(Port port) {
        this.port = port;
    }

    @Override public boolean wantsWrite(Pipe p, Pipe.OutLane ol) { return port.wantsWrite(p, ol); }
    @Override public void write(RecordWriter w, Pipe p, Pipe.OutLane ol) { port.write(w, p, ol); }
    @Override public boolean acksInControl() { return true; }
    @Override public void maxDataSent(boolean peerWaiting) {}
    @Override public void maxStreamDataSent(Stream st, boolean peerWaiting) {}
    @Override public void creditBlocked(Stream st) {}
    @Override public boolean urgentLane(Stream st) { return st.tab.urgency() <= 2; }

    @Override public long sendBuffer(long configured) { return configured; }
    @Override public long retainRoom() { return Long.MAX_VALUE; }
    @Override public void retain(long n) {}
    @Override public void unretain(long n) {}
    @Override public boolean retainsSent() { return false; }
    @Override public long retained() { return 0; }

    @Override public void pickInput() {}

    @Override
    public void onPathFrame(Pipe p, Frame f) throws WireException {
        throw WireException.protocol(f.kind + " without multipath");
    }

    @Override public void onPipeAck(Frame.AckEntries it) throws WireException { port.onPipeAck(it); }
    @Override public void inboundEnded(Pipe p, boolean failed, boolean allEnded, Throwable cause) { port.inboundEnded(failed, allEnded, cause); }
    @Override public void outboundFailed(long pipe, Throwable cause) { port.outboundFailed(cause); }

    @Override public long deadline(long now) { return Time.NEVER; }
    @Override public void onTimeout(long t) {}

    @Override public long srttMax() { return port.srttMax(); }
    @Override public PipeInfo.States pipeState(Pipe p) { return PipeInfo.States.SINGLE; }
    @Override public Session.JoinProgress joinProgress(Pipe p) { return Session.JoinProgress.CONFIRMED; }
    @Override public boolean pipeUsable(Pipe p) { return true; }
    @Override public long nextPipeId() { return 0; }
    @Override public boolean joinReady() { return false; }
    @Override public void policyChanged() {}
    @Override public boolean[] liveDirections(boolean closing) { return new boolean[] {!closing, !closing}; }
    @Override public boolean wantsPick() { return false; }
    @Override public boolean takePick(PathScheduler.PickRequest req) { return false; }
    @Override public void answerPick(PathScheduler.PickRequest req, List<PathScheduler.PickAnswer> answers, boolean failed) {}
}
