package io.zmux;

import io.zmux.carrier.PipeIo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * The pipes of a {@link ZmuxSession} (MULTIPATH.md 5). Adding pipes needs multipath
 * negotiated; the snapshots work on any session.
 */
public final class Paths {
    private final ZmuxSession session;

    Paths(ZmuxSession session) { this.session = session; }

    /** Whether multipath was negotiated, as of the loop's last round. */
    public boolean isEnabled() { return session.info().multipath(); }

    /**
     * Joins a new TCP connection to address as a pipe and waits until the join is confirmed.
     * A join that the attachment cap holds waits for room. The connection starts with the
     * prefix of the session configuration's {@link ZmuxConfig#credential()}.
     */
    public ZmuxPipe add(InetSocketAddress address, PipeOptions options) throws IOException {
        return session.addPipe(address, options);
    }

    /**
     * Joins a new TLS connection (server name verified, the session's ALPN offered) as a pipe;
     * after the handshake it presents the session configuration's {@link ZmuxConfig#credential()}.
     */
    public ZmuxPipe addTls(InetSocketAddress address, TlsOptions tls, PipeOptions options) throws IOException {
        return session.addPipeTls(address, tls, options);
    }

    /** Joins a pipe input as a pipe. */
    public ZmuxPipe add(PipeIo io, PipeOptions options) throws IOException {
        return session.addPipe(io, options);
    }

    /** Reserves a pipe for an out-of-band PIPE_JOIN header, before opening its carrier. */
    public PipeReservation reserve(PipeOptions options) throws IOException {
        return session.reservePipe(options);
    }

    /** The attachments as of the loop's last round; no I/O. */
    public List<ZmuxPipe> list() { return session.pipes(); }

    /** Per-pipe snapshots as of the loop's last round, including pipe 0; no I/O. */
    public List<PipeStats> stats() { return session.pipeStats(); }

    /** Maintains one route's joined pipes; closing the handle stops maintenance, preserving existing traffic. */
    public PipeMaintenance maintain(PipePool pool, Cancellation cancellation) throws IOException {
        return session.maintainPipes(pool, cancellation);
    }

    public PipeMaintenance maintain(PipePool pool) throws IOException { return maintain(pool, null); }

    /** Whether the maintenance target and join admission permit dialing another pipe now. */
    public boolean joinReady() throws IOException { return session.joinReady(); }

    /** Re-checks every pipe after a suspected path change. */
    public void revalidate() throws IOException { session.revalidatePipes(); }

    /** Changes the session's scheduling policy. */
    public void setScheduler(Scheduler s) { session.setScheduler(s); }

    /** The default quality event band of outbound pipes. */
    public void setQualityBand(QualityBand band) throws IOException { session.setQualityBand(band); }

    /** The outbound capacity floor, with hysteresis. */
    public void setCapacityFloor(CapacityFloor floor) throws IOException { session.setCapacityFloor(floor); }
}
