package io.zmux;

import io.zmux.protocol.FrameCodec;
import io.zmux.protocol.Limits;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;

/**
 * Frame reads for raw test peers that poll a socket without ever timing out in the middle of a frame.
 *
 * <p>Wrapping a whole {@link FrameCodec#readFrame} in a short {@code SO_TIMEOUT} loses the bytes already consumed
 * when the timeout strikes mid-frame, and every later read is then desynchronized from the frame boundaries. These
 * helpers apply the caller's timeout only to the first byte of a frame (mark/read/reset) and read the rest of the
 * frame under a long timeout. Every raw test peer that polls a socket should read frames through here.
 */
final class RawFrameReads {
    private static final int FRAME_READ_TIMEOUT_MILLIS = 5_000;

    private RawFrameReads() {
    }

    /**
     * {@link #readFrameIfStarted(Socket, BufferedInputStream, int, Limits)} with the default limits.
     */
    static FrameCodec.Frame readFrameIfStarted(Socket socket,
                                               BufferedInputStream input,
                                               int firstByteTimeoutMillis) throws IOException {
        return readFrameIfStarted(socket, input, firstByteTimeoutMillis, Settings.defaults().limits());
    }

    /**
     * {@link #readFrameIfStarted(Socket, BufferedInputStream, int, Limits)} with the default limits.
     */
    static FrameCodec.Frame readFrameIfStarted(Socket socket,
                                               BufferedInputStream input,
                                               Duration firstByteTimeout) throws IOException {
        return readFrameIfStarted(socket, input, timeoutMillis(firstByteTimeout), Settings.defaults().limits());
    }

    /**
     * Reads one frame if one starts to arrive within {@code firstByteTimeoutMillis}, else returns {@code null}. Only
     * the wait for the first byte uses the short timeout; the rest of the frame is read under a long one, so a poll
     * timeout can never strike mid-frame and desynchronize the frame stream.
     *
     * @throws EOFException if the transport is closed before a frame starts
     */
    static FrameCodec.Frame readFrameIfStarted(Socket socket,
                                               BufferedInputStream input,
                                               int firstByteTimeoutMillis,
                                               Limits limits) throws IOException {
        socket.setSoTimeout(Math.max(1, firstByteTimeoutMillis));
        input.mark(1);
        try {
            if (input.read() < 0) {
                throw new EOFException("raw peer transport closed");
            }
        } catch (SocketTimeoutException nothingArrived) {
            return null;
        }
        input.reset();
        socket.setSoTimeout(FRAME_READ_TIMEOUT_MILLIS);
        return FrameCodec.readFrame(input, limits);
    }

    /**
     * {@link #readFrame(Socket, BufferedInputStream, Duration, Limits)} with the default limits.
     */
    static FrameCodec.Frame readFrame(Socket socket, BufferedInputStream input, Duration timeout) throws IOException {
        return readFrame(socket, input, timeout, Settings.defaults().limits());
    }

    /**
     * Reads one frame that must start to arrive within {@code timeout}, failing the test with an
     * {@link AssertionError} otherwise.
     */
    static FrameCodec.Frame readFrame(Socket socket,
                                      BufferedInputStream input,
                                      Duration timeout,
                                      Limits limits) throws IOException {
        FrameCodec.Frame frame = readFrameIfStarted(socket, input, timeoutMillis(timeout), limits);
        if (frame == null) {
            throw new AssertionError("timed out waiting for frame");
        }
        return frame;
    }

    private static int timeoutMillis(Duration timeout) {
        return (int) Math.min(Integer.MAX_VALUE, Math.max(1L, timeout.toMillis()));
    }
}
