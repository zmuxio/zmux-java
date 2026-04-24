package io.zmux;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.channels.GatheringByteChannel;
import java.util.IdentityHashMap;
import java.util.Objects;

public final class BasicDuplexConnection implements DuplexConnection {
    private final InputStream input;
    private final OutputStream output;
    private final GatheringByteChannel gatheringOutput;
    private final AutoCloseable closer;
    private final SocketAddress localAddress;
    private final SocketAddress remoteAddress;

    public BasicDuplexConnection(InputStream input, OutputStream output) {
        this(input, output, null, null, null, null);
    }

    public BasicDuplexConnection(InputStream input,
                                 OutputStream output,
                                 AutoCloseable closer,
                                 SocketAddress localAddress,
                                 SocketAddress remoteAddress) {
        this(input, output, closer, localAddress, remoteAddress, null);
    }

    public BasicDuplexConnection(InputStream input,
                                 OutputStream output,
                                 AutoCloseable closer,
                                 SocketAddress localAddress,
                                 SocketAddress remoteAddress,
                                 GatheringByteChannel gatheringOutput) {
        this.input = Objects.requireNonNull(input, "input");
        this.output = Objects.requireNonNull(output, "output");
        this.gatheringOutput = gatheringOutput;
        this.closer = closer;
        this.localAddress = localAddress;
        this.remoteAddress = remoteAddress;
    }

    public static Builder builder(InputStream input, OutputStream output) {
        return new Builder(input, output);
    }

    private static ZmuxException closeFailure(String target, Exception cause) {
        return new ZmuxException(
                ErrorCode.INTERNAL.code(),
                "close",
                "zmux: failed to close " + target,
                cause,
                ZmuxErrorScope.SESSION,
                ZmuxErrorSource.LOCAL,
                ZmuxErrorDirection.BOTH,
                ZmuxTerminationKind.SESSION_TERMINATION
        );
    }

    @Override
    public InputStream input() {
        return input;
    }

    @Override
    public OutputStream output() {
        return output;
    }

    @Override
    public GatheringByteChannel gatheringOutput() {
        return gatheringOutput;
    }

    @Override
    public SocketAddress localAddress() {
        return localAddress;
    }

    @Override
    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public void close() throws IOException {
        CloseFailures failures = new CloseFailures();
        IdentityHashMap<Object, Boolean> closed = new IdentityHashMap<>(4);
        if (closer != null && closed.put(closer, Boolean.TRUE) == null) {
            failures.closeAuto("duplex connection closer", closer);
        }
        if (closed.put(input, Boolean.TRUE) == null) {
            failures.closeIo(input);
        }
        if (closed.put(output, Boolean.TRUE) == null) {
            failures.closeIo(output);
        }
        if (gatheringOutput != null && closed.put(gatheringOutput, Boolean.TRUE) == null) {
            failures.closeIo(gatheringOutput);
        }
        failures.throwIfAny();
    }

    private static final class CloseFailures {
        private IOException error;

        void closeAuto(String target, AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (IOException e) {
                append(e);
            } catch (Exception e) {
                append(closeFailure(target, e));
            }
        }

        void closeIo(Closeable closeable) {
            try {
                closeable.close();
            } catch (IOException e) {
                append(e);
            }
        }

        void throwIfAny() throws IOException {
            if (error != null) {
                throw error;
            }
        }

        private void append(IOException next) {
            if (error == null) {
                error = next;
                return;
            }
            error.addSuppressed(next);
        }
    }

    public static final class Builder {
        private final InputStream input;
        private final OutputStream output;
        private AutoCloseable closer;
        private SocketAddress localAddress;
        private SocketAddress remoteAddress;
        private GatheringByteChannel gatheringOutput;

        private Builder(InputStream input, OutputStream output) {
            this.input = Objects.requireNonNull(input, "input");
            this.output = Objects.requireNonNull(output, "output");
        }

        public Builder closer(AutoCloseable closer) {
            this.closer = closer;
            return this;
        }

        public Builder localAddress(SocketAddress localAddress) {
            this.localAddress = localAddress;
            return this;
        }

        public Builder remoteAddress(SocketAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
            return this;
        }

        public Builder addresses(SocketAddress localAddress, SocketAddress remoteAddress) {
            this.localAddress = localAddress;
            this.remoteAddress = remoteAddress;
            return this;
        }

        public Builder gatheringOutput(GatheringByteChannel gatheringOutput) {
            this.gatheringOutput = gatheringOutput;
            return this;
        }

        public BasicDuplexConnection build() {
            return new BasicDuplexConnection(
                    input,
                    output,
                    closer,
                    localAddress,
                    remoteAddress,
                    gatheringOutput
            );
        }
    }
}
