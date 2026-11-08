package io.zmux;

/** {@link ZmuxSession#exportKeyingMaterial}: the bootstrap carrier has no keying material exporter. */
public final class NoExporterException extends ZmuxException {
    public NoExporterException() {
        super("the carrier has no keying material exporter");
    }
}
