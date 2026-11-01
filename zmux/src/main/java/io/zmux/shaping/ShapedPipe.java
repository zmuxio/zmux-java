package io.zmux.shaping;

/**
 * The outbound pipe a {@link ShaperFactory} is asked to shape.
 *
 * @param id the pipe identifier
 * @param carrier the kind of the pipe's carrier, as its pipe input declared it; null when undeclared
 *     (raw TCP or TLS, or unknown)
 * @param writeOverhead the fixed number of bytes each carrier write adds below the pipe, as its
 *     pipe input declared it ({@link io.zmux.carrier.PipeIo#withWriteOverhead}); 0 when none
 */
public record ShapedPipe(long id, io.zmux.CarrierKind carrier, int writeOverhead) {
    /** A pipe whose carrier declares no write overhead. */
    public ShapedPipe(long id, io.zmux.CarrierKind carrier) { this(id, carrier, 0); }
}
