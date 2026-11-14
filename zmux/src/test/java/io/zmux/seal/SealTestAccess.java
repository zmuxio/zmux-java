package io.zmux.seal;

import io.zmux.RecordMode;
import io.zmux.carrier.Framing;
import io.zmux.carrier.PipeChannel;
import io.zmux.carrier.PipeIo;
import io.zmux.internal.join.Mode;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/** The sans-IO client for a test that sends what no public interface sends: a Trusted one with first bytes. */
public final class SealTestAccess {
    private SealTestAccess() {}

    private static EstablishClient client;
    private static Established done;

    /** Runs messages 1 and 2 over ch: the pipe input for what follows them. */
    public static synchronized PipeIo handshakeUpToFinish(SocketChannel ch) throws Exception {
        done = null;
        byte[] secret = new byte[32];
        HandlePsk hp = HandlePsk.derive(secret, "test".getBytes(java.nio.charset.StandardCharsets.UTF_8), null);
        client = EstablishClient.create(Profile.XXHFS_PSK0_AES, Mode.TRUSTED, io.zmux.internal.join.Framing.STREAM, hp.handle(), hp.psk(), null, StaticKey.generate());
        ch.write(ByteBuffer.wrap(client.writeHello(EstablishPayload.padding(0, false))));
        ByteBuffer b = ByteBuffer.allocate(4096);
        while (true) {
            if (ch.read(b) < 0) throw new java.io.EOFException();
            try {
                client.readReply(b.array(), 0, b.position());
                break;
            } catch (io.zmux.seal.SecureCarrierException e) {
                if (e.code() != io.zmux.seal.SecureCarrierException.Code.INCOMPLETE) throw e;
            }
        }
        client.trustPskOnly(null);
        return PipeChannel.pipe(ch, ch, Framing.BYTE_STREAM, 1 << 16);
    }

    /** Message 3 with first as its carrier first bytes. */
    public static synchronized void finishWith(SocketChannel ch, byte[] first) throws Exception {
        ByteBuffer m = ByteBuffer.wrap(client.writeFinish(new EstablishPayload(null, null, first, 0), null));
        while (m.hasRemaining()) ch.write(m);
        done = client.finish();
    }

    /** The carrier as a Trusted secure first pipe; its exporter becomes readable with message 3's finish. */
    public static synchronized PipeIo established(PipeIo p) throws Exception {
        PipeIo.Exporter e = (label, context, n) -> {
            try {
                synchronized (SealTestAccess.class) {
                    return done == null ? null : done.export(label, context, n);
                }
            } catch (Exception notYet) {
                return null;
            }
        };
        return io.zmux.internal.loop.PipeIoAccess.get().withEstablished(p, RecordMode.TRUSTED, e, null, null);
    }
}
