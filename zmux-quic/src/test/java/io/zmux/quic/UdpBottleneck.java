package io.zmux.quic;

import io.zmux.*;
import java.io.IOException;
import java.net.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/** Test-only full-duplex UDP relay: serialization, propagation delay and a bounded drop-tail queue. */
final class UdpBottleneck implements AutoCloseable {
    record Config(long bytesPerSecond, Duration oneWayDelay, int queueBytes) {
        Config {
            if (bytesPerSecond <= 0 || oneWayDelay.isNegative() || queueBytes <= 0)
                throw new IllegalArgumentException("positive rate/queue and nonnegative delay required");
            long delay = oneWayDelay.toNanos();
            if (queueBytes * 1e9 / bytesPerSecond + delay >= Long.MAX_VALUE / 2.0)
                throw new IllegalArgumentException("queue delay exceeds monotonic clock range");
        }
    }
    record Stats(long received, long sent, long dropped, long sentBytes, int peakQueued) {}

    /** The one serialization/queue authority used by the live relay and deterministic tests. */
    static final class Reservation {
        final int bytes;
        final long serializationDelay;
        final long deliveryDelay;
        boolean released;
        Reservation(int bytes, long serializationDelay, long propagation) {
            this.bytes = bytes; this.serializationDelay = serializationDelay; this.deliveryDelay = serializationDelay + propagation;
        }
    }
    static final class Queue {
        final Config config;
        long available;
        boolean started;
        int queued;
        int peak;
        Queue(Config config) { this.config = config; }
        synchronized Reservation reserve(int bytes, long now) {
            if (bytes <= 0 || bytes > config.queueBytes() - queued) return null;
            long backlog = started ? Math.max(0, available - now) : 0;
            long service = Math.max(1, (long) Math.ceil(bytes * 1e9 / config.bytesPerSecond()));
            available = now + backlog + service;
            started = true;
            queued += bytes;
            peak = Math.max(peak, queued);
            return new Reservation(bytes, backlog + service, config.oneWayDelay().toNanos());
        }
        synchronized void complete(Reservation reservation) {
            if (!reservation.released) { reservation.released = true; queued -= reservation.bytes; }
        }
        synchronized void clear() { queued = 0; }
        synchronized int peak() { return peak; }
    }

    private final DatagramSocket front;
    private final DatagramSocket back;
    private final ScheduledExecutorService delivery;
    private final AtomicReference<SocketAddress> client = new AtomicReference<>();
    private final AtomicReference<IOException> failure = new AtomicReference<>();
    private final Direction forward;
    private final Direction reverse;
    private final Thread upstream;
    private final Thread downstream;
    private volatile boolean closed;

    UdpBottleneck(InetSocketAddress target, Config config) throws IOException {
        front = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
        DatagramSocket created = null;
        try { created = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0)); created.connect(target); }
        catch (IOException | RuntimeException error) { if (created != null) created.close(); front.close(); throw error; }
        back = created;
        delivery = Executors.newScheduledThreadPool(1, Thread.ofPlatform().daemon(true).name("quic-probe-delivery-", 0).factory());
        forward = new Direction(back, config);
        reverse = new Direction(front, config);
        upstream = Thread.ofVirtual().name("quic-probe-upstream").start(() -> receive(front, forward, true));
        downstream = Thread.ofVirtual().name("quic-probe-downstream").start(() -> receive(back, reverse, false));
    }
    InetSocketAddress address() { return (InetSocketAddress) front.getLocalSocketAddress(); }
    Stats forward() { return forward.stats(); }
    Stats reverse() { return reverse.stats(); }
    void check() throws IOException { if (failure.get() != null) throw failure.get(); }

    private void receive(DatagramSocket socket, Direction direction, boolean upstream) {
        byte[] buffer = new byte[65535];
        try {
            while (!closed) {
                var packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                if (upstream) {
                    client.compareAndSet(null, packet.getSocketAddress());
                    if (!client.get().equals(packet.getSocketAddress())) throw new IOException("relay accepts one client endpoint");
                }
                SocketAddress destination = upstream ? back.getRemoteSocketAddress() : client.get();
                if (destination == null) throw new IOException("reply arrived before the client endpoint");
                direction.offer(packet, destination);
            }
        } catch (IOException error) { if (!closed) failure.compareAndSet(null, error); }
    }

    private final class Direction {
        final DatagramSocket socket;
        final Queue queue;
        long received;
        long sent;
        long dropped;
        long sentBytes;
        Direction(DatagramSocket socket, Config config) { this.socket = socket; queue = new Queue(config); }
        synchronized Stats stats() { return new Stats(received, sent, dropped, sentBytes, queue.peak()); }
        synchronized void offer(DatagramPacket packet, SocketAddress destination) {
            received++;
            int bytes = packet.getLength();
            long now = System.nanoTime();
            Reservation reservation = queue.reserve(bytes, now);
            if (reservation == null) { dropped++; return; }
            byte[] data = Arrays.copyOfRange(packet.getData(), packet.getOffset(), packet.getOffset() + bytes);
            try {
                // The finite send queue ends at serialization, before propagation.
                delivery.schedule(() -> queue.complete(reservation),
                        Math.max(0, reservation.serializationDelay - (System.nanoTime() - now)), TimeUnit.NANOSECONDS);
                delivery.schedule(() -> {
                try {
                    socket.send(new DatagramPacket(data, data.length, destination));
                    synchronized (this) { sent++; sentBytes += bytes; }
                } catch (IOException error) { if (!closed) failure.compareAndSet(null, error); }
                finally { queue.complete(reservation); }
            }, Math.max(0, reservation.deliveryDelay - (System.nanoTime() - now)), TimeUnit.NANOSECONDS); }
            catch (RejectedExecutionException error) {
                queue.complete(reservation);
                if (!closed) failure.compareAndSet(null, new IOException("relay delivery rejected", error));
            }
        }
    }

    @Override public void close() throws Exception {
        closed = true;
        front.close(); back.close(); delivery.shutdownNow();
        upstream.join(5000); downstream.join(5000);
        if (!delivery.awaitTermination(5, TimeUnit.SECONDS) || upstream.isAlive() || downstream.isAlive())
            throw new IOException("relay workers did not terminate");
        forward.queue.clear(); reverse.queue.clear();
        check();
    }
}
