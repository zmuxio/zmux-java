package io.zmux.quic;

import io.zmux.*;
import static org.junit.jupiter.api.Assertions.*;
import io.zmux.carrier.PipeIo;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Explicitly selected reproduction; intentionally excluded from the default *Test suite. */
@Timeout(600)
class QuicStartupProbe {
    private static void forceLegacyPortCollision(QuicPoolTest.Fixture f) throws Exception {
        int targetPort = f.server.address().getPort();
        java.nio.channels.DatagramChannel selected = null;
        for (int i = 0; i < 200000; i++) {
            var socket = java.nio.channels.DatagramChannel.open(); socket.bind(new java.net.InetSocketAddress(0));
            if (((java.net.InetSocketAddress)socket.getLocalAddress()).getPort() == targetPort) { selected = socket; break; }
            socket.close();
        }
        assertNotNull(selected,"host did not exhibit the wildcard/specific-address ephemeral-port collision");
        var field = QuicClient.class.getDeclaredField("datagrams"); field.setAccessible(true);
        @SuppressWarnings("unchecked") var endpoints = (java.util.concurrent.ConcurrentHashMap<io.netty.channel.socket.SocketProtocolFamily,CompletableFuture<io.netty.channel.Channel>>)field.get(f.client);
        var original = endpoints.get(io.netty.channel.socket.SocketProtocolFamily.INET).get();
        var context = TestTls.client.earlyData(false).endpointIdentificationAlgorithm("HTTPS").build();
        var codec = QuicOptions.defaults().apply(new io.netty.handler.codec.quic.QuicClientCodecBuilder())
                .sslEngineProvider(c -> context.newEngine(c.alloc(),"localhost",targetPort)).build();
        var chosen = selected;
        io.netty.channel.Channel replacement = null;
        try {
            replacement = new io.netty.bootstrap.Bootstrap().group(original.eventLoop())
                    .channelFactory(() -> new io.netty.channel.socket.nio.NioDatagramChannel(chosen)).handler(codec).register().sync().channel();
            endpoints.put(io.netty.channel.socket.SocketProtocolFamily.INET,CompletableFuture.completedFuture(replacement)); original.close().sync();
            System.out.println("FORCED_LEGACY_COLLISION,client="+replacement.localAddress()+",server="+f.server.address());
        } catch (Exception error) { if (replacement != null) replacement.close(); else selected.close(); throw error; }
    }
    @Test void freshEndpointFirstPooledStream() throws Exception {
        TestTls.init();
        int count = Integer.getInteger("zmux.startup.count",300);
        if (count < 1 || count > 3000) throw new IllegalArgumentException("startup count outside1..3000");
        String scenario = System.getProperty("zmux.startup.scenario", "fresh");
        var methods = Arrays.stream(QuicPoolTest.class.getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Test.class))
                .sorted(Comparator.comparingInt((java.lang.reflect.Method m) -> m.getName().hashCode()).thenComparing(java.lang.reflect.Method::getName)).toList();
        assertFalse(methods.isEmpty());
        System.out.println("STARTUP_SUITE_METHODS,"+methods.size());
        if (!Set.of("fresh","cancellation","suite","collision").contains(scenario)) throw new IllegalArgumentException("unknown startup scenario");
        for (int i = 0; i < count; i++) {
            if (scenario.equals("collision")) {
                try (var f = new QuicPoolTest.Fixture(); var pool = f.pool(1,2,java.time.Duration.ZERO)) {
                    forceLegacyPortCollision(f);
                    assertThrows(TimeoutException.class,() -> QuicPoolTest.firstPipe(f,pool));
                }
            } else if (!scenario.equals("fresh")) {
                var test = new QuicPoolTest();
                try {
                    if (scenario.equals("cancellation")) test.cancellationDoesNotReturnAPendingNativeStreamSlotEarlyAndCountsTheCreatedStream();
                    else for (var method : methods) method.invoke(test);
                } catch (java.lang.reflect.InvocationTargetException failed) {
                    throw new AssertionError("suite iteration " + i,failed.getCause());
                }
            } else try (var f = new QuicPoolTest.Fixture(); var pool = f.pool(1,2,java.time.Duration.ZERO)) {
                PipeIo io = QuicPoolTest.firstPipe(f,pool);
                QuicPoolTest.release(io);
                QuicPoolTest.until(() -> pool.connections().getFirst().active()==0);
                assertEquals(1,pool.connections().size());
            }
            if ((i+1)%50==0) System.out.println("STARTUP_COMPLETED,"+(i+1));
        }
        System.out.println("STARTUP_TOTAL,"+scenario+","+count);
    }
}
