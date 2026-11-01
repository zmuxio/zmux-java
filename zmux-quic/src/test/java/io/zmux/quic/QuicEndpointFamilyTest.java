package io.zmux.quic;

import io.zmux.*;
import static org.junit.jupiter.api.Assertions.*;
import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.carrier.PipeIo;
import java.net.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class QuicEndpointFamilyTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }
    private static InetSocketAddress ipv6() throws Exception { return new InetSocketAddress(InetAddress.getByName("::1"),0); }
    @Test void ipv4AndIpv6HaveSeparateUdpFamiliesButShareTheOwningLoopAndClose() throws Exception {
        try (var f = new QuicPoolTest.Fixture(); var v4 = f.pool(2,0,Duration.ZERO);
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var a = QuicPoolTest.firstPipe(f,v4);
            var udp4 = ((QuicChannel)a.carrierConn()).parent();
            try {
                assertInstanceOf(Inet4Address.class,((InetSocketAddress)udp4.localAddress()).getAddress(),
                        "IPv4 dials must not use an IPv6 wildcard socket");
                try (var server6 = new QuicServer(ipv6(),TestTls.server,QuicOptions.defaults(),f.remote);
                     var v6 = new QuicPool(f.client,server6.address(),"localhost")) {
                    var b = v6.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
                    var udp6 = ((QuicChannel)b.carrierConn()).parent();
                    try {
                        assertInstanceOf(Inet6Address.class,((InetSocketAddress)udp6.localAddress()).getAddress());
                        assertNotSame(udp4,udp6); assertSame(udp4.eventLoop(),udp6.eventLoop());
                        assertEquals(2,f.client.connections().size());
                        var session = f.local.open(b, Role.CLIENT);
                        var peer = f.remote.accept(Duration.ofSeconds(5)); assertNotNull(peer);
                        try { QuicTest.transfer(workers,session,peer,2,64<<10); QuicTest.transfer(workers,peer,session,2,64<<10); }
                        finally { QuicEndpointTest.closed(session,peer); }
                    } finally { QuicPoolTest.release(b); }
                    QuicPoolTest.release(a);
                    f.client.close();
                    assertFalse(udp4.isActive()); assertFalse(udp6.isActive());
                }
            } finally { QuicPoolTest.release(a); }
        }
    }
    @Test void mappedIpv4RoutesUseTheIpv4Endpoint() throws Exception {
        try (var f = new QuicPoolTest.Fixture()) {
            byte[] mapped = new byte[16]; mapped[10] = mapped[11] = (byte)0xff;
            byte[] v4 = f.server.address().getAddress().getAddress(); System.arraycopy(v4,0,mapped,12,4);
            var address = new InetSocketAddress(Inet6Address.getByAddress(null,mapped,-1),f.server.address().getPort());
            var carrier = f.client.connectCarrier(address,"localhost",null).toCompletableFuture().get(5,TimeUnit.SECONDS);
            try { assertInstanceOf(Inet4Address.class,((InetSocketAddress)carrier.channel().parent().localAddress()).getAddress()); }
            finally { carrier.dispose(); }
        }
    }
    @Test void simultaneousIpv6DialsReuseOneLazyUdpEndpoint() throws Exception {
        try (var f = new QuicPoolTest.Fixture();
             var server6 = new QuicServer(ipv6(),TestTls.server,QuicOptions.defaults(),f.remote);
             var pool = new QuicPool(f.client,server6.address(),"localhost",new QuicPool.Limits(2,0,Duration.ZERO))) {
            var futures = new ArrayList<CompletableFuture<PipeIo>>();
            var pipes = new ArrayList<PipeIo>();
            try {
                for (int i=0;i<8;i++) futures.add(pool.openPipe().toCompletableFuture());
                for (var future : futures) pipes.add(future.get(5,TimeUnit.SECONDS));
                var parents = new HashSet<io.netty.channel.Channel>();
                for (var p : pipes) parents.add(((QuicChannel)p.carrierConn()).parent());
                assertEquals(1,parents.size()); assertEquals(4,pool.connections().size());
            } finally { for (var p : pipes) QuicPoolTest.release(p); }
        }
    }
    @Test void endpointCloseCancelsAQueuedIpv6BindAndDoesNotLeakItsLateChannel() throws Exception {
        try (var f = new QuicPoolTest.Fixture(); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var entered = new CountDownLatch(1); var proceed = new CountDownLatch(1);
            var loop = f.client.nativeLoop();
            loop.execute(() -> { entered.countDown(); try { proceed.await(4,TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); } });
            assertTrue(entered.await(2,TimeUnit.SECONDS));
            Future<?> closing = null;
            try {
                var pending = f.client.connectCarrier(new InetSocketAddress(InetAddress.getByName("::1"),443),"localhost",null).toCompletableFuture();
                assertFalse(pending.isDone());
                closing = workers.submit(f.client::close);
                assertThrows(ExecutionException.class,() -> pending.get(2,TimeUnit.SECONDS));
            } finally { proceed.countDown(); if (closing != null) closing.get(5,TimeUnit.SECONDS); }
            assertTrue(f.client.connections().isEmpty());
            assertTrue(loop.terminationFuture().isDone());
        }
    }
}
