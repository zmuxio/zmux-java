package io.zmux.quic;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.*;
import io.zmux.carrier.PipeIo;
import java.time.Duration;
import java.nio.file.*;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class QuicPoolTest {
    @BeforeAll static void tls() throws Exception { TestTls.init(); }
    @FunctionalInterface interface Checked { boolean get() throws Exception; }
    static void until(Checked c) throws Exception {
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while (!c.get() && System.nanoTime()<end) Thread.sleep(3);
        assertTrue(c.get(),"condition did not become true");
    }
    static void release(PipeIo io) { try { io.reader().close(); } finally { io.writer().abort(new java.io.IOException("released")); } }
    private static String pending(QuicClient client) throws Exception {
        var field = QuicClient.class.getDeclaredField("pending"); field.setAccessible(true);
        return field.get(client).toString();
    }
    private static void nativeState(StringBuilder b, String role, QuicClient client, QuicServer server) {
        var connections = client == null ? server.connections() : client.connections();
        for (var c : connections) {
            b.append(role).append(" channel=").append(c).append(" active=").append(c.isActive())
                    .append(" loopShutdown=").append(c.eventLoop().isShuttingDown())
                    .append(" local=").append(c.localSocketAddress()).append(" remote=").append(c.remoteSocketAddress()).append('\n');
            try { b.append("stats=").append(c.collectStats().get(200, TimeUnit.MILLISECONDS)).append('\n'); }
            catch (Exception error) { b.append("stats unavailable: ").append(error).append('\n'); }
        }
    }
    private static void diagnose(long started, QuicPoolTest.Fixture f, QuicPool pool, CompletableFuture<PipeIo> future) throws Exception {
        var b = new StringBuilder("elapsed_ns=" + (System.nanoTime()-started) + " future=" + future + "\n");
        b.append("pool=").append(pool.connections()).append("\npending=").append(pending(f.client)).append('\n');
        var datagrams = QuicClient.class.getDeclaredField("datagrams"); datagrams.setAccessible(true);
        b.append("client UDP endpoints=").append(datagrams.get(f.client)).append(" server UDP=").append(f.server.address()).append('\n');
        for (String name : List.of("opening", "requests")) {
            var field = QuicPool.class.getDeclaredField(name); field.setAccessible(true);
            synchronized (pool) {
                Object value = field.get(pool); b.append(name).append('=').append(value).append('\n');
                if (name.equals("opening") && value != null) {
                    for (var detail : value.getClass().getDeclaredFields()) {
                        detail.setAccessible(true); b.append(detail.getName()).append('=').append(detail.get(value)).append('\n');
                    }
                }
            }
        }
        nativeState(b,"client",f.client,null); nativeState(b,"server",null,f.server);
        for (var entry : Thread.getAllStackTraces().entrySet()) {
            var t = entry.getKey(); b.append("\nTHREAD ").append(t.getName()).append(' ').append(t.getState()).append('\n');
            for (var frame : entry.getValue()) b.append("  ").append(frame).append('\n');
        }
        Path out = Path.of(System.getProperty("zmux.startup.diagnostics", "target"));
        Files.createDirectories(out); Files.writeString(out.resolve("quic-startup-" + System.nanoTime() + ".txt"),b);
    }
    static PipeIo firstPipe(Fixture f, QuicPool pool) throws Exception {
        long started = System.nanoTime();
        var opened = pool.openPipe().toCompletableFuture();
        try { return opened.get(5,TimeUnit.SECONDS); }
        catch (TimeoutException timeout) {
            try { diagnose(started,f,pool,opened); }
            catch (Exception diagnosticFailure) { timeout.addSuppressed(diagnosticFailure); }
            finally { opened.cancel(false); }
            throw timeout;
        }
    }
    static final class Fixture implements AutoCloseable {
        final EventLoops loops=EventLoops.create(3);
        final ZmuxConfig config=QuicEndpointTest.config(loops);
        final ZmuxListener local=Zmux.listener(ListenerConfig.of(config)),remote=Zmux.listener(ListenerConfig.of(config));
        final QuicServer server;
        final QuicClient client;
        Fixture() throws Exception {
            server=new QuicServer(QuicTest.any(),TestTls.server,QuicOptions.defaults(),remote);
            client=new QuicClient(TestTls.client,QuicOptions.defaults(),local);
        }
        QuicPool pool(int concurrent,PoolPolicy policy) {
            return new QuicPool(client,server.address(),"localhost",new QuicPool.Limits(concurrent,0,Duration.ZERO),null,policy);
        }
        QuicPool pool(int concurrent,long lifetime,Duration age) {
            return new QuicPool(client,server.address(),"localhost",new QuicPool.Limits(concurrent,lifetime,age));
        }
        @Override public void close() throws Exception { client.close();server.close();local.close();remote.close();loops.close();assertEquals(0,io.zmux.internal.diag.Diagnostics.outstandingBuffers(loops)); }
    }
    @Test void concurrentRequestsShareDialsAndRespectReservationLimits() throws Exception {
        try (var f=new Fixture();var pool=f.pool(2,0,Duration.ZERO)) {
            var futures=new ArrayList<CompletableFuture<PipeIo>>();
            for(int i=0;i<12;i++) futures.add(pool.openPipe().toCompletableFuture());
            var carriers=new ArrayList<PipeIo>();
            try {
                for(var future:futures) carriers.add(future.get(8,TimeUnit.SECONDS));
                assertEquals(6,pool.connections().size(),"shared pending dial should fill available slots before dialing again");
                var groups=new HashSet<Long>();
                for(var info:pool.connections()) { assertEquals(2,info.active());assertEquals(2,info.total());assertTrue(info.fateGroup()>0 && info.fateGroup()<(1L<<62));groups.add(info.fateGroup()); }
                assertEquals(6,groups.size());
                for(var io:carriers) assertTrue(groups.contains(io.capabilities().fateGroup()));
            } finally { for(var io:carriers) release(io); }
            until(() -> pool.connections().stream().allMatch(c -> c.active()==0));
            pool.close();until(() -> pool.connections().isEmpty());
        }
    }
    @Test void lifetimeRetiresOnlyAfterTheLastStreamAndReusesFreeSlots() throws Exception {
        try(var f=new Fixture();var pool=f.pool(2,3,Duration.ZERO)) {
            var one=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            var two=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            assertSame(one.carrierConn(),two.carrierConn());assertEquals(one.capabilities().fateGroup(),two.capabilities().fateGroup());
            release(one);until(() -> pool.connections().getFirst().active()==1);
            var three=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            assertSame(two.carrierConn(),three.carrierConn());assertTrue(pool.connections().getFirst().retiring());
            var four=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            assertNotSame(three.carrierConn(),four.carrierConn());
            var original=(io.netty.handler.codec.quic.QuicChannel)two.carrierConn();assertTrue(original.isActive());
            release(two);release(three);until(() -> !original.isActive());
            release(four);
        }
    }
    @Test void ageRetirementDrainsActiveConnectionsAndClosesIdleOnesWithoutFurtherGets() throws Exception {
        try(var f=new Fixture();var pool=f.pool(2,0,Duration.ofMillis(150))) {
            var one=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            var original=(io.netty.handler.codec.quic.QuicChannel)one.carrierConn();
            until(() -> pool.connections().getFirst().retiring());assertTrue(original.isActive());
            var two=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);assertNotSame(one.carrierConn(),two.carrierConn());
            release(one);until(() -> !original.isActive());release(two);until(() -> pool.connections().isEmpty());
        }
    }
    @Test void poolCloseKeepsJoinedTrafficAndReleasesConnectionAfterItsStreamsEnd() throws Exception {
        try(var f=new Fixture();var workers=Executors.newVirtualThreadPerTaskExecutor();var pool=f.pool(2,0,Duration.ZERO)) {
            var a=f.client.connect(f.server.address(),"localhost").toCompletableFuture().get(8,TimeUnit.SECONDS);
            var b=f.remote.accept(Duration.ofSeconds(5));assertNotNull(b);
            try {
                var bootstrap=f.client.connections().getFirst();
                var first=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
                var second=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
                a.paths().add(first,PipeOptions.defaults());a.paths().add(second,PipeOptions.defaults());
                var pooled=(io.netty.handler.codec.quic.QuicChannel)first.carrierConn();
                pool.close();assertTrue(pooled.isActive());bootstrap.close().sync();
                QuicTest.transfer(workers,a,b,4,128<<10);QuicTest.transfer(workers,b,a,2,64<<10);
                assertThrows(ExecutionException.class,() -> pool.openPipe().toCompletableFuture().get(2,TimeUnit.SECONDS));
                QuicEndpointTest.closed(a,b);until(() -> pool.connections().isEmpty() && !pooled.isActive());
            } finally { QuicEndpointTest.closed(a,b); }
        }
    }
    @Test void cancellationDoesNotReturnAPendingNativeStreamSlotEarlyAndCountsTheCreatedStream() throws Exception {
        try(var f=new Fixture();var pool=f.pool(1,2,Duration.ZERO)) {
            var first=firstPipe(f,pool);
            var connection=(io.netty.handler.codec.quic.QuicChannel)first.carrierConn();release(first);
            until(() -> pool.connections().getFirst().active()==0);
            var entered=new CountDownLatch(1);var proceed=new CountDownLatch(1);
            connection.eventLoop().execute(() -> { entered.countDown();try { proceed.await(3,TimeUnit.SECONDS); } catch(InterruptedException e) { Thread.currentThread().interrupt(); } });
            assertTrue(entered.await(2,TimeUnit.SECONDS));
            try {
                var pending=pool.openPipe().toCompletableFuture();assertTrue(pending.cancel(false));
                assertEquals(1,pool.connections().getFirst().active(),"cancelled notification is not native resource completion");
            } finally { proceed.countDown(); }
            // Disposal closes the channel asynchronously on its event loop; wait for both effects.
            until(() -> pool.connections().isEmpty() && !connection.isActive());
        }
    }
    @Test void failedConnectionsAreNotReusedAndOneCancelledWaiterDoesNotCancelOtherDialWaiters() throws Exception {
        try(var f=new Fixture();var pool=f.pool(2,0,Duration.ZERO)) {
            var old=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            ((io.netty.handler.codec.quic.QuicChannel)old.carrierConn()).close().sync();until(() -> pool.connections().isEmpty());
            var next=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            assertNotSame(old.carrierConn(),next.carrierConn());assertNotEquals(old.capabilities().fateGroup(),next.capabilities().fateGroup());release(next);
        }
        try(var f=new Fixture();var blackhole=new java.net.DatagramSocket(QuicTest.any());
            var pool=new QuicPool(f.client,(java.net.InetSocketAddress)blackhole.getLocalSocketAddress(),"localhost")) {
            var one=pool.openPipe().toCompletableFuture();var two=pool.openPipe().toCompletableFuture();
            assertTrue(one.cancel(false));Thread.sleep(30);assertFalse(two.isDone());
            pool.close();assertThrows(ExecutionException.class,() -> two.get(3,TimeUnit.SECONDS));
        }
    }
    @Test void closingPoolWinsAgainstSuccessWhileFailureNotificationsAreStillRunning() throws Exception {
        try(var f=new Fixture();var pool=f.pool(2,0,Duration.ZERO);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            var warm=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            var connection=(io.netty.handler.codec.quic.QuicChannel)warm.carrierConn();release(warm);
            until(() -> pool.connections().getFirst().active()==0);
            var nativeEntered=new CountDownLatch(1);var nativeProceed=new CountDownLatch(1);
            var failed=new CountDownLatch(1);var finishFailure=new CountDownLatch(1);
            var once=new java.util.concurrent.atomic.AtomicBoolean();
            connection.eventLoop().execute(() -> { nativeEntered.countDown();try { nativeProceed.await(3,TimeUnit.SECONDS); } catch(InterruptedException e) { Thread.currentThread().interrupt(); } });
            assertTrue(nativeEntered.await(2,TimeUnit.SECONDS));
            var one=pool.openPipe().toCompletableFuture();var two=pool.openPipe().toCompletableFuture();
            for(var future:java.util.List.of(one,two)) future.whenComplete((value,error) -> {
                if(error!=null && once.compareAndSet(false,true)) {
                    failed.countDown();try { finishFailure.await(3,TimeUnit.SECONDS); } catch(InterruptedException e) { Thread.currentThread().interrupt(); }
                }
            });
            var stopping=workers.submit(pool::close);
            try {
                assertTrue(failed.await(2,TimeUnit.SECONDS));nativeProceed.countDown();
                until(() -> pool.connections().isEmpty());
                assertTrue(!one.isDone() || one.isCompletedExceptionally());
                assertTrue(!two.isDone() || two.isCompletedExceptionally());
            } finally { nativeProceed.countDown();finishFailure.countDown();stopping.get(4,TimeUnit.SECONDS); }
        }
    }
    @Test void cancelledOldFactoryCannotStartAlongsideItsReplacement() throws Exception {
        try(var f=new Fixture();var pool=f.pool(1,0,Duration.ZERO)) {
            var field=QuicClient.class.getDeclaredField("pending");field.setAccessible(true);
            @SuppressWarnings("unchecked") var original=(java.util.Set<CompletableFuture<?>>)field.get(f.client);
            var calls=new java.util.concurrent.atomic.AtomicInteger();var entered=new CountDownLatch(1);var proceed=new CountDownLatch(1);
            java.util.Set<CompletableFuture<?>> gate=new java.util.AbstractSet<>() {
                @Override public java.util.Iterator<CompletableFuture<?>> iterator() { return original.iterator(); }
                @Override public int size() { return original.size(); }
                @Override public boolean remove(Object value) { return original.remove(value); }
                @Override public boolean add(CompletableFuture<?> value) {
                    if(calls.incrementAndGet()==2) { entered.countDown();try { proceed.await(3,TimeUnit.SECONDS); } catch(InterruptedException e) { Thread.currentThread().interrupt(); } }
                    return original.add(value);
                }
            };
            field.set(f.client,gate);
            var one=pool.openPipe().toCompletableFuture();var two=pool.openPipe().toCompletableFuture();
            CompletableFuture<PipeIo> replacement=null;
            try {
                assertTrue(entered.await(3,TimeUnit.SECONDS));
                for(var future:java.util.List.of(one,two)) if(!future.cancel(false)) future.thenAccept(QuicPoolTest::release);
                replacement=pool.openPipe().toCompletableFuture();
                assertEquals(2,calls.get(),"new factory started before the cancelled factory returned its handle");
            } finally { proceed.countDown(); }
            if(replacement!=null) release(replacement.get(8,TimeUnit.SECONDS));
        }
    }
    @Test void streamObserverSeesInitializationBeforeRegistrationFailure() throws Exception {
        try(var f=new Fixture();var pool=f.pool(2,0,Duration.ZERO)) {
            var warm=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            var connection=(io.netty.handler.codec.quic.QuicChannel)warm.carrierConn();
            var carrier=connection.attr(ConnectionCarrier.KEY).get();
            var seen=new CompletableFuture<io.netty.handler.codec.quic.QuicStreamChannel>();
            var failures=new java.util.concurrent.atomic.AtomicInteger();
            var broken=carrier.openManagedPipe(new ConnectionCarrier.StreamObserver() {
                @Override public void created(io.netty.handler.codec.quic.QuicStreamChannel stream) { seen.complete(stream);throw new IllegalStateException("initializer rejected stream"); }
                @Override public void failed(Throwable error) { failures.incrementAndGet(); }
            }).toCompletableFuture();
            assertThrows(ExecutionException.class,() -> broken.get(3,TimeUnit.SECONDS));
            var failedStream=seen.get(3,TimeUnit.SECONDS);until(() -> failedStream.closeFuture().isDone());assertEquals(1,failures.get());
            assertTrue(connection.isActive());release(warm);connection.close().sync();
            var notSeen=new java.util.concurrent.atomic.AtomicInteger();var failedEarly=new java.util.concurrent.atomic.AtomicInteger();
            assertThrows(ExecutionException.class,() -> carrier.openManagedPipe(new ConnectionCarrier.StreamObserver() {
                @Override public void created(io.netty.handler.codec.quic.QuicStreamChannel stream) { notSeen.incrementAndGet(); }
                @Override public void failed(Throwable error) { failedEarly.incrementAndGet(); }
            }).toCompletableFuture().get(3,TimeUnit.SECONDS));
            assertEquals(0,notSeen.get());assertEquals(1,failedEarly.get());
        }
    }
    @Test void oneNativeConnectionServesSeveralSessionsAndClosingOneDoesNotCloseTheOthers() throws Exception {
        try(var f=new Fixture();var pool=f.pool(2,0,Duration.ZERO);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            var first=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            var a=f.local.open(first, Role.CLIENT);var b=f.remote.accept(Duration.ofSeconds(5));assertNotNull(b);
            var second=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);
            assertSame(first.carrierConn(),second.carrierConn());
            var c=f.local.open(second, Role.CLIENT);var d=f.remote.accept(Duration.ofSeconds(5));assertNotNull(d);
            try {
                assertEquals(1,pool.connections().size());assertEquals(2,pool.connections().getFirst().active());
                QuicEndpointTest.closed(a,b);until(() -> pool.connections().getFirst().active()==1);
                QuicTest.transfer(workers,c,d,2,128<<10);
                var third=pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS);assertSame(second.carrierConn(),third.carrierConn());release(third);
                pool.close();QuicTest.transfer(workers,d,c,2,64<<10);QuicEndpointTest.closed(c,d);until(() -> pool.connections().isEmpty());
            } finally { QuicEndpointTest.closed(a,b);QuicEndpointTest.closed(c,d); }
        }
    }
    @Test void maintenanceRefillsFromThePoolWithoutLosingPipeOwnership() throws Exception {
        try(var f=new Fixture();var pool=f.pool(2,0,Duration.ZERO);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            var a=f.client.connect(f.server.address(),"localhost").toCompletableFuture().get(8,TimeUnit.SECONDS);
            var b=f.remote.accept(Duration.ofSeconds(5));assertNotNull(b);
            var maintenance=a.paths().maintain(PipePool.carriers(cancel -> QuicEndpointTest.awaitPipe(pool.openPipe())).withCount(2)
                    .withInspectionInterval(Duration.ofMillis(5)).withBackoff(new PipePool.Backoff(Duration.ofMillis(10),Duration.ofMillis(40),0)));
            try {
                until(() -> maintenance.pipes().size()==2);assertEquals(1,pool.connections().size());
                var old=maintenance.pipes().getFirst();old.abort();
                until(() -> maintenance.pipes().size()==2 && maintenance.pipes().stream().noneMatch(p -> p.id()==old.id()));
                assertEquals(1,pool.connections().size());assertTrue(pool.connections().getFirst().total()>=3);
                QuicTest.transfer(workers,a,b,2,128<<10);
            } finally {
                maintenance.close();maintenance.completion().toCompletableFuture().get(3,TimeUnit.SECONDS);
                QuicEndpointTest.closed(a,b);
            }
        }
    }

    static PipeIo get(QuicPool pool) throws Exception { return pool.openPipe().toCompletableFuture().get(5,TimeUnit.SECONDS); }
    @Test void policyCountSpreadsPipesOverThatManyConnections() throws Exception {
        PoolPolicy policy = v -> PoolPolicy.Decision.KEEP.withCount(3);
        try (var f=new Fixture();var pool=f.pool(8,policy)) {
            var pipes=new ArrayList<PipeIo>();
            try {
                for (int i=0;i<3;i++) pipes.add(get(pool));
                assertEquals(3,pool.connections().size());
                for (int i=0;i<3;i++) pipes.add(get(pool));
                assertEquals(3,pool.connections().size());
                assertTrue(pool.connections().stream().allMatch(c -> c.active()==2));
            } finally { pipes.forEach(QuicPoolTest::release); }
        }
    }
    @Test void policyDialAfterSpacesAddedConnections() throws Exception {
        var since=new AtomicReference<Duration>();
        PoolPolicy policy = v -> { since.set(v.sinceLastDial()); return PoolPolicy.Decision.KEEP.withCount(2).withDialAfter(Duration.ofMillis(400)); };
        try (var f=new Fixture();var pool=f.pool(8,policy)) {
            var pipes=new ArrayList<PipeIo>();
            try {
                pipes.add(get(pool)); pipes.add(get(pool));
                assertEquals(1,pool.connections().size(),"second connection is not due yet");
                assertNotNull(since.get());
                Thread.sleep(450);
                pipes.add(get(pool));
                assertEquals(2,pool.connections().size());
            } finally { pipes.forEach(QuicPoolTest::release); }
        }
    }
    @Test void policyRetireReplacesTheConnectionBeforeItsSessionsEnd() throws Exception {
        var retire=new AtomicLong(-1);
        PoolPolicy policy = v -> PoolPolicy.Decision.KEEP.withRetire(retire.get()<0 ? Set.of() : Set.of(retire.get()));
        try(var f=new Fixture();var pool=f.pool(8,policy);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            var first=get(pool);
            var a=f.local.open(first, Role.CLIENT);var b=f.remote.accept(Duration.ofSeconds(5));assertNotNull(b);
            try {
                var old=(io.netty.handler.codec.quic.QuicChannel)first.carrierConn();
                var spare=get(pool);assertSame(old,spare.carrierConn());
                retire.set(first.capabilities().fateGroup());
                release(spare); // the release asks the policy, which retires the connection
                until(() -> pool.connections().size()==2 && pool.connections().stream().anyMatch(c -> !c.retiring()));
                assertTrue(old.isActive());
                QuicTest.transfer(workers,a,b,2,64<<10);
                var next=get(pool);assertNotSame(old,next.carrierConn());release(next);
                QuicEndpointTest.closed(a,b);
                until(() -> !old.isActive() && pool.connections().size()==1);
            } finally { QuicEndpointTest.closed(a,b); }
        }
    }
    @Test void policyExceptionsAndNullDecisionsKeepThePoolBehaviour() throws Exception {
        var calls=new AtomicInteger();
        PoolPolicy policy = v -> { if (calls.incrementAndGet()%2==0) return null; throw new IllegalStateException("policy failure"); };
        try (var f=new Fixture();var pool=f.pool(2,policy)) {
            var pipes=new ArrayList<PipeIo>();
            try {
                for (int i=0;i<3;i++) pipes.add(get(pool));
                assertEquals(2,pool.connections().size());
                assertTrue(calls.get()>=3);
            } finally { pipes.forEach(QuicPoolTest::release); }
            until(() -> pool.connections().stream().allMatch(c -> c.active()==0));
        }
    }
    @Test void machinePolicyAgeRuleRotatesConnectionsUnderALiveSession() throws Exception {
        PoolPolicy policy = io.zmux.shaping.machine.Machines.poolPolicy(
                "{\"count\":{\"const\":1},\"retire\":[{\"metric\":\"age\",\"above\":{\"const\":0.3}}]}".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                io.zmux.shaping.machine.Options.defaults());
        try(var f=new Fixture();var pool=f.pool(8,policy);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            var first=get(pool);long fate=first.capabilities().fateGroup();
            var a=f.local.open(first, Role.CLIENT);var b=f.remote.accept(Duration.ofSeconds(5));assertNotNull(b);
            try {
                Thread.sleep(350);
                release(get(pool));
                until(() -> pool.connections().stream().anyMatch(c -> !c.retiring() && c.fateGroup()!=fate));
                assertTrue(pool.connections().stream().anyMatch(c -> c.retiring() && c.fateGroup()==fate));
                QuicTest.transfer(workers,a,b,2,64<<10);
                QuicEndpointTest.closed(a,b);
                until(() -> pool.connections().stream().noneMatch(c -> c.fateGroup()==fate));
            } finally { QuicEndpointTest.closed(a,b); }
        }
    }
    @Test void machinePolicyAgeRuleRetiresABusyConnectionByRecheckAlone() throws Exception {
        PoolPolicy policy = io.zmux.shaping.machine.Machines.poolPolicy(
                "{\"count\":{\"const\":1},\"retire\":[{\"metric\":\"age\",\"above\":{\"const\":0.3}}]}".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                io.zmux.shaping.machine.Options.defaults());
        try(var f=new Fixture();var pool=f.pool(8,policy);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
            var first=get(pool);long fate=first.capabilities().fateGroup();
            var a=f.local.open(first, Role.CLIENT);var b=f.remote.accept(Duration.ofSeconds(5));assertNotNull(b);
            try {
                // No pipe is taken or released: only the recheck asks the policy again.
                until(() -> pool.connections().stream().anyMatch(c -> !c.retiring() && c.fateGroup()!=fate));
                assertTrue(pool.connections().stream().anyMatch(c -> c.retiring() && c.fateGroup()==fate));
                QuicTest.transfer(workers,a,b,2,64<<10);
            } finally { QuicEndpointTest.closed(a,b); }
            until(() -> pool.connections().stream().noneMatch(c -> c.fateGroup()==fate));
        }
    }
    @Test void handWrittenRecheckIsHonouredAndFloored() throws Exception {
        var calls=new AtomicInteger();
        PoolPolicy policy = v -> { calls.incrementAndGet(); return PoolPolicy.Decision.KEEP.withRecheck(Duration.ofNanos(1)); };
        try (var f=new Fixture();var pool=f.pool(8,policy)) {
            var pipe=get(pool);
            try {
                int before=calls.get();
                Thread.sleep(300);
                int n=calls.get()-before;
                assertTrue(n>=5, n+" rechecks in 300 ms");
                assertTrue(n<=45, n+" rechecks in 300 ms: the 10 ms floor does not hold");
            } finally { release(pipe); }
        }
    }
    /** A recheck that fires after a newer one was scheduled leaves the newer one pending. */
    @Test void aStaleRecheckKeepsTheNewerOne() throws Exception {
        var calls=new AtomicInteger();
        PoolPolicy policy = v -> { calls.incrementAndGet(); return PoolPolicy.Decision.KEEP.withRecheck(Duration.ofSeconds(30)); };
        try (var f=new Fixture();var pool=f.pool(8,policy)) {
            var pipe=get(pool);
            try {
                var recheck=QuicPool.class.getDeclaredField("recheck"); recheck.setAccessible(true);
                var generation=QuicPool.class.getDeclaredField("recheckGeneration"); generation.setAccessible(true);
                var fired=QuicPool.class.getDeclaredMethod("rechecked", long.class); fired.setAccessible(true);
                Object pending; long g;
                synchronized (pool) { pending=recheck.get(pool); g=(long) generation.get(pool); }
                assertNotNull(pending);
                int before=calls.get();
                fired.invoke(pool, g-1); // an older recheck, replaced while it fired
                synchronized (pool) { assertSame(pending, recheck.get(pool), "the newer recheck was dropped"); }
                assertEquals(before, calls.get(), "a stale recheck asked the policy");
                fired.invoke(pool, g); // the pending one
                assertTrue(calls.get()>before);
            } finally { release(pipe); }
        }
    }
    /** A connection member is idle 0 while it carries a pipe, else since its last pipe ended (or since it was added). */
    @Test void connectionIdleFollowsItsPipes() throws Exception {
        var seen=new AtomicReference<List<PoolPolicy.Member>>();
        PoolPolicy policy = v -> { seen.set(List.copyOf(v.members())); return PoolPolicy.Decision.KEEP.withRecheck(Duration.ofMillis(10)); };
        try (var f=new Fixture();var pool=f.pool(8,policy)) {
            var pipe=get(pool);
            Thread.sleep(50);
            until(() -> seen.get().size()==1 && seen.get().get(0).pipes()==1);
            assertEquals(Duration.ZERO, seen.get().get(0).idle());
            release(pipe);
            until(() -> seen.get().get(0).pipes()==0 && seen.get().get(0).idle().toMillis()>=30);
            assertTrue(seen.get().get(0).age().compareTo(seen.get().get(0).idle())>0, "idle counts from the last pipe, not from the connection's birth");
        }
    }
}
