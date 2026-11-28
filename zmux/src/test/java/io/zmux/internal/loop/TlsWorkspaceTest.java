package io.zmux.internal.loop;

import static org.junit.jupiter.api.Assertions.*;

import io.zmux.internal.memory.MemoryBudget;
import io.zmux.internal.memory.SharedMemory;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class TlsWorkspaceTest {
    private static final long RECORDS = 6 * 18_437;

    @Test void creationRequiresTheWholeRecordWorkspaceAndCloseReturnsIt() throws Exception {
        var loop = new EventLoop("tls-workspace", true, BufferPool.DEFAULT_IDLE_BYTES);
        var budget = new MemoryBudget(RECORDS - 1, null);
        var engine = SSLContext.getDefault().createSSLEngine();
        try (var channel = SocketChannel.open()) {
            ExternalTransportTest.run(loop, () -> {
                assertThrows(SharedMemory.Unavailable.class, () -> TlsTransport.create(loop, channel, engine, budget));
                assertEquals(0, budget.used());

                long attachment = ChannelReader.WORKING_BYTES + CarrierCapacity.OUTBOUND_WORKSPACE;
                budget.limit(RECORDS + attachment);
                var carrier = TlsTransport.create(loop, channel, engine, budget);
                try {
                    assertEquals(RECORDS, budget.used(), "the transport owns the charge before handshake");
                    assertTrue(CarrierCapacity.reserve(carrier, budget, false));
                    assertEquals(RECORDS + attachment, budget.used(), "attachment excludes the prepaid TLS records");
                } finally {
                    carrier.reject(null);
                }
                assertEquals(0, budget.used());
            });
        } finally {
            loop.close();
        }
    }

    @Test void forwardedRecordsRemainChargedInBothLedgersUntilTheSocketCloses() throws Exception {
        var loop = new EventLoop("forwarded-tls-workspace", true, BufferPool.DEFAULT_IDLE_BYTES);
        var origin = new MemoryBudget(RECORDS, null);
        var pressureTransitions = new CountDownLatch(2);
        var target = new MemoryBudget(Long.MAX_VALUE, pressureTransitions::countDown);
        var engine = SSLContext.getDefault().createSSLEngine();
        try (var channel = SocketChannel.open()) {
            ExternalTransportTest.run(loop, () -> {
                var carrier = TlsTransport.create(loop, channel, engine, origin);
                var upper = new ExternalTransport(loop, carrier.asPipe());
                try {
                    long attachment = upper.privateWorkspace() + upper.inputQueueCapacity()
                            + upper.info().receiveMemory() + CarrierCapacity.OUTBOUND_WORKSPACE;
                    target.limit(attachment + RECORDS - 1);
                    assertFalse(CarrierCapacity.reserve(upper, target, false));
                    assertEquals(0, target.used());
                    target.limit(attachment + RECORDS);
                    assertTrue(CarrierCapacity.reserve(upper, target, false));
                    assertEquals(attachment + RECORDS, target.used());
                    assertEquals(RECORDS, origin.used());

                    upper.reject(null);
                    // The forwarding halves post the actual socket close back to this owner.
                    assertTrue(channel.isOpen());
                    assertEquals(attachment + RECORDS, target.used());
                    assertEquals(RECORDS, origin.used());
                } finally {
                    carrier.reject(null);
                    upper.reject(null);
                }
            });
            // Exact admission enters pressure. The records are less than a quarter of the
            // attachment, so pressure clears when the final attachment charge is released.
            pressureTransitions.await();
            assertEquals(0, origin.used());
            assertEquals(0, target.used());
        } finally {
            loop.close();
        }
    }
}
