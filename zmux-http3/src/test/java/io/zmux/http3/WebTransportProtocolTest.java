package io.zmux.http3;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http3.DefaultHttp3Headers;
import io.netty.handler.codec.http3.Http3Settings;
import io.netty.handler.codec.quic.QuicStreamType;
import io.zmux.HttpStatusException;
import io.zmux.ListenerConfig;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.http.HttpCarrierClient;
import io.zmux.http.HttpEndpoint;
import io.zmux.internal.spi.CarrierBudget;
import io.zmux.internal.spi.ChannelDemux;
import io.zmux.quic.internal.QuicVarInts;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class WebTransportProtocolTest {
    private static final URI TARGET = URI.create("https://localhost/p%2Fq?a=1");

    @Test void advertisesCompleteSettingsAndAcceptsLegacyCapabilities() {
        var wtKeys = List.of(0x2c7cf000L, 0x2b603742L, 0x2b603743L, 0x14e9cd29L, 0xc671706aL);
        for (boolean server : List.of(false, true)) {
            var local = WebTransportSettings.advertise(server);
            for (long key : wtKeys) assertEquals(1, local.getOrDefault(key, 0));
            assertEquals(server ? Boolean.TRUE : null, local.connectProtocolEnabled());
            assertTrue(local.h3DatagramEnabled());
            assertEquals(server ? 7 : 6, java.util.stream.StreamSupport.stream(local.spliterator(), false).count());
        }
        for (long wireId : wtKeys) {
            var settings = new Http3Settings(WebTransportSettings::validate);
            settings.put(wireId, wireId == 0x2c7cf000L ? 1L : 2L);
            assertFalse(WebTransportSettings.read(settings).permits(false, true, false));
            settings.enableH3Datagram(true);
            var profile = WebTransportSettings.read(settings);
            assertTrue(profile.permits(false, true, false));
            assertFalse(profile.permits(true, true, false));
            assertFalse(profile.permits(false, false, true));
            settings.enableConnectProtocol(true);
            assertTrue(WebTransportSettings.read(settings).permits(true, true, false));
            settings.put(wireId, 0L);
            assertFalse(WebTransportSettings.read(settings).permits(true, true, false));
        }
        var settings = new Http3Settings(WebTransportSettings::validate);
        for (long ignored : List.of(0x21L, 0x2b61L, 0x2b64L, 0x2b65L, 0xffd277L)) {
            settings.put(ignored, 1L << 60);
            assertNull(settings.get(ignored));
        }
        assertFalse(WebTransportSettings.read(settings).permits(false, true, true));
        settings.enableH3Datagram(true);
        assertTrue(WebTransportSettings.read(settings).permits(false, true, true));
        assertFalse(WebTransportSettings.read(settings).permits(false, true, false));
        assertThrows(IllegalArgumentException.class, () -> settings.put(0x2b64L, (1L << 60) + 1));
        assertThrows(IllegalArgumentException.class, () -> settings.put(WebTransportSettings.WT_ENABLED, 2L));
        assertNull(settings.put(WebTransportSettings.WT_ENABLED, 1L));
        assertEquals(1L, settings.put(WebTransportSettings.WT_ENABLED, 0L));
    }

    @Test void v2RawConnectInformationalResponseRefusalsAndCredentialsBoundary() throws IOException {
        var head = WebTransportConnect.request(TARGET, Map.of("X-Http3-Protocol", List.of("webtransport")));
        var headers = head.headers();
        assertEquals("CONNECT", headers.method().toString());
        assertEquals("https", headers.scheme().toString());
        assertEquals("localhost", headers.authority().toString());
        assertEquals("/p%2Fq?a=1", headers.path().toString());
        assertEquals("webtransport-h3", headers.protocol().toString());
        assertTrue(WebTransportConnection.accepts(headers.protocol()));
        assertTrue(WebTransportConnection.accepts("webtransport"));
        assertFalse(WebTransportConnection.accepts(null));
        assertFalse(WebTransportConnection.accepts("websocket"));
        assertEquals(0, WebTransportRequest.refusal(headers));
        headers.add("origin", "https://other.test");
        assertEquals(403, WebTransportRequest.refusal(headers));
        headers.add("early-data", "1");
        assertEquals(425, WebTransportRequest.refusal(headers));
        assertThrows(IllegalArgumentException.class, () -> WebTransportConnect.request(TARGET, Map.of(":protocol", List.of("x"))));
        assertThrows(IllegalArgumentException.class, () -> WebTransportConnect.request(URI.create("http://localhost"), Map.of()));
        var response = new DefaultHttp3Headers();
        for (int code : List.of(100, 103, 200, 204, 299)) {
            assertEquals(code, WebTransportConnect.status(response.status(Integer.toString(code))));
        }
        for (int code : List.of(101, 302, 403, 405, 425, 429, 599)) {
            assertEquals(code, assertThrows(HttpStatusException.class,
                    () -> WebTransportConnect.status(response.status(Integer.toString(code)))).status());
        }
        assertThrows(IOException.class, () -> WebTransportConnect.status(response.status("999")));
        assertThrows(IllegalArgumentException.class,
                () -> WebTransportConnect.request(TARGET, Map.of("wt-available-protocols", List.of("\"zmux\""))));
    }

    @Test void v3IncrementalPrefacesAndOrdinaryHttpReplay() {
        for (var type : QuicStreamType.values()) {
            long marker = type == QuicStreamType.BIDIRECTIONAL ? 0x41 : 0x54;
            var prefix = wire(marker, 16384);
            try {
                for (int n = 0; n < prefix.readableBytes(); n++) {
                    var fragment = prefix.slice(0, n);
                    assertEquals(WebTransportConnection.MORE, WebTransportConnection.readPreface(fragment, type));
                    assertEquals(0, fragment.readerIndex());
                }
                prefix.writeByte(19);
                assertEquals(16384, WebTransportConnection.readPreface(prefix, type));
                assertEquals(19, prefix.readUnsignedByte());
            } finally { prefix.release(); }
        }
        var http = wire(1, 42);
        try {
            assertEquals(WebTransportConnection.HTTP, WebTransportConnection.readPreface(http, QuicStreamType.BIDIRECTIONAL));
            assertEquals(0, http.readerIndex());
        } finally { http.release(); }
    }

    @Test void v3SkipsUnknownFramesAcrossFragments() {
        for (int preamble = 0; preamble <= 2; preamble++) {
            var decoder = new EmbeddedChannel(new WebTransportHttpFrames(preamble));
            var expected = Unpooled.buffer();
            var wire = Unpooled.buffer();
            var decoded = Unpooled.buffer();
            try {
                for (int i = 0; i < preamble; i++) QuicVarInts.write(expected, 0x41);
                if (preamble == 1) {
                    QuicVarInts.write(expected, 4);
                    QuicVarInts.write(expected, 0);
                }
                wire.writeBytes(expected, expected.readerIndex(), expected.readableBytes());
                for (long unknown : List.of(0x21L, 0x1a2b3cL, 0x1_0000_0002L)) {
                    QuicVarInts.write(wire, unknown);
                    QuicVarInts.write(wire, 2);
                    wire.writeShort(0x4041);
                }
                var data = wire(0, 2, 0x41);
                wire.writeBytes(data, data.readerIndex(), data.readableBytes());
                expected.writeBytes(data);
                data.release();
                while (wire.isReadable()) {
                    decoder.writeInbound(wire.readRetainedSlice(1));
                    for (ByteBuf frame; (frame = decoder.readInbound()) != null; ) {
                        decoded.writeBytes(frame);
                        frame.release();
                    }
                }
                assertEquals(expected, decoded);
            } finally {
                decoder.finishAndReleaseAll();
                expected.release();
                wire.release();
                decoded.release();
            }
        }

        var decoder = new EmbeddedChannel(new WebTransportHttpFrames(0));
        try {
            assertFalse(decoder.writeInbound(wire(0x21, 1L << 32).writeByte(0)));
        } finally {
            decoder.finishAndReleaseAll();
        }
    }

    @Test void v4QuarterIdBeforeFullPipeIdOnTheSelectedConnection() {
        var sessions = Map.of(0L, new ChannelDemux(), 4L, new ChannelDemux());
        var seen = new ArrayList<Integer>();
        sessions.get(0L).register(8, (buf, from, to) -> fail("cross-session delivery"));
        sessions.get(4L).register(8, (buf, from, to) -> seen.add((int) buf.get(from)));
        var packet = wire(1, 8, 0x30);
        try {
            assertEquals("010830", ByteBufUtil.hexDump(packet));
            var session = sessions.get(WebTransportConnection.datagramSessionId(packet));
            session.deliver(packet.nioBuffer(), 0, packet.readableBytes());
            assertEquals(List.of(0x30), seen);
        } finally { packet.release(); }
        for (long q : List.of(0L, 63L, 64L, 1L << 60)) {
            var encoded = wire(q);
            try { assertEquals(q == 1L << 60 ? -1 : 4 * q, WebTransportConnection.datagramSessionId(encoded)); }
            finally { encoded.release(); }
        }
        var truncated = Unpooled.wrappedBuffer(new byte[]{0x40});
        try { assertEquals(-1, WebTransportConnection.datagramSessionId(truncated)); }
        finally { truncated.release(); }
    }

    @Test void v5MappingPreservesFullCodesAndDirection() {
        for (long n : List.of(0L, 29L, 30L, 0xffff_ffffL)) {
            long code = WebTransportStreamException.wireCode(n);
            assertTrue(code > Integer.MAX_VALUE);
            var reset = new WebTransportStreamException(false, code, 17, 3);
            assertEquals(n, reset.applicationCode().orElseThrow());
            assertEquals(17, reset.finalSize().orElseThrow());
            assertEquals(3, reset.reliableSize().orElseThrow());
            var stop = new WebTransportStreamException(true, code, 0, 0);
            assertTrue(stop.stopSending());
            assertTrue(stop.finalSize().isEmpty());
        }
        for (long code : List.of(0x170d7b68L, WebTransportStreamException.wireCode(29) + 1)) {
            var reset = new WebTransportStreamException(false, code, 7, 4);
            assertEquals(code, reset.wireCode());
            assertTrue(reset.applicationCode().isEmpty());
        }
    }

    @Test void v6IgnoresUnknownAndFlowControlCapsulesAcrossEveryByte() throws IOException {
        var seen = new ArrayList<String>();
        var parser = decoder(seen);
        var close = WebTransportCapsules.close(ByteBufAllocator.DEFAULT, 42, "再见");
        var drain = WebTransportCapsules.drain(ByteBufAllocator.DEFAULT);
        var frames = wire(19, 2).writeShort(0x1212);
        try {
            for (long credit = 0x190b4d3dL; credit <= 0x190b4d44L; credit++) {
                QuicVarInts.write(frames, credit);
                QuicVarInts.write(frames, 1);
                frames.writeByte(0xff);
            }
            frames.writeBytes(drain.content()).writeBytes(close.content());
            while (frames.isReadable()) parser.read(frames.readSlice(1));
            parser.end();
            assertEquals(List.of("DRAIN", "42 再见"), seen);
            assertThrows(IOException.class, () -> parser.read(frames.clear().writeByte(0)));
        } finally { frames.release(); drain.release(); close.release(); }
        var exact = WebTransportCapsules.close(ByteBufAllocator.DEFAULT, 0xffff_ffffL, "x".repeat(1024));
        try { decoder(new ArrayList<>()).read(exact.content()); } finally { exact.release(); }
        assertThrows(IllegalArgumentException.class, () -> WebTransportCapsules.close(ByteBufAllocator.DEFAULT, 0, "x".repeat(1025)));
        assertThrows(IllegalArgumentException.class, () -> WebTransportCapsules.close(ByteBufAllocator.DEFAULT, 0, String.valueOf((char) 0xd800)));
        for (ByteBuf bad : List.of(wire(0x2843, 3), wire(0x2843, 1029), wire(0x78ae, 1),
                wire(0x2843, 5).writeInt(0).writeByte(0xff), wire(0x2843, 4).writeByte(0), wire(0x78ae))) {
            var decoder = decoder(new ArrayList<>());
            try { assertThrows(IOException.class, () -> { decoder.read(bad); decoder.end(); }); }
            finally { bad.release(); }
        }
        seen.clear();
        decoder(seen).end();
        assertEquals(List.of("0 "), seen);
    }

    @Test void v8FirstPipeWithdrawalAndAbsoluteReorderDeadline() {
        var withdrawals = new AtomicInteger();
        var delayed = new WebTransportFirstPipe(withdrawals::incrementAndGet);
        var io = delayed.pipe(this);
        assertFalse(delayed.first.isDone());
        io.reader().close();
        io.writer().close();
        assertTrue(delayed.first.isCompletedExceptionally());
        assertEquals(1, withdrawals.get());
        var loop = new EmbeddedChannel();
        var clock = new AtomicLong(40);
        var held = new AtomicInteger();
        var reorder = new WebTransportReorder(loop.eventLoop(), clock::get, 10, 1);
        var discarded = new ArrayList<Boolean>();
        var input = new WebTransportReorder.Input() {
            @Override public void deliver(WebTransportSession session) { fail("expired input delivered"); }
            @Override public void discard(boolean retired) { discarded.add(retired); }
        };
        CarrierBudget budget = (physical, capacity, bytes) -> {
            held.incrementAndGet();
            return held::decrementAndGet;
        };
        try {
            var cancel = reorder.hold(4, 2, budget, this, () -> input);
            assertNotNull(cancel);
            assertNull(reorder.hold(8, 0, budget, this, () -> fail("retained at capacity")));
            clock.set(50);
            loop.advanceTimeBy(10, TimeUnit.NANOSECONDS);
            loop.runScheduledPendingTasks();
            cancel.run();
            reorder.close();
            assertEquals(List.of(false), discarded);
            assertEquals(0, held.get());
        } finally { loop.finishAndReleaseAll(); }
    }

    @Test void v10ReorderReservesBothTheBackingAndDescriptorBeforeCopy() {
        var channel = new EmbeddedChannel();
        var budget = CarrierBudget.standalone(CarrierBudget.DELIVERY_BYTES + 2);
        var reorder = new WebTransportReorder(channel.eventLoop(), () -> 0, Long.MAX_VALUE, 2);
        var physical = new Object();
        var released = new AtomicInteger();
        try {
            var input = new WebTransportReorder.Input() {
                @Override public void deliver(WebTransportSession session) { fail(); }
                @Override public void discard(boolean retired) { released.incrementAndGet(); }
            };
            assertNotNull(reorder.hold(4, 2, budget, physical, () -> input));
            assertNull(reorder.hold(4, 1, budget, physical, () -> fail("copied without budget")));
            reorder.resolve(4, null);
            assertEquals(1, released.get());
            assertNotNull(reorder.hold(8, 2, budget, physical, () -> input));
            reorder.close();
            assertEquals(2, released.get());
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test void v11NettyUsesOrdinaryResetAndPreservesRepresentableCodes() throws Exception {
        try (var h2 = HttpCarrierClient.builder().build();
             var listener = Zmux.listener(ListenerConfig.of(ZmuxConfig.builder().build()));
             var endpoint = HttpEndpoint.builder(listener).webTransport().build()) {
            assertThrows(IllegalArgumentException.class, () -> Http3Client.openWebTransportAsync(h2, TARGET, Map.of(), null, Duration.ofSeconds(1)));
            assertThrows(IllegalArgumentException.class, () -> Http3Server.requestInitializer(endpoint, null));
            assertEquals(WebTransportConnection.SESSION_GONE, NettyWebTransportProvider.sendCode(WebTransportConnection.SESSION_GONE));
            assertEquals(0x10c, NettyWebTransportProvider.sendCode(WebTransportStreamException.wireCode(42)));
            var reset = new WebTransportStreamException(false, WebTransportStreamException.wireCode(42), -1, -1);
            assertEquals(42, reset.applicationCode().orElseThrow());
            assertTrue(reset.finalSize().isEmpty());
            assertTrue(reset.reliableSize().isEmpty());
            assertEquals(List.of("close", "closeAsync", "closed", "drainAsync", "draining", "openPipeAsync"),
                    Arrays.stream(WebTransportSession.class.getDeclaredMethods())
                            .filter(m -> Modifier.isPublic(m.getModifiers()))
                            .map(Method::getName).sorted().toList());
        }
    }

    private static WebTransportCapsules decoder(List<String> seen) {
        return new WebTransportCapsules(new WebTransportCapsules.Receiver() {
            @Override public void peerClosed(long code, String reason) { seen.add(code + " " + reason); }
            @Override public void peerDraining() { seen.add("DRAIN"); }
        });
    }

    private static ByteBuf wire(long... values) {
        var result = Unpooled.buffer();
        for (long value : values) QuicVarInts.write(result, value);
        return result;
    }
}
