package io.zmux;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;

final class MetadataValueTypeTest {
    @Test
    void openOptionsRemainsDefensiveWhileReusingEmptySingleton() {
        byte[] openInfo = new byte[]{1, 2, 3};
        OpenOptions options = new OpenOptions(7L, 9L, openInfo);
        openInfo[0] = 9;

        assertArrayEquals(new byte[]{1, 2, 3}, options.openInfo());

        byte[] exposed = options.openInfo();
        exposed[1] = 8;
        assertArrayEquals(new byte[]{1, 2, 3}, options.openInfo());

        assertSame(OpenOptions.empty(), OpenOptions.empty());
        assertEquals(0, OpenOptions.empty().openInfoLength());
    }

    @Test
    void openOptionsUsesContentValueSemanticsForOpenInfo() {
        OpenOptions left = new OpenOptions(7L, 9L, new byte[]{1, 2, 3});
        OpenOptions right = new OpenOptions(7L, 9L, new byte[]{1, 2, 3});
        OpenOptions different = new OpenOptions(7L, 9L, new byte[]{1, 2, 4});

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
        assertNotEquals(left, different);
        assertEquals("OpenOptions[initialPriority=7, initialGroup=9, openInfoLength=3]", left.toString());
    }

    @Test
    void openOptionsFactoriesAndBuilderMatchConstructorSemantics() {
        OpenOptions built = OpenOptions.builder()
                .priority(7L)
                .group(9L)
                .openInfo("abc")
                .build();

        assertEquals(new OpenOptions(7L, 9L, new byte[]{'a', 'b', 'c'}), built);
        assertEquals(new OpenOptions(null, null, new byte[]{'x'}), OpenOptions.withOpenInfo(new byte[]{'x'}));
        assertEquals(new OpenOptions(null, null, new byte[]{'y'}), OpenOptions.withOpenInfo("y"));
        assertEquals(new OpenOptions(7L, null, null), OpenOptions.priority(7L));
        assertEquals(new OpenOptions(null, 9L, null), OpenOptions.group(9L));
        assertSame(OpenOptions.empty(), OpenOptions.of(null, null, null));
        assertSame(OpenOptions.empty(), OpenOptions.builder().build());
        assertTrue(built.hasInitialPriority());
        assertTrue(built.hasInitialGroup());
        assertTrue(built.hasOpenInfo());
        assertFalse(built.isEmpty());
        assertFalse(OpenOptions.empty().hasInitialPriority());
        assertFalse(OpenOptions.empty().hasInitialGroup());
        assertFalse(OpenOptions.empty().hasOpenInfo());
        assertTrue(OpenOptions.empty().isEmpty());
    }

    @Test
    void metadataUpdateFactoriesAndBuilderMatchConstructorSemantics() {
        MetadataUpdate built = MetadataUpdate.builder()
                .priority(3L)
                .group(5L)
                .build();

        assertEquals(new MetadataUpdate(3L, 5L), built);
        assertEquals(new MetadataUpdate(3L, null), MetadataUpdate.priority(3L));
        assertEquals(new MetadataUpdate(null, 5L), MetadataUpdate.group(5L));
        assertEquals(new MetadataUpdate(3L, 5L), MetadataUpdate.of(3L, 5L));
        assertTrue(built.hasPriority());
        assertTrue(built.hasGroup());
        assertFalse(built.isEmpty());
        assertTrue(MetadataUpdate.of(null, null).empty());
        assertTrue(MetadataUpdate.of(null, null).isEmpty());
        assertFalse(MetadataUpdate.of(null, null).hasPriority());
        assertFalse(MetadataUpdate.of(null, null).hasGroup());
    }

    @Test
    void streamMetadataRemainsDefensiveWhileReusingEmptySingleton() {
        byte[] openInfo = new byte[]{4, 5, 6};
        StreamMetadata metadata = new StreamMetadata(3L, 11L, openInfo);
        openInfo[0] = 0;

        assertArrayEquals(new byte[]{4, 5, 6}, metadata.openInfo());

        byte[] exposed = metadata.openInfo();
        exposed[2] = 7;
        assertArrayEquals(new byte[]{4, 5, 6}, metadata.openInfo());

        assertSame(StreamMetadata.empty(), StreamMetadata.empty());
        assertEquals(0, StreamMetadata.empty().openInfoLength());
    }

    @Test
    void streamMetadataUsesContentValueSemanticsForOpenInfo() {
        StreamMetadata left = new StreamMetadata(3L, 11L, new byte[]{4, 5, 6});
        StreamMetadata right = new StreamMetadata(3L, 11L, new byte[]{4, 5, 6});
        StreamMetadata different = new StreamMetadata(3L, 11L, new byte[]{4, 5, 7});

        assertEquals(left, right);
        assertEquals(left.hashCode(), right.hashCode());
        assertNotEquals(left, different);
        assertEquals("StreamMetadata[priority=3, group=11, openInfoLength=3]", left.toString());
    }

    @Test
    void streamMetadataFactoriesAndInfoHelpersStayConsistent() {
        StreamMetadata metadata = StreamMetadata.of(3L, 11L, new byte[]{4, 5, 6});

        assertEquals(new StreamMetadata(3L, 11L, new byte[]{4, 5, 6}), metadata);
        assertEquals(new StreamMetadata(0L, null, new byte[]{7}), StreamMetadata.withOpenInfo(new byte[]{7}));
        assertSame(StreamMetadata.empty(), StreamMetadata.of(0L, null, null));
        assertTrue(metadata.hasGroup());
        assertTrue(metadata.hasOpenInfo());
        assertFalse(metadata.isEmpty());
        assertFalse(StreamMetadata.empty().hasGroup());
        assertFalse(StreamMetadata.empty().hasOpenInfo());
        assertTrue(StreamMetadata.empty().isEmpty());
    }

    @Test
    void streamInfoDefaultHelpersReuseMetadataView() {
        ZmuxStreamInfo info = new ZmuxStreamInfo() {
            @Override
            public long streamId() {
                return 7L;
            }

            @Override
            public byte[] openInfo() {
                return new byte[]{1, 2, 3};
            }

            @Override
            public StreamMetadata metadata() {
                return StreamMetadata.of(3L, 11L, new byte[]{1, 2, 3});
            }

            @Override
            public java.net.SocketAddress localAddress() {
                return new InetSocketAddress(1000);
            }

            @Override
            public java.net.SocketAddress remoteAddress() {
                return new InetSocketAddress(2000);
            }
        };

        assertEquals(3, info.openInfoLength());
        assertTrue(info.hasOpenInfo());
    }

    @Test
    void settingsDefaultsReuseSingletonTemplate() {
        assertSame(Settings.defaults(), Settings.defaults());
        assertEquals(Settings.defaults(), Settings.builder().build());
    }

    @Test
    void metadataValueTypesRejectValuesOutsideVarint62Range() {
        long tooLarge = Protocol.MAX_VARINT62 + 1L;

        assertThrows(IllegalArgumentException.class, () -> new OpenOptions(-1L, null, null));
        assertThrows(IllegalArgumentException.class, () -> new OpenOptions(null, tooLarge, null));
        assertThrows(IllegalArgumentException.class, () -> new MetadataUpdate(-1L, null));
        assertThrows(IllegalArgumentException.class, () -> new MetadataUpdate(null, tooLarge));
        assertThrows(IllegalArgumentException.class, () -> new StreamMetadata(-1L, null, null));
        assertThrows(IllegalArgumentException.class, () -> new StreamMetadata(0L, tooLarge, null));
        assertThrows(IllegalArgumentException.class, () -> new ApplicationError(-1L, ""));
        assertThrows(IllegalArgumentException.class, () -> new ApplicationError(tooLarge, ""));
    }
}
