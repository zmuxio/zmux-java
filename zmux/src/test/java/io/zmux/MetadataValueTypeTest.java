package io.zmux;

import org.junit.jupiter.api.Test;

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
        assertSame(OpenOptions.empty(), OpenOptions.of(null, null, null));
        assertSame(OpenOptions.empty(), OpenOptions.builder().build());
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
        assertTrue(MetadataUpdate.of(null, null).empty());
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
