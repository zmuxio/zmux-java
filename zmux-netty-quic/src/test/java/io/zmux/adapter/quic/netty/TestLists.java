package io.zmux.adapter.quic.netty;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

final class TestLists {
    private TestLists() {
    }

    @SafeVarargs
    static <T> List<T> listOf(T... values) {
        return values.length == 0 ? Collections.emptyList() : Arrays.asList(values);
    }
}
