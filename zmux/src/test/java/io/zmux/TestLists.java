package io.zmux;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class TestLists {
    private TestLists() {
    }

    @SafeVarargs
    public static <T> List<T> listOf(T... values) {
        return values.length == 0 ? Collections.emptyList() : Arrays.asList(values);
    }
}
