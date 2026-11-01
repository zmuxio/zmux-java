package io.zmux.internal.core;

import java.lang.reflect.Field;

/** Test-only access to component-private state, for fixtures that set up a state without driving the protocol to it. */
final class Poke {
    private Poke() {}

    static void set(Object target, String name, Object value) {
        try {
            Field f = target.getClass().getDeclaredField(name);
            f.setAccessible(true);
            f.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
}
