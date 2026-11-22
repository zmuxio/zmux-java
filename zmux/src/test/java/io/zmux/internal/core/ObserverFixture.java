package io.zmux.internal.core;

/** Counts retained lifecycle observations on the owner loop for resource-regression tests. */
public final class ObserverFixture {
    private ObserverFixture() {}
    public static void wakeWaiters(Session session) { session.wakeAll(); }
    public static int closedWaiters(Session session) { return session.closedW.size(); }
}
