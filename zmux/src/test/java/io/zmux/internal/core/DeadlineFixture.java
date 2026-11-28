package io.zmux.internal.core;

final class DeadlineFixture {
    private DeadlineFixture() {}

    static long next(Session session) {
        session.refreshDeadlines();
        return session.nextTimeout();
    }
}
