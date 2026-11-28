package io.zmux.internal.core;

/** Links are paid with their record. There is no retained table or array after unlinking. */
final class OwnerQueue<T> {
    private Link<T> first, last;

    boolean isEmpty() { return first == null; }

    Link<T> first() { return first; }

    void add(Link<T> link) {
        if (link.owner == this) return;
        if (link.owner != null) throw new IllegalStateException("link already queued");
        link.owner = this;
        link.previous = last;
        if (last == null) first = link;
        else last.next = link;
        last = link;
    }

    static final class Link<T> {
        private final T value;
        private OwnerQueue<T> owner;
        private Link<T> previous, next;

        Link(T value) { this.value = value; }

        T value() { return value; }

        Link<T> next() { return next; }

        void remove() {
            if (owner == null) return;
            if (previous == null) owner.first = next;
            else previous.next = next;
            if (next == null) owner.last = previous;
            else next.previous = previous;
            previous = next = null;
            owner = null;
        }
    }
}
