package io.zmux.shaping.machine;

import java.io.Serial;

/**
 * A definition or a configuration bundle that does not load, an argument error;
 * {@link #category()} names the kind of error.
 */
public final class DefinitionException extends IllegalArgumentException {
    @Serial
    private static final long serialVersionUID = 1L;

    /** The kinds of error. */
    public enum Category {
        SYNTAX, DUPLICATE_KEY, UNKNOWN_MEMBER, TYPE, RANGE, TABLE, SCOPE, CONFLICT, REFERENCE, UNREACHABLE, CYCLE,
        ZERO_DELAY_CYCLE, LIMIT, NO_DEFAULT, NO_CANDIDATE, SCHEMA_VERSION;

        /** The lower case name, as in {@code zero_delay_cycle}. */
        public String id() { return this == SCHEMA_VERSION ? "schema" : name().toLowerCase(java.util.Locale.ROOT); }
    }

    private final transient Category category;

    public DefinitionException(Category category, String message) {
        super(category.id() + ": " + message);
        this.category = category;
    }

    public Category category() { return category; }
}
