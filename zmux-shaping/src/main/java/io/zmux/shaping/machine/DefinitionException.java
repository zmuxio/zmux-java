package io.zmux.shaping.machine;

/** A definition that cannot be read or does not validate. */
public final class DefinitionException extends Exception {
    public DefinitionException(String message) { super(message); }

    DefinitionException(String context, DefinitionException cause) {
        super(context + ": " + cause.getMessage(), cause);
    }
}
