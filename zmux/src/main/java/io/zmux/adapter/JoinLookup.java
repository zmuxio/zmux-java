package io.zmux.adapter;

/**
 * The answer of {@link ListenerAdapter#knownJoin}: whether the Locator is known, and the total
 * prefix length still needed (0: none).
 */
public record JoinLookup(boolean known, int need) {}
