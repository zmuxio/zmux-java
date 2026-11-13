package io.zmux;

/** The acceptor's check of a Hello's record mode ({@link ZmuxConfig.Builder#allowRecordMode}); peer is the session's host value. */
@FunctionalInterface
public interface AllowRecordMode {
    boolean allow(RecordPolicyInput carrier, RecordMode mode, Object peer);
}
