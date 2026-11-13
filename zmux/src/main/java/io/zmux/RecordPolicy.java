package io.zmux;

/** Chooses the record mode of a carrier the local endpoint joins ({@link ZmuxConfig.Builder#recordPolicy}). */
@FunctionalInterface
public interface RecordPolicy {
    RecordMode choose(RecordPolicyInput carrier);
}
