package io.zmux.runtime;

final class OrdinaryBatchCandidateComparator {
    private OrdinaryBatchCandidateComparator() {
    }

    static boolean betterGroupCandidate(OrdinaryBatchOrderer.GroupKey preferredGroupHead,
                                        OrdinaryBatchOrderer.GroupCandidate left,
                                        OrdinaryBatchOrderer.GroupCandidate right) {
        if (left == null) {
            return false;
        }
        if (right == null) {
            return true;
        }
        if (left.eligible() != right.eligible()) {
            return left.eligible();
        }
        if (left.eligible()) {
            if (left.groupFinish() != right.groupFinish()) {
                return left.groupFinish() < right.groupFinish();
            }
            if (left.groupStart() != right.groupStart()) {
                return left.groupStart() < right.groupStart();
            }
        } else {
            if (left.groupStart() != right.groupStart()) {
                return left.groupStart() < right.groupStart();
            }
            if (left.groupFinish() != right.groupFinish()) {
                return left.groupFinish() < right.groupFinish();
            }
        }
        boolean leftPreferred = prefersGroup(preferredGroupHead, left.group().key());
        boolean rightPreferred = prefersGroup(preferredGroupHead, right.group().key());
        if (leftPreferred != rightPreferred) {
            return leftPreferred;
        }
        if (left.streamFinish() != right.streamFinish()) {
            return left.streamFinish() < right.streamFinish();
        }
        if (left.streamStart() != right.streamStart()) {
            return left.streamStart() < right.streamStart();
        }
        if (left.groupLastServed() != right.groupLastServed()) {
            return left.groupLastServed() < right.groupLastServed();
        }
        if (left.streamLastServed() != right.streamLastServed()) {
            return left.streamLastServed() < right.streamLastServed();
        }
        if (left.group().order() != right.group().order()) {
            return left.group().order() < right.group().order();
        }
        return left.streamState().order() < right.streamState().order();
    }

    private static boolean prefersGroup(OrdinaryBatchOrderer.GroupKey preferredGroupHead,
                                        OrdinaryBatchOrderer.GroupKey groupKey) {
        return preferredGroupHead != null && preferredGroupHead.equals(groupKey);
    }
}
