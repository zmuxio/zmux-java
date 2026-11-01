package io.zmux.internal.loop;

import io.zmux.internal.core.PathScheduler;
import io.zmux.internal.core.Session;
import io.zmux.internal.core.Stream;
import java.util.ArrayList;
import java.util.List;

/**
 * Hands the core's pending frame placements to the session's and streams' custom
 * schedulers and returns their answers; on the session's loop only.
 */
public final class PickerBridge {
    private final Session core;

    PickerBridge(Session core, Picker picker) {
        this.core = core;
        this.picker = picker;
    }

    /**
     * A value-only policy callback, run synchronously on the loop. It must be fast and must
     * not block; blocking session calls fail there at once. The lists are read-only views
     * valid only during the call.
     */
    @FunctionalInterface
    public interface Picker {
        List<PathScheduler.PickAnswer> pick(List<PathScheduler.PickFrame> requests, List<PathScheduler.PickPipe> pipes);
    }

    private Picker picker;
    private final java.util.IdentityHashMap<Stream, Picker> streamPickers = new java.util.IdentityHashMap<>();
    private final PathScheduler.PickRequest pickRequest = new PathScheduler.PickRequest();

    /** Installs one policy and callback together on the owning loop. Null stream means the session. */
    public void install(Stream stream, io.zmux.internal.core.CoreConfig.Policy policy, Picker custom) {
        if (stream == null) { picker = custom; core.setPolicy(policy); }
        else {
            if (custom == null) streamPickers.remove(stream); else streamPickers.put(stream, custom);
            core.setStreamPolicy(stream, policy);
        }
    }

    public void forget(Stream stream) { streamPickers.remove(stream); }

    private static final PathScheduler.PickAnswer MISS = new PathScheduler.PickAnswer(-1, 0, false);
    /** Reused per round: the decision buffers allocate nothing in the steady state. */
    private final ArrayList<PathScheduler.PickAnswer> pickAnswers = new ArrayList<>();
    private final List<PathScheduler.PickPipe> pickPipes = java.util.Collections.unmodifiableList(pickRequest.pipes);
    private final List<PathScheduler.PickFrame> pickFrames = java.util.Collections.unmodifiableList(pickRequest.frames);
    private final ArrayList<Picker> pickGroups = new ArrayList<>(2);
    private final ArrayList<PathScheduler.PickFrame> groupFrames = new ArrayList<>();
    private final List<PathScheduler.PickFrame> groupView = java.util.Collections.unmodifiableList(groupFrames);
    private int[] groupIndex = new int[8];

    /**
     * Runs the custom policies synchronously on the loop (IMPLEMENTATION.md Section 2.5.2):
     * one call per distinct picker with the frames it owns, over the reused value view.
     */
    boolean pick() {
        if (!core.wantsPick() || !core.takePick(pickRequest)) return false;
        int n = pickRequest.frames.size();
        pickAnswers.clear();
        for (int i = 0; i < n; i++) pickAnswers.add(MISS);
        if (streamPickers.isEmpty()) {
            if (picker != null) ask(picker, pickFrames, null, n);
        } else {
            pickGroups.clear();
            for (int i = 0; i < n; i++) {
                Picker p = streamPickers.getOrDefault(pickRequest.stream(i), picker);
                if (p != null && !containsIdentity(pickGroups, p)) pickGroups.add(p);
            }
            for (Picker group : pickGroups) {
                groupFrames.clear();
                if (groupIndex.length < n) groupIndex = new int[Math.max(n, groupIndex.length * 2)];
                for (int i = 0; i < n; i++) {
                    if (streamPickers.getOrDefault(pickRequest.stream(i), picker) != group) continue;
                    groupIndex[groupFrames.size()] = i;
                    groupFrames.add(pickRequest.frames.get(i));
                }
                ask(group, groupView, groupIndex, groupFrames.size());
            }
            groupFrames.clear();
            pickGroups.clear();
        }
        core.answerPick(pickRequest, pickAnswers, false);
        pickAnswers.clear();
        pickRequest.clear();
        return true;
    }

    private void ask(Picker p, List<PathScheduler.PickFrame> frames, int[] index, int count) {
        try {
            List<PathScheduler.PickAnswer> result = p.pick(frames, pickPipes);
            if (result == null) return;
            for (int k = 0, m = Math.min(result.size(), count); k < m; k++) {
                PathScheduler.PickAnswer a = result.get(k);
                if (a != null && a.bytes() >= 0) pickAnswers.set(index == null ? k : index[k], a);
            }
        } catch (RuntimeException | AssertionError ignored) {
            // Invalid or failed callbacks use the core's affinity fallback.
        }
    }

    private static boolean containsIdentity(List<Picker> list, Picker p) {
        for (Picker q : list) if (q == p) return true;
        return false;
    }

    void clear() {
        streamPickers.clear();
        picker = null;
        pickRequest.clear();
    }
}
