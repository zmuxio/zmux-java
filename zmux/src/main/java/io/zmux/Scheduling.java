package io.zmux;

import io.zmux.internal.core.CoreConfig;
import io.zmux.internal.core.PathScheduler;
import io.zmux.internal.loop.PickerBridge;
import java.time.Duration;
import java.util.List;

/** Maps values at the public boundary; the core owns every scheduling decision. */
final class Scheduling {
    private Scheduling() {}
    static CoreConfig.Policy policy(Scheduler s) {
        return s instanceof Scheduler.Builtin b ? CoreConfig.Policy.valueOf(b.name()) : CoreConfig.Policy.CUSTOM;
    }
    static PickerBridge.Picker picker(Scheduler s) {
        if (!(s instanceof Scheduler.Custom(Scheduler.Picker custom))) return null;
        return (requests, pipes) -> {
            int n = requests.size();
            Scheduler.Request[] req = new Scheduler.Request[n];
            for (int i = 0; i < n; i++) {
                PathScheduler.PickFrame f = requests.get(i);
                req[i] = new Scheduler.Request(f.stream(), f.len(), f.eligibleTier(), f.hasTier(), f.lastPipe(),
                        f.hasLastPipe(), f.urgency());
            }
            Scheduler.Pipe[] view = new Scheduler.Pipe[pipes.size()];
            for (int i = 0; i < view.length; i++) {
                PathScheduler.PickPipe p = pipes.get(i);
                view[i] = new Scheduler.Pipe(p.pipe(), p.state().toPublic(), Duration.ofNanos(p.srtt()), Duration.ofNanos(p.rttvar()),
                        p.rate(), p.unsent(), p.inFlight(), p.priority(), p.weight(), p.writable(), p.standby(),
                        p.saturated(), p.probation(), p.eligible(), p.shaped(), p.shapingWeight());
            }
            List<Scheduler.Answer> answers = custom.pick(List.of(req), List.of(view));
            if (answers == null) return List.of();
            PathScheduler.PickAnswer[] mapped = new PathScheduler.PickAnswer[Math.min(answers.size(), n)];
            for (int i = 0; i < mapped.length; i++) {
                Scheduler.Answer a = answers.get(i);
                mapped[i] = a == null ? null : new PathScheduler.PickAnswer(a.pipeIndex(), a.bytes(), a.waitForInput());
            }
            return java.util.Arrays.asList(mapped);
        };
    }
}
