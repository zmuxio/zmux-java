package io.zmux.shaping.machine;

import static org.junit.jupiter.api.Assertions.*;
import io.zmux.shaping.machine.Definition.Dist;
import io.zmux.shaping.machine.Definition.DistKind;
import io.zmux.shaping.machine.Definition.Value;
import java.util.List;
import java.util.random.RandomGeneratorFactory;
import org.junit.jupiter.api.Test;

class SamplerTest {
    @Test void aBetaOfShapesSoSmallThatBothGammasUnderflowIsInTheUnitInterval() {
        Sampler s = new Sampler(RandomGeneratorFactory.of("L64X128MixRandom").create(12)) {
            @Override double sample(Value v) { return draw(v); }
        };
        Value v = new Value(null, null, new Dist(DistKind.BETA, new double[] {1e-3, 1e-3}, null, null, List.of()), null, null, null);
        for (int i = 0; i < 10000; i++) {
            double x = s.draw(v);
            assertTrue(x >= 0 && x <= 1, "Beta(1e-3, 1e-3) drew " + x);
        }
    }
}
