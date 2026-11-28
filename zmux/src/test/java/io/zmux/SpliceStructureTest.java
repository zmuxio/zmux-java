package io.zmux;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * Guards the SpotBugs exclusion of {@code Splice$Direction}: every plain mutable field of a direction belongs
 * to the splice's serial context, so a new one cannot appear without that decision (and its review).
 */
@org.junit.jupiter.api.Timeout(60)
class SpliceStructureTest {
    /** The direction's plain fields, all read and written only on the splice's serial context. */
    private static final Set<String> SERIAL = Set.of(
            "closeRead", "closeWrite", "settings", "readStage", "endStage", "writeKind", "read", "write", "terminal", "dgReceive",
            "grant", "inputPermit", "memberPermit", "writePermit", "memberCharge", "memberChargeOffset", "writeCharge",
            "direct", "batch", "batchSettings", "directEnd", "active",
            "dgActive", "dgSend", "collected", "compacted", "readCopy", "readingSize", "inputHooks", "inputAt", "inputStart", "inputGeneration",
            "inputSequence", "sequence", "fifoOrder", "inputOrder", "resetCode", "protectedBytes", "targetBytes", "bytes", "datagramBytes",
            "asCapsules", "timerAt", "stopTimer", "fin", "rawHeader", "streaming", "writingAtTarget",
            "dgClosed", "dgInputClosed", "sourcePosition", "datagramLength", "end", "callbackFailure",
            "writeLength", "outputGrant", "compactedPayload", "discardingCapsule", "dgRoute", "dgRouting", "capacity", "packetPending");

    @Test void everyPlainFieldOfADirectionIsSerialContextState() throws Exception {
        Set<String> plain = new TreeSet<>();
        for (Field f : Class.forName("io.zmux.Splice$Direction").getDeclaredFields()) {
            int m = f.getModifiers();
            if (!f.isSynthetic() && !Modifier.isStatic(m) && !Modifier.isFinal(m) && !Modifier.isVolatile(m)) plain.add(f.getName());
        }
        assertEquals(new TreeSet<>(SERIAL), plain, "a new plain field of Splice.Direction needs an owner decision");
    }
}
