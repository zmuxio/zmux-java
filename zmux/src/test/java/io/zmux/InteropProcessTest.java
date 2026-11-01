package io.zmux;

import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class InteropProcessTest {
    private static InteropProcess child(String mode) throws IOException {
        return new InteropProcess(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), Child.class.getName(), mode);
    }
    @Test void rejectsUnterminatedOversizedLine() throws Exception {
        try (var process = child("oversize")) {
            var failure = assertThrows(IOException.class, () -> process.expect("DONE"));
            assertTrue(failure.getMessage().contains("output failed"));
        }
    }
    @Test void interruptedCloseStillReapsChild() throws Exception {
        var process = child("wait");
        long pid = Long.parseLong(process.expect("PID "));
        Thread.currentThread().interrupt();
        try {
            process.close();
            assertTrue(Thread.currentThread().isInterrupted(), "interrupt must be restored");
            assertFalse(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false));
        } finally { Thread.interrupted(); process.close(); }
    }
    public static final class Child {
        public static void main(String[] args) throws Exception {
            if (args[0].equals("oversize")) {
                for (int i = 0; i < 128; i++) System.out.print("x".repeat(1024));
                System.out.flush();
            } else System.out.println("PID " + ProcessHandle.current().pid());
            TimeUnit.SECONDS.sleep(60);
        }
    }
}
