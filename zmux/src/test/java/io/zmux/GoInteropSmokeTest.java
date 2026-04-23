package io.zmux;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Comparator;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

final class GoInteropSmokeTest {
    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(20);

    private static void runJavaClient(String address) throws Exception {
        String[] hostPort = address.split(":");
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);
        long capabilities = Protocol.CAPABILITY_OPEN_METADATA
                | Protocol.CAPABILITY_PRIORITY_UPDATE
                | Protocol.CAPABILITY_PRIORITY_HINTS
                | Protocol.CAPABILITY_STREAM_GROUPS;
        ZmuxConfig config = ZmuxConfig.defaults()
                .toBuilder()
                .capabilities(capabilities)
                .build();
        try (Socket socket = new Socket(host, port);
             ZmuxNativeSession session = Zmux.client(new BasicDuplexConnection(
                     socket.getInputStream(),
                     socket.getOutputStream(),
                     socket,
                     socket.getLocalSocketAddress(),
                     socket.getRemoteSocketAddress()
             ), config)) {
            OpenOptions options = new OpenOptions(
                    7L,
                    9L,
                    "java-open".getBytes(StandardCharsets.UTF_8)
            );
            ZmuxStream stream = session.openStream(options);
            stream.writeFinal("java->go".getBytes(StandardCharsets.UTF_8));

            ByteArrayOutputStream response = new ByteArrayOutputStream();
            byte[] buffer = new byte[64];
            int read;
            while ((read = stream.read(buffer)) >= 0) {
                response.write(buffer, 0, read);
            }
            assertEquals("go:java->go", response.toString(StandardCharsets.UTF_8.name()));
            session.close();
            session.awaitTerminationOrThrow(Duration.ofSeconds(5));
        }
    }

    private static String firstOutputLine(BufferedReader output) throws Exception {
        CompletableFuture<String> line = CompletableFuture.supplyAsync(() -> {
            try {
                return output.readLine();
            } catch (IOException error) {
                throw new RuntimeException(error);
            }
        });
        return line.get(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static boolean commandExists(String command) {
        String checker = System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win")
                ? "where"
                : "which";
        try {
            Process process = new ProcessBuilder(checker, command)
                    .redirectErrorStream(true)
                    .start();
            return process.waitFor(5, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static String goMod(Path goRoot) {
        return String.format(
                "module zmux_java_interop_smoke\n"
                        + "\n"
                        + "go 1.25\n"
                        + "\n"
                        + "require github.com/zmuxio/zmux-go v0.0.0\n"
                        + "\n"
                        + "replace github.com/zmuxio/zmux-go => %s\n",
                goRoot.toAbsolutePath().toString().replace('\\', '/')
        );
    }

    private static void deleteTree(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                deleteTreeOnce(root);
                return;
            } catch (IOException ignored) {
                if (!sleepBeforeCleanupRetry(attempt)) {
                    break;
                }
            }
        }
        markDeleteOnExit(root);
    }

    private static void deleteTreeOnce(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static boolean sleepBeforeCleanupRetry(int attempt) {
        try {
            TimeUnit.MILLISECONDS.sleep(50L * (attempt + 1L));
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void markDeleteOnExit(Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                path.toFile().deleteOnExit();
            }
        } catch (IOException ignored) {
            root.toFile().deleteOnExit();
        }
    }

    private static void terminateProcess(Process process) {
        try {
            if (process.waitFor(200, TimeUnit.MILLISECONDS)) {
                return;
            }
            process.destroy();
            if (process.waitFor(2, TimeUnit.SECONDS)) {
                return;
            }
            process.destroyForcibly();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static String goServerMain() {
        return String.join("\n",
                "package main",
                "",
                "import (",
                "    \"context\"",
                "    \"fmt\"",
                "    \"io\"",
                "    \"net\"",
                "    \"os\"",
                "    \"time\"",
                "",
                "    zmux \"github.com/zmuxio/zmux-go\"",
                ")",
                "",
                "func fatal(format string, args ...any) {",
                "    fmt.Fprintf(os.Stdout, \"ERR \"+format+\"\\n\", args...)",
                "    os.Exit(1)",
                "}",
                "",
                "func main() {",
                "    listener, err := net.Listen(\"tcp\", \"127.0.0.1:0\")",
                "    if err != nil {",
                "        fatal(\"listen: %v\", err)",
                "    }",
                "    defer listener.Close()",
                "    fmt.Fprintln(os.Stdout, \"ADDR \"+listener.Addr().String())",
                "",
                "    raw, err := listener.Accept()",
                "    if err != nil {",
                "        fatal(\"accept: %v\", err)",
                "    }",
                "    caps := zmux.CapabilityOpenMetadata | zmux.CapabilityPriorityUpdate | zmux.CapabilityPriorityHints | zmux.CapabilityStreamGroups",
                "    session, err := zmux.Server(raw, &zmux.Config{Capabilities: caps})",
                "    if err != nil {",
                "        fatal(\"server: %v\", err)",
                "    }",
                "    defer session.Close()",
                "",
                "    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)",
                "    defer cancel()",
                "    stream, err := session.AcceptStream(ctx)",
                "    if err != nil {",
                "        fatal(\"accept stream: %v\", err)",
                "    }",
                "    if got := string(stream.OpenInfo()); got != \"java-open\" {",
                "        fatal(\"open info = %q\", got)",
                "    }",
                "    meta := stream.Metadata()",
                "    if meta.Priority != 7 || meta.Group == nil || *meta.Group != 9 {",
                "        fatal(\"metadata = priority:%d group:%v\", meta.Priority, meta.Group)",
                "    }",
                "    payload, err := io.ReadAll(stream)",
                "    if err != nil {",
                "        fatal(\"read stream: %v\", err)",
                "    }",
                "    if _, err := stream.WriteFinal([]byte(\"go:\"+string(payload))); err != nil {",
                "        fatal(\"write final: %v\", err)",
                "    }",
                "    _ = stream.Close()",
                "    if err := session.Close(); err != nil {",
                "        fatal(\"close session: %v\", err)",
                "    }",
                "    waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)",
                "    defer waitCancel()",
                "    if err := session.Wait(waitCtx); err != nil {",
                "        fatal(\"wait: %v\", err)",
                "    }",
                "}",
                "");
    }

    @Test
    void javaClientTalksToGoServerWithOpenMetadata() throws Exception {
        assumeTrue("1".equals(System.getenv("ZMUX_INTEROP")), "set ZMUX_INTEROP=1 to run Java/Go interop smoke");
        String goRootEnv = System.getenv("ZMUX_GO_ROOT");
        assumeTrue(goRootEnv != null && !goRootEnv.trim().isEmpty(), "set ZMUX_GO_ROOT to the Go implementation root");
        Path goRoot = Paths.get(goRootEnv);
        assumeTrue(Files.isDirectory(goRoot), "Go implementation root not found: " + goRoot);
        assumeTrue(commandExists("go"), "go executable not found");

        Path work = Files.createTempDirectory("zmux-java-go-interop-");
        Files.write(work.resolve("go.mod"), goMod(goRoot).getBytes(StandardCharsets.UTF_8));
        Files.write(work.resolve("main.go"), goServerMain().getBytes(StandardCharsets.UTF_8));

        Process process = new ProcessBuilder("go", "run", ".")
                .directory(work.toFile())
                .redirectErrorStream(true)
                .start();
        try (BufferedReader output = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String address = firstOutputLine(output);
            assertTrue(address.startsWith("ADDR "), "unexpected go helper output: " + address);
            runJavaClient(address.substring("ADDR ".length()));
            assertTrue(process.waitFor(PROCESS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "go helper did not exit");
            String rest = output.lines().collect(Collectors.joining("\n"));
            assertEquals(0, process.exitValue(), rest);
        } finally {
            terminateProcess(process);
            deleteTree(work);
        }
    }
}
