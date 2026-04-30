# zmux-java

Java implementation of the ZMux stream multiplexing protocol.

Released artifacts are Java 8+ compatible. Public APIs live under `io.zmux`
and `io.zmux.adapter.quic.netty`; `io.zmux.internal` is implementation detail.

## Artifacts

- `io.github.zmuxio:zmux`: core ZMux APIs and native framing transport.
- `io.github.zmuxio:zmux-netty-quic`: optional adapter for existing Netty QUIC
  `QuicChannel` transports.

Replace placeholders in the snippets below:

- `VERSION`: zmux-java version.
- `NETTY_VERSION`: Netty QUIC version compatible with `zmux-netty-quic`.
- `OS_CLASSIFIER`: Netty native runtime classifier, such as `windows-x86_64`,
  `linux-x86_64`, `linux-aarch_64`, `osx-x86_64`, or `osx-aarch_64`.

## Installation

### Gradle

`build.gradle.kts`:

```kts
repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.zmuxio:zmux:VERSION")
}
```

`build.gradle`:

```groovy
repositories {
    mavenCentral()
}

dependencies {
    implementation 'io.github.zmuxio:zmux:VERSION'
}
```

Optional Netty QUIC adapter:

`build.gradle.kts`:

```kts
dependencies {
    implementation("io.github.zmuxio:zmux-netty-quic:VERSION")
}
```

`build.gradle`:

```groovy
dependencies {
    implementation 'io.github.zmuxio:zmux-netty-quic:VERSION'
}
```

If your application does not already provide Netty QUIC native runtime
artifacts, add the platform native runtime too:

`build.gradle.kts`:

```kts
dependencies {
    runtimeOnly("io.netty:netty-codec-native-quic:NETTY_VERSION:OS_CLASSIFIER")
}
```

`build.gradle`:

```groovy
dependencies {
    runtimeOnly 'io.netty:netty-codec-native-quic:NETTY_VERSION:OS_CLASSIFIER'
}
```

### Maven

```xml
<dependency>
    <groupId>io.github.zmuxio</groupId>
    <artifactId>zmux</artifactId>
    <version>VERSION</version>
</dependency>
```

Optional Netty QUIC adapter:

```xml
<dependency>
    <groupId>io.github.zmuxio</groupId>
    <artifactId>zmux-netty-quic</artifactId>
    <version>VERSION</version>
</dependency>
```

If your application does not already provide Netty QUIC native runtime
artifacts, add the platform native dependency:

```xml
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-codec-native-quic</artifactId>
    <version>NETTY_VERSION</version>
    <classifier>OS_CLASSIFIER</classifier>
    <scope>runtime</scope>
</dependency>
```

## Quick Start

The examples assume the surrounding method declares `throws Exception`.

### Client

```java
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;

import java.net.Socket;
import java.nio.charset.StandardCharsets;

try (Socket socket = new Socket("127.0.0.1", 9000);
     ZmuxSession session = Zmux.clientSession(socket);
     ZmuxStream stream = session.openStream()) {
    stream.writeFinal("hello".getBytes(StandardCharsets.UTF_8));
    String reply = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
}
```

### Server

```java
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

try (ServerSocket listener = new ServerSocket(9000);
     Socket socket = listener.accept();
     ZmuxSession session = Zmux.serverSession(socket);
     ZmuxStream stream = session.acceptStream()) {
    String request = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    stream.writeFinal(("echo:" + request).getBytes(StandardCharsets.UTF_8));
}
```

## Core Usage

### Sessions

Use `ZmuxSession` for common application code. Native sessions and the Netty
QUIC adapter both expose the same session and stream interfaces:

```java
void handle(ZmuxSession session) throws Exception {
    try (ZmuxStream stream = session.acceptStream()) {
        byte[] request = stream.readAllBytes();
        stream.writeFinal(request);
    }
}
```

Use `ZmuxNativeSession` only when you need native-framing features such as
explicit ping, GOAWAY, local/peer prefaces, negotiated settings, or peer close
details.

### Bidirectional Streams

```java
try (ZmuxStream stream = session.openStream()) {
    stream.write("request".getBytes(StandardCharsets.UTF_8));
    stream.closeWrite();

    byte[] response = stream.readAllBytes();
}
```

`writeFinal(...)` writes bytes and gracefully closes the local send half:

```java
stream.writeFinal(payload);
```

### Unidirectional Streams

```java
try (ZmuxSendStream send = session.openUniStream()) {
    send.writeFinal("event".getBytes(StandardCharsets.UTF_8));
}

try (ZmuxRecvStream recv = session.acceptUniStream()) {
    byte[] event = recv.readAllBytes();
}
```

### Existing Buffers

If data already lives in a slice or `ByteBuffer`, send it without copying into
a separate array first:

```java
byte[] frame = ...;
ZmuxStream bidi = session.openAndSend(frame, 4, 128); // stream stays open
ZmuxSendStream uni = session.openUniAndSend(ByteBuffer.wrap(frame, 132, 64));
```

`openAndSend(...)` seeds a bidirectional stream and leaves it open for later
writes. `openUniAndSend(...)` writes the final contents of a unidirectional
stream.

### Metadata And Priority

Both peers must enable the capabilities they use:

```java
import io.zmux.MetadataUpdate;
import io.zmux.OpenOptions;
import io.zmux.Protocol;
import io.zmux.ZmuxConfig;

long capabilities = Protocol.CAPABILITY_OPEN_METADATA
        | Protocol.CAPABILITY_PRIORITY_HINTS
        | Protocol.CAPABILITY_STREAM_GROUPS
        | Protocol.CAPABILITY_PRIORITY_UPDATE;

ZmuxConfig config = ZmuxConfig.builder()
        .capabilities(capabilities)
        .build();

OpenOptions options = OpenOptions.builder()
        .priority(7L)
        .group(2L)
        .openInfo("rpc")
        .build();

try (ZmuxStream stream = session.openStream(options)) {
    stream.updateMetadata(MetadataUpdate.of(3L, 2L));
    stream.writeFinal("hello".getBytes(StandardCharsets.UTF_8));
}
```

The accepting peer can read `stream.openInfo()` and `stream.metadata()`.

### Timeouts And Deadlines

```java
import java.time.Duration;

ZmuxStream stream = session.openStreamWithTimeout(Duration.ofSeconds(2));
stream.setReadTimeout(Duration.ofSeconds(5));
stream.setWriteTimeout(Duration.ofSeconds(5));
stream.clearReadDeadline();
stream.clearWriteDeadline();
```

### Closing And Errors

```java
stream.closeWrite();                  // graceful write-half close
stream.closeRead();                   // local read cancellation
stream.close();                       // closes ordinary local use of both halves
stream.closeWithError(0x100L, "bye"); // stream application error

session.close();                      // graceful session close
session.closeWithError(0x100L, "bye");
session.awaitTerminationOrThrow(Duration.ofSeconds(5));
```

```java
import io.zmux.ApplicationError;
import io.zmux.ErrorCode;
import io.zmux.ZmuxErrors;

try {
    stream.write(payload);
} catch (IOException error) {
    if (ZmuxErrors.sessionClosed(error)) {
        // session is already gone
    }
    if (ZmuxErrors.writeClosed(error)) {
        // write side is no longer available
    }

    ApplicationError app = ZmuxErrors.applicationError(error);
    if (app != null && app.isCode(ErrorCode.PROTOCOL)) {
        long code = app.applicationCode();
        String reason = app.reason();
    }
}
```

## Custom Transports

Wrap custom transports as `DuplexConnection`:

```java
import io.zmux.DuplexConnection;
import io.zmux.ZmuxConnections;

DuplexConnection connection = ZmuxConnections.builder(input, output)
        .closer(transport)
        .gatheringOutput(gatheringOutput)
        .addresses(localAddress, remoteAddress)
        .build();

ZmuxSession session = Zmux.clientSession(connection);
```

If the transport can enforce blocking read or write deadlines, implement
`supportsReadDeadline()` / `setReadDeadline(...)` and
`supportsWriteDeadline()` / `setWriteDeadline(...)`.

You can also join directional halves:

```java
JoinedDuplexConnection connection = Zmux.join(inbound, outbound);
ZmuxSession nested = Zmux.clientSession(connection);
```

The joined connection exposes `InputStream` / `OutputStream`, directional
half-close methods, deadline helpers, and pause handles for replacing the
current read or write half.

## Netty QUIC Adapter

Use `zmux-netty-quic` only when your application already has an established
Netty `QuicChannel`. The adapter exposes the same `ZmuxSession`,
`ZmuxStream`, `ZmuxSendStream`, and `ZmuxRecvStream` interfaces as the native
transport.

```java
import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;
import io.zmux.adapter.quic.netty.NettyQuic;
import io.zmux.adapter.quic.netty.NettyQuicSessionOptions;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

QuicChannel channel = ...;
NettyQuicSessionOptions options = NettyQuicSessionOptions.defaults()
        .withAcceptedPreludeReadTimeout(Duration.ofSeconds(2))
        .withAcceptedPreludeMaxConcurrent(16);

try (ZmuxSession session = NettyQuic.wrapSessionWithOptions(channel, options);
     ZmuxStream stream = session.openStream()) {
    stream.writeFinal("hello".getBytes(StandardCharsets.UTF_8));
}
```

Call blocking zmux APIs from application or worker threads, not from the Netty
event loop. The adapter preserves numeric application error codes where Netty
QUIC exposes them. It does not model datagrams, packet acknowledgements, or
transport RTT/loss state.

## Public API Overview

- `Zmux`: opens client, server, or auto-role sessions over sockets, streams,
  NIO channels, or `DuplexConnection`.
- `ZmuxSession`: opens and accepts bidirectional and unidirectional streams,
  reports state/stats, closes sessions, and waits for termination.
- `ZmuxStream`, `ZmuxSendStream`, `ZmuxRecvStream`: bidirectional, send-only,
  and receive-only stream APIs.
- `OpenOptions`, `MetadataUpdate`, `StreamMetadata`: open-time and runtime
  stream metadata.
- `ZmuxConfig`, `Settings`, `Limits`: session configuration and negotiated
  limits.
- `ZmuxEventHandler`, `ZmuxEvent`, `SessionStats`: events and diagnostics.
- `ZmuxException` and `ZmuxErrors`: structured error metadata and helpers.
- `ZmuxCodec` plus protocol value types: public codec helpers for tests,
  proxies, diagnostics, and conformance tools.

## Semantics Notes

- A successful `write(...)`, `writeFinal(...)`, or `writevFinal(...)` means the
  bytes entered the local zmux send path; it is not a peer application
  acknowledgement.
- `closeWrite()` gracefully finishes only the local send half.
- `closeRead()` stops local interest in inbound bytes and sends cancellation.
- `closeWithError(code, reason)` carries a numeric application error and
  optional diagnostic text.
- `openInfo()` is peer-visible only when `OPEN_METADATA` is negotiated;
  requests that require unavailable open metadata fail instead of silently
  discarding it.
