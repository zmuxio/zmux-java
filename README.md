# zmux-java

Java implementation of the ZMux stream multiplexing protocol.

The published artifacts are Java 8+ compatible. Core session and stream APIs
start in `io.zmux`; transport helpers live in `io.zmux.transport`, and protocol
diagnostic helpers live in `io.zmux.protocol`.

## Installation

Gradle:

```kts
repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.zmuxio:zmux:VERSION")
}
```

Maven:

```xml
<dependency>
    <groupId>io.github.zmuxio</groupId>
    <artifactId>zmux</artifactId>
    <version>VERSION</version>
</dependency>
```

The Netty QUIC adapter is a separate optional module:

```kts
implementation("io.github.zmuxio:zmux-netty-quic:VERSION")
```

```xml
<dependency>
    <groupId>io.github.zmuxio</groupId>
    <artifactId>zmux-netty-quic</artifactId>
    <version>VERSION</version>
</dependency>
```

Adapter usage and Netty runtime notes are in
[`zmux-netty-quic/README.md`](zmux-netty-quic/README.md).

## Start A Session

Use `ZmuxSession` for application code. Native ZMux sessions and adapters both
implement this stable surface.

```java
import io.zmux.Zmux;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;

import java.net.Socket;
import java.nio.charset.StandardCharsets;

try (Socket socket = new Socket("127.0.0.1", 9000);
     ZmuxSession session = Zmux.openSession(socket);
     ZmuxStream stream = session.openStream()) {
    stream.writeFinal("hello".getBytes(StandardCharsets.UTF_8));
    byte[] reply = stream.readAllBytes();
}
```

Constructor choice:

- `Zmux.openSession(...)`: both peers may use the same call on an already
  established reliable connection; ZMux negotiates the wire role.
- `Zmux.clientSession(...)` / `Zmux.serverSession(...)`: use these when an outer
  protocol already defines initiator and responder.
- `Zmux.open(...)`, `Zmux.client(...)`, `Zmux.server(...)`: native-only return
  type (`ZmuxNativeSession`) for ping, go-away, preface, and negotiated native
  details.

All constructor families accept `Socket`, `InputStream` plus `OutputStream`,
`ByteChannel`, `ReadableByteChannel` plus `WritableByteChannel`, or
`io.zmux.transport.DuplexConnection`, with `ZmuxConfig` overloads.

## Streams

Bidirectional stream:

```java
try (ZmuxStream stream = session.openStream()) {
    stream.write("request".getBytes(StandardCharsets.UTF_8));
    stream.closeWrite();

    byte[] response = stream.readAllBytes();
}
```

Unidirectional stream:

```java
session.openUniAndSend("event".getBytes(StandardCharsets.UTF_8));

try (ZmuxRecvStream recv = session.acceptUniStream()) {
    byte[] event = recv.readAllBytes();
}
```

Useful stream methods:

- open: `openStream(...)`, `openUniStream(...)`, `acceptStream(...)`,
  `acceptUniStream(...)`
- open and write: `openAndSend(...)`, `openUniAndSend(...)`
- write: `write(...)`, `writeFinal(...)`, `writevFinal(...)`
- read: `read(...)`, `readAllBytes()`, `readAllBytes(maxBytes)`
- Java IO views: `asInputStream()`, `asOutputStream()`
- deadlines: `setReadTimeout(...)`, `setWriteTimeout(...)`,
  `clearReadDeadline()`, `clearWriteDeadline()`
- close/error: `closeRead()`, `closeWrite()`, `cancelRead(...)`,
  `cancelWrite(...)`, `closeWithError(...)`

`openAndSend(...)` leaves a bidirectional stream open for reading and further
writes. `openUniAndSend(...)` writes the final payload for a unidirectional
send stream.

## Shared Code Across Native And Adapters

Keep upper-layer code on `ZmuxSession`, `ZmuxStream`, `ZmuxSendStream`, and
`ZmuxRecvStream` when it should work with both native ZMux and adapters.

```java
void handle(ZmuxSession session) throws Exception {
    try (ZmuxStream stream = session.acceptStream()) {
        byte[] request = stream.readAllBytes();
        stream.writeFinal(request);
    }
}
```

The optional async surface is transport-agnostic too:

```java
import io.zmux.ZmuxAsync;
import io.zmux.ZmuxAsyncSession;

ZmuxAsync.optionalSession(session).ifPresent(async -> {
    async.openStreamAsync()
            .thenCompose(stream -> stream.writeFinalAsync(new byte[]{1, 2, 3}));
});
```

Use `ZmuxAsync.session(...)`, `stream(...)`, `sendStream(...)`, or
`recvStream(...)` when lack of async support should fail fast. Use
`supportsSession(...)` or `optionalSession(...)` when async support is optional.

## Metadata And Priority

Default sessions advertise the implemented metadata capabilities. Use
`OpenOptions` when a new stream should carry opener metadata:

```java
import io.zmux.MetadataUpdate;
import io.zmux.OpenOptions;

OpenOptions options = OpenOptions.builder()
        .priority(7L)
        .group(2L)
        .openInfo("rpc")
        .build();

try (ZmuxSession session = Zmux.openSession(socket);
     ZmuxStream stream = session.openStream(options)) {
    stream.updateMetadata(MetadataUpdate.priority(3L));
    stream.writeFinal("hello".getBytes(StandardCharsets.UTF_8));
}
```

The peer reads open metadata through `stream.openInfo()` or
`stream.metadata()`.

## Custom Transports

Any reliable ordered byte transport can be wrapped as a `DuplexConnection`:

```java
import io.zmux.transport.DuplexConnection;
import io.zmux.transport.ZmuxConnections;

DuplexConnection connection = ZmuxConnections.builder(input, output)
        .closer(transport)
        .addresses(localAddress, remoteAddress)
        .gatheringOutput(gatheringOutput)
        .build();

ZmuxSession session = Zmux.openSession(connection);
```

Convenience factories:

```java
ZmuxConnections.of(socket);
ZmuxConnections.of(byteChannel);
ZmuxConnections.of(readableChannel, writableChannel);
ZmuxConnections.of(input, output);
ZmuxConnections.of(input, output, closer);
ZmuxConnections.of(input, output, localAddress, remoteAddress);
ZmuxConnections.of(input, output, closer, localAddress, remoteAddress);
ZmuxConnections.of(input, output, closer, localAddress, remoteAddress, gatheringOutput);
ZmuxConnections.of(zmuxStream);
```

When a transport exposes read and write halves separately, join them:

```java
DuplexConnection connection = Zmux.join(readHalf, writeHalf);
DuplexConnection streamConnection = Zmux.join(recvStream, sendStream);
DuplexConnection ioConnection = Zmux.join(input, output);
```

`ReadHalf` and `WriteHalf` are small interfaces for custom transports that can
also expose deadlines, addresses, and gathering writes. `JoinedDuplexConnection`
supports pause/resume handles when a caller needs to swap the active read or
write half.

## Closing And Errors

```java
stream.closeWrite();                  // graceful local send-half close
stream.closeRead();                   // local read cancellation
stream.closeWithError(0x100L, "bye"); // stream application error

session.close();                      // graceful session close
session.closeWithError(0x100L, "bye");
session.awaitTerminationOrThrow();
```

Use `ZmuxErrors` instead of matching exception text:

```java
try {
    stream.write(payload);
} catch (IOException error) {
    if (ZmuxErrors.sessionClosed(error)) {
        return;
    }

    ApplicationError app = ZmuxErrors.applicationError(error);
    if (app != null) {
        long code = app.applicationCode();
        String reason = app.reason();
    }
}
```

Common helpers include `sessionClosed(...)`, `readClosed(...)`,
`writeClosed(...)`, `timeout(...)`, `interrupted(...)`,
`applicationError(...)`, `openLimited(...)`, `openExpired(...)`,
`openInfoUnavailable(...)`, `priorityUpdateUnavailable(...)`, and
`adapterUnsupported(...)`.

## Configuration

```java
ZmuxConfig config = ZmuxConfig.builder()
        .keepaliveInterval(Duration.ofSeconds(30))
        .keepaliveTimeout(Duration.ofSeconds(10))
        .eventHandler(event -> {
            // observe stream/session lifecycle
        })
        .build();
```

`Settings.builder()` controls negotiated stream windows, incoming stream
limits, frame payload limits, scheduler hints, and ping padding keys.

`ZmuxConfig.configureDefaultConfig(...)` can set the process-wide default
template during startup; `ZmuxConfig.resetDefaultConfig()` restores built-in
defaults.

Built-in defaults enable metadata capabilities and keepalive PINGs. Use
`disableCapabilities()` when a deployment needs to advertise no optional
protocol features.

## Native And Diagnostics

`ZmuxNativeSession` extends `ZmuxSession` with native protocol controls:
`ping(...)`, `goAway(...)`, `peerGoAwayError()`, `peerCloseError()`,
`localPreface()`, `peerPreface()`, and `negotiated()`.

`io.zmux.protocol.ZmuxCodec`, `Protocol`, `Preface`, and `Negotiated` are
public for diagnostics, proxies, and conformance tests. `ZmuxConformance` and
`ZmuxCoreConformance` describe the implementation's conformance surface.

## Semantics

- Successful write calls mean the local implementation accepted and flushed the
  write to its backend. They are not peer application acknowledgements.
- Buffers passed to write/open-and-send methods are not retained after the call
  returns.
- `closeWrite()` finishes only the local send half.
- `closeRead()` cancels local interest in inbound bytes.
- Open metadata is sent only when negotiated; required but unavailable metadata
  fails instead of being silently discarded.
