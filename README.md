# zmux-java

Java implementation of the ZMux stream multiplexing protocol.

Released artifacts are Java 8+ compatible. Public APIs live under `io.zmux`
and `io.zmux.adapter.quic.netty`; `io.zmux.internal` is implementation detail.

It provides:

- native ZMux sessions through `ZmuxNativeSession`
- transport-agnostic stable interfaces: `ZmuxSession`, `ZmuxStream`,
  `ZmuxSendStream`, and `ZmuxRecvStream`
- an optional Netty QUIC adapter in `zmux-netty-quic`

## Installation

Use `io.github.zmuxio:zmux` for the core APIs and native ZMux transport.
Add `io.github.zmuxio:zmux-netty-quic` only when wrapping an existing Netty
QUIC `QuicChannel`.

Placeholders:

- `VERSION`: zmux-java version.
- `NETTY_VERSION`: Netty QUIC version compatible with `zmux-netty-quic`.
- `OS_CLASSIFIER`: Netty native runtime classifier, such as `windows-x86_64`,
  `linux-x86_64`, `linux-aarch_64`, `osx-x86_64`, or `osx-aarch_64`.

<details>
<summary>Gradle</summary>

`build.gradle.kts`:

```kts
repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.zmuxio:zmux:VERSION")
}
```

Optional Netty QUIC adapter:

```kts
dependencies {
    implementation("io.github.zmuxio:zmux-netty-quic:VERSION")
}
```

The adapter artifact brings the Netty QUIC classes it compiles against. If
your application does not already provide Netty QUIC native runtime artifacts,
add the platform native runtime too:

```kts
dependencies {
    runtimeOnly("io.netty:netty-codec-native-quic:NETTY_VERSION:OS_CLASSIFIER")
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

```groovy
dependencies {
    implementation 'io.github.zmuxio:zmux-netty-quic:VERSION'
}
```

If your application does not already provide Netty QUIC native runtime
artifacts, add the platform native runtime too:

```groovy
dependencies {
    runtimeOnly 'io.netty:netty-codec-native-quic:NETTY_VERSION:OS_CLASSIFIER'
}
```

</details>

<details>
<summary>Maven</summary>

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

</details>

## Constructors

Use the stable constructors when the rest of your application should work with
native ZMux and adapters through the same interfaces:

```java
ZmuxSession session = Zmux.openSession(socket);
ZmuxSession client = Zmux.clientSession(socket);
ZmuxSession server = Zmux.serverSession(socket);
```

Use `openSession(...)` when the underlying connection is already established
and the application does not need to name one peer as client and the other as
server. Both peers may call the same `openSession(...)` constructor; ZMux
negotiates the initiator/responder role during establishment.

Use `clientSession(...)` or `serverSession(...)` when an outer protocol already
defines the side that must be the ZMux initiator or responder. Use the native
constructors only when you need native-only controls:

```java
ZmuxNativeSession nativeSession = Zmux.open(socket);
ZmuxNativeSession nativeClient = Zmux.client(socket);
ZmuxNativeSession nativeServer = Zmux.server(socket);
```

Every constructor family supports `Socket`, `InputStream` plus `OutputStream`,
`ByteChannel`, `ReadableByteChannel` plus `WritableByteChannel`, and
`DuplexConnection`, with optional `ZmuxConfig` overloads.

<details>
<summary>Constructor matrix</summary>

Stable session constructors:

```java
Zmux.openSession(DuplexConnection connection);
Zmux.openSession(DuplexConnection connection, ZmuxConfig config);
Zmux.openSession(Socket socket);
Zmux.openSession(Socket socket, ZmuxConfig config);
Zmux.openSession(InputStream input, OutputStream output);
Zmux.openSession(InputStream input, OutputStream output, ZmuxConfig config);
Zmux.openSession(ByteChannel channel);
Zmux.openSession(ByteChannel channel, ZmuxConfig config);
Zmux.openSession(ReadableByteChannel input, WritableByteChannel output);
Zmux.openSession(ReadableByteChannel input, WritableByteChannel output, ZmuxConfig config);

Zmux.clientSession(...);
Zmux.serverSession(...);
```

Native session constructors use the same transport and config overloads:

```java
Zmux.open(...);
Zmux.client(...);
Zmux.server(...);
```

Null-safe and adapter helpers:

```java
Zmux.closedSession();
Zmux.closedNativeSession();
Zmux.asSession(session);
Zmux.asNativeSession(nativeSession);
```

</details>

## Basic Use

The examples assume the surrounding method declares `throws Exception`.

### Auto-Role Session

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
    String reply = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
}
```

### Fixed Client And Server

```java
try (Socket socket = new Socket("127.0.0.1", 9000);
     ZmuxSession session = Zmux.clientSession(socket);
     ZmuxStream stream = session.openStream()) {
    stream.writeFinal("hello".getBytes(StandardCharsets.UTF_8));
    byte[] reply = stream.readAllBytes();
}
```

```java
try (ServerSocket listener = new ServerSocket(9000);
     Socket socket = listener.accept();
     ZmuxSession session = Zmux.serverSession(socket);
     ZmuxStream stream = session.acceptStream()) {
    byte[] request = stream.readAllBytes();
    stream.writeFinal(request);
}
```

### Transport-Agnostic Handler

Native sessions and the Netty QUIC adapter expose the same stable session and
stream interfaces, so upper layers can share one handler:

```java
void handle(ZmuxSession session) throws Exception {
    try (ZmuxStream stream = session.acceptStream()) {
        byte[] request = stream.readAllBytes();
        stream.writeFinal(request);
    }
}
```

## Streams

Open and use a bidirectional stream:

```java
try (ZmuxStream stream = session.openStream()) {
    stream.write("request".getBytes(StandardCharsets.UTF_8));
    stream.closeWrite();

    byte[] response = stream.readAllBytes();
}
```

Use unidirectional streams when only one side writes:

```java
try (ZmuxSendStream send = session.openUniStream()) {
    send.writeFinal("event".getBytes(StandardCharsets.UTF_8));
}

try (ZmuxRecvStream recv = session.acceptUniStream()) {
    byte[] event = recv.readAllBytes();
}
```

Open a stream and send an initial payload in one call:

```java
ZmuxStream bidi = session.openAndSend("hello".getBytes(StandardCharsets.UTF_8));
ZmuxSendStream uni = session.openUniAndSend(ByteBuffer.wrap(payload));
```

`openAndSend(...)` leaves the bidirectional stream open. `openUniAndSend(...)`
writes the final contents of a unidirectional stream.

<details>
<summary>Stable session methods</summary>

```java
ZmuxStream acceptStream();
ZmuxStream acceptStream(Duration timeout);
ZmuxRecvStream acceptUniStream();
ZmuxRecvStream acceptUniStream(Duration timeout);

ZmuxStream openStream();
ZmuxStream openStream(OpenOptions options);
ZmuxStream openStream(Duration timeout);
ZmuxStream openStream(OpenOptions options, Duration timeout);
ZmuxStream openStreamWithTimeout(Duration timeout);
ZmuxStream openStreamWithTimeout(OpenOptions options, Duration timeout);

ZmuxSendStream openUniStream();
ZmuxSendStream openUniStream(OpenOptions options);
ZmuxSendStream openUniStream(Duration timeout);
ZmuxSendStream openUniStream(OpenOptions options, Duration timeout);
ZmuxSendStream openUniStreamWithTimeout(Duration timeout);
ZmuxSendStream openUniStreamWithTimeout(OpenOptions options, Duration timeout);

ZmuxStream openAndSend(byte[] data);
ZmuxStream openAndSend(OpenOptions options, byte[] data);
ZmuxStream openAndSend(byte[] data, int offset, int length);
ZmuxStream openAndSend(OpenOptions options, byte[] data, int offset, int length);
ZmuxStream openAndSend(ByteBuffer data);
ZmuxStream openAndSend(OpenOptions options, ByteBuffer data);
ZmuxStream openAndSend(Duration timeout, byte[] data);
ZmuxStream openAndSend(OpenOptions options, Duration timeout, byte[] data);
ZmuxStream openAndSend(Duration timeout, byte[] data, int offset, int length);
ZmuxStream openAndSend(OpenOptions options, Duration timeout, byte[] data, int offset, int length);
ZmuxStream openAndSend(Duration timeout, ByteBuffer data);
ZmuxStream openAndSend(OpenOptions options, Duration timeout, ByteBuffer data);
ZmuxStream openAndSendWithTimeout(Duration timeout, byte[] data);
ZmuxStream openAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data);
ZmuxStream openAndSendWithTimeout(Duration timeout, byte[] data, int offset, int length);
ZmuxStream openAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data, int offset, int length);
ZmuxStream openAndSendWithTimeout(Duration timeout, ByteBuffer data);
ZmuxStream openAndSendWithTimeout(OpenOptions options, Duration timeout, ByteBuffer data);

ZmuxSendStream openUniAndSend(byte[] data);
ZmuxSendStream openUniAndSend(OpenOptions options, byte[] data);
ZmuxSendStream openUniAndSend(byte[] data, int offset, int length);
ZmuxSendStream openUniAndSend(OpenOptions options, byte[] data, int offset, int length);
ZmuxSendStream openUniAndSend(ByteBuffer data);
ZmuxSendStream openUniAndSend(OpenOptions options, ByteBuffer data);
ZmuxSendStream openUniAndSend(Duration timeout, byte[] data);
ZmuxSendStream openUniAndSend(OpenOptions options, Duration timeout, byte[] data);
ZmuxSendStream openUniAndSend(Duration timeout, byte[] data, int offset, int length);
ZmuxSendStream openUniAndSend(OpenOptions options, Duration timeout, byte[] data, int offset, int length);
ZmuxSendStream openUniAndSend(Duration timeout, ByteBuffer data);
ZmuxSendStream openUniAndSend(OpenOptions options, Duration timeout, ByteBuffer data);
ZmuxSendStream openUniAndSendWithTimeout(Duration timeout, byte[] data);
ZmuxSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data);
ZmuxSendStream openUniAndSendWithTimeout(Duration timeout, byte[] data, int offset, int length);
ZmuxSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, byte[] data, int offset, int length);
ZmuxSendStream openUniAndSendWithTimeout(Duration timeout, ByteBuffer data);
ZmuxSendStream openUniAndSendWithTimeout(OpenOptions options, Duration timeout, ByteBuffer data);

void closeWithError(long code, String reason);
void closeWithError(ErrorCode code, String reason);
void closeWithError(Throwable error);
boolean awaitTermination();
boolean awaitTermination(Duration timeout);
Optional<IOException> awaitTerminationCause();
Optional<IOException> awaitTerminationCause(Duration timeout);
void awaitTerminationOrThrow();
void awaitTerminationOrThrow(Duration timeout);
Optional<IOException> terminationCause();
boolean isClosed();
SessionState state();
SessionStats stats();
void close();
```

</details>

<details>
<summary>Stable stream methods</summary>

Common stream information:

```java
long streamId();
byte[] openInfo();
int openInfoLength();
boolean hasOpenInfo();
StreamMetadata metadata();
SocketAddress localAddress();
SocketAddress remoteAddress();
```

Send-side methods on `ZmuxStream` and `ZmuxSendStream`:

```java
void write(byte[] src);
void write(byte[] src, int offset, int length);
int write(ByteBuffer src);
int writeFinal(byte[] src);
int writeFinal(byte[] src, int offset, int length);
int writeFinal(ByteBuffer src);
int writevFinal(byte[]... parts);
OutputStream asOutputStream();
void updateMetadata(MetadataUpdate update);
void closeWrite();
void cancelWrite(long code);
void cancelWrite(ErrorCode code);
void closeWithError(long code, String reason);
void closeWithError(ErrorCode code, String reason);
void setWriteDeadline(Instant deadline);
void setWriteTimeout(Duration timeout);
void clearWriteDeadline();
void close();
```

Receive-side methods on `ZmuxStream` and `ZmuxRecvStream`:

```java
int read(byte[] dst);
int read(byte[] dst, int offset, int length);
int read(ByteBuffer dst);
byte[] readAllBytes();
byte[] readAllBytes(int maxBytes);
InputStream asInputStream();
void closeRead();
void cancelRead(long code);
void cancelRead(ErrorCode code);
void closeWithError(long code, String reason);
void closeWithError(ErrorCode code, String reason);
void setReadDeadline(Instant deadline);
void setReadTimeout(Duration timeout);
void clearReadDeadline();
void close();
```

`ZmuxStream` combines both halves and also exposes:

```java
void setDeadline(Instant deadline);
void setTimeout(Duration timeout);
void clearDeadline();
```

</details>

## Metadata And Priority

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

Supported value helpers:

```java
OpenOptions.empty();
OpenOptions.of(priority, group, openInfo);
OpenOptions.priority(priority);
OpenOptions.group(group);
OpenOptions.withOpenInfo(openInfoBytes);
OpenOptions.withOpenInfo(openInfoString);
OpenOptions.builder().priority(priority).group(group).openInfo(openInfo).build();

MetadataUpdate.of(priority, group);
MetadataUpdate.priority(priority);
MetadataUpdate.group(group);
MetadataUpdate.builder().priority(priority).group(group).build();

StreamMetadata.empty();
StreamMetadata.of(priority, group, openInfo);
StreamMetadata.withOpenInfo(openInfo);
```

## Timeouts, Closing, And Errors

```java
ZmuxStream stream = session.openStream(Duration.ofSeconds(2));
stream.setReadTimeout(Duration.ofSeconds(5));
stream.setWriteTimeout(Duration.ofSeconds(5));
stream.clearReadDeadline();
stream.clearWriteDeadline();
```

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

Error helpers include `ZmuxErrors.find(...)`, `details(...)`,
`applicationError(...)`, `hasCode(...)`, `code(...)`, `isCode(...)`,
`operation(...)`, `reason(...)`, `scope(...)`, `source(...)`, `direction(...)`,
`terminationKind(...)`, `timeout(...)`, `interrupted(...)`,
`adapterUnsupported(...)`, `priorityUpdateUnavailable(...)`,
`emptyMetadataUpdate(...)`, `openInfoUnavailable(...)`,
`openMetadataTooLarge(...)`, `openLimited(...)`, `openExpired(...)`,
`priorityUpdateTooLarge(...)`, `keepaliveTimeout(...)`, `sessionClosed(...)`,
`readClosed(...)`, `writeClosed(...)`, `streamNotReadable(...)`,
`streamNotWritable(...)`, and `gracefulCloseTimeout(...)`.

## Native-Only API

`ZmuxNativeSession` extends `ZmuxSession` and returns native stream types from
open/accept methods. It adds native session controls:

```java
Duration ping();
Duration ping(byte[] echo);
Duration ping(byte[] echo, Duration timeout);
void goAway(long lastAcceptedBidi, long lastAcceptedUni);
void goAway(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason);
void goAwayWithError(long lastAcceptedBidi, long lastAcceptedUni, long code, String reason);
void goAwayWithError(long lastAcceptedBidi, long lastAcceptedUni, ErrorCode code, String reason);
ApplicationError peerGoAwayError();
ApplicationError peerCloseError();
Preface localPreface();
Preface peerPreface();
Negotiated negotiated();
```

Native streams add local/native state queries:

```java
boolean openedLocally();
boolean bidirectional();
boolean readClosed();   // native receive streams
boolean writeClosed();  // native send streams
```

Adapters only need to implement the stable interfaces.

## Configuration

Start from defaults or a builder:

```java
ZmuxConfig config = ZmuxConfig.builder()
        .role(Role.AUTO)
        .prefacePadding(true)
        .pingPadding(true)
        .keepaliveInterval(Duration.ofSeconds(30))
        .keepaliveTimeout(Duration.ofSeconds(10))
        .gracefulCloseDrainTimeout(Duration.ofMillis(100))
        .eventHandler(event -> {
            // observe stream/session lifecycle
        })
        .build();

ZmuxSession session = Zmux.openSession(socket, config);
```

`ZmuxConfig.defaults()` returns the process-wide default template used by
constructors called with `null` config. Configure that template during process
initialization:

```java
ZmuxConfig.configureDefaultConfig(builder -> builder
        .prefacePadding(true)
        .pingPadding(true));

ZmuxConfig.resetDefaultConfig();
```

Important config groups:

- role and protocol negotiation: `role`, `tieBreakerNonce`, `minProto`,
  `maxProto`, `capabilities`
- limits and flow control: `settings`, `sessionMemoryCap`,
  `perStreamQueuedDataHwm`, `sessionQueuedDataHwm`
- liveness: `keepaliveInterval`, `keepaliveMaxPingInterval`,
  `keepaliveTimeout`
- padding: `prefacePadding`, `prefacePaddingMinBytes`,
  `prefacePaddingMaxBytes`, `pingPadding`, `pingPaddingMinBytes`,
  `pingPaddingMaxBytes`
- close and abuse protection: `gracefulCloseDrainTimeout`,
  `stopSendingGracefulDrainWindow`, control-frame budgets, tombstone budgets,
  and retained reason/open-info budgets
- diagnostics: `eventHandler`, `SessionStats`, and `ZmuxEvent`

`Settings.builder()` controls negotiated stream data windows, incoming stream
limits, max frame/control/extension payload sizes, idle timeout hints,
keepalive hints, scheduler hints, and the ping padding key.

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

ZmuxSession session = Zmux.openSession(connection);
```

Transport helpers:

```java
ZmuxConnections.builder(input, output)
        .closer(closer)
        .localAddress(localAddress)
        .remoteAddress(remoteAddress)
        .addresses(localAddress, remoteAddress)
        .gatheringOutput(gatheringOutput)
        .build();
ZmuxConnections.of(socket);
ZmuxConnections.of(input, output);
ZmuxConnections.of(input, output, localAddress, remoteAddress);
ZmuxConnections.of(input, output, closer, localAddress, remoteAddress, gatheringOutput);
ZmuxConnections.of(byteChannel);
ZmuxConnections.of(readableChannel, writableChannel);
ZmuxConnections.of(zmuxStream);
```

If the transport can enforce blocking read or write deadlines, implement
`supportsReadDeadline()` / `setReadDeadline(...)` and
`supportsWriteDeadline()` / `setWriteDeadline(...)`.

`ReadHalf` exposes `read(...)`, `read(ByteBuffer)`, `asInputStream()`,
`closeRead()`, read deadline helpers, and optional addresses. `WriteHalf`
exposes `write(...)`, `write(ByteBuffer)`, `asOutputStream()`, `closeWrite()`,
write deadline helpers, optional gathering output, and optional addresses.

Join directional halves when the underlying transport exposes read and write
sides separately:

```java
JoinedDuplexConnection connection = Zmux.join(readHalf, writeHalf);
ZmuxSession nested = Zmux.openSession(connection);
```

Supported join inputs:

```java
Zmux.join(ReadHalf input, WriteHalf output);
Zmux.join(ZmuxRecvStream input, ZmuxSendStream output);
Zmux.join(InputStream input, OutputStream output);
Zmux.join(InputStream input, OutputStream output, SocketAddress local, SocketAddress remote);
Zmux.join(InputStream input, OutputStream output, GatheringByteChannel gatheringOutput,
          SocketAddress local, SocketAddress remote);
```

`JoinedDuplexConnection` exposes `inputHalf()`, `outputHalf()`, `readHalf()`,
`writeHalf()`, `pauseInput(...)`, `pauseOutput(...)`, `pauseRead(...)`,
`pauseWrite(...)`, `closeInput()`, `closeOutput()`, `closeRead()`,
`closeWrite()`, deadline helpers, and pause handles for replacing the current
read or write half.

## Netty QUIC Adapter

Use `zmux-netty-quic` only when your application already has an established
Netty `QuicChannel`. The adapter exposes the stable `ZmuxSession`,
`ZmuxStream`, `ZmuxSendStream`, and `ZmuxRecvStream` interfaces, so the same
upper-layer code can handle native ZMux sessions and adapted QUIC sessions.

```java
import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;
import io.zmux.adapter.quic.netty.NettyQuic;

import java.nio.charset.StandardCharsets;

QuicChannel channel = ...;
try (ZmuxSession session = NettyQuic.wrapSession(channel);
     ZmuxStream stream = session.openStream()) {
    stream.writeFinal("hello".getBytes(StandardCharsets.UTF_8));
}
```

Call blocking ZMux APIs from application or worker threads, not from the Netty
event loop. Adapter-specific constructors, options, metadata mapping, error
mapping, and reduced behavior are documented in
[`zmux-netty-quic/README.md`](zmux-netty-quic/README.md).

## Codec And Diagnostics

`ZmuxCodec` is public for tests, proxies, diagnostics, and conformance tools:

```java
ZmuxCodec.varintLength(value);
ZmuxCodec.writeVarint(output, value);
ZmuxCodec.appendVarint(prefix, value);
ZmuxCodec.encodeVarint(value);
ZmuxCodec.parseVarint(bytes);
ZmuxCodec.parseVarint(bytes, offset);
ZmuxCodec.readVarint(input);
ZmuxCodec.writeTlv(output, type, value);
ZmuxCodec.appendTlv(prefix, type, value);
ZmuxCodec.parseTlvs(bytes);
ZmuxCodec.parseFrame(bytes, limits);
ZmuxCodec.readFrame(input, limits);
ZmuxCodec.writeFrame(output, frame, limits);
ZmuxCodec.parsePreface(bytes);
ZmuxCodec.readPreface(input);
ZmuxCodec.writePreface(output, preface);
ZmuxCodec.negotiatePrefaces(local, peer);
```

`ZmuxConformance`, `ZmuxCoreConformance`, and
`io.zmux.adapter.quic.netty.NettyQuicConformance` provide conformance support
for implementation tests.

`Protocol`, `Preface`, and `Negotiated` expose capability helpers such as
`hasCapability(...)`, `supportsOpenMetadata()`, `supportsPriorityUpdate()`,
`canCarryOpenInfo()`, and the priority/group metadata checks.

## Semantics Notes

- A successful `write(...)`, `writeFinal(...)`, `writevFinal(...)`,
  `openAndSend(...)`, or `openUniAndSend(...)` means the bytes entered the
  local ZMux send path; it is not a peer application acknowledgement.
- Payload buffers passed to write/open-and-send methods are not retained after
  the call returns.
- `closeWrite()` gracefully finishes only the local send half.
- `closeRead()` stops local interest in inbound bytes and sends cancellation.
- `closeWithError(code, reason)` carries a numeric application error and
  optional diagnostic text.
- `openInfo()` is peer-visible only when `OPEN_METADATA` is negotiated;
  requests that require unavailable open metadata fail instead of silently
  discarding it.
