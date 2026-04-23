# zmux-java

Java implementation of the ZMux stream multiplexing protocol.

Released artifacts are Java 8+ compatible. If you import the source modules
directly into your application build, they follow your application's Java
configuration.

## Installation

Pick one of these common import styles.

### Gradle From Maven Central

Use this after a release is published:

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("io.github.zmuxio:zmux:VERSION")
}
```

Optional Netty QUIC adapter:

```kotlin
dependencies {
    implementation("io.github.zmuxio:zmux-netty-quic:VERSION")
}
```

The adapter wraps an existing Netty `QuicChannel`. If your application does
not already provide Netty QUIC native runtime artifacts, add the native module
for your platform too:

```kotlin
dependencies {
    runtimeOnly("io.netty:netty-codec-native-quic:NETTY_VERSION:OS_CLASSIFIER")
}
```

Typical classifiers are `windows-x86_64`, `linux-x86_64`, `linux-aarch_64`,
`osx-x86_64`, and `osx-aarch_64`.

### Maven From Maven Central

Use this after a release is published:

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
artifacts, add the platform native dependency as well:

```xml
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-codec-native-quic</artifactId>
    <version>NETTY_VERSION</version>
    <classifier>OS_CLASSIFIER</classifier>
    <scope>runtime</scope>
</dependency>
```

### Gradle From GitHub

Maven and Gradle do not consume a raw GitHub URL as a normal dependency. For a
GitHub tag or commit without manual cloning, use JitPack.

Gradle Kotlin DSL:

```kotlin
repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.zmuxio.zmux-java:zmux:TAG_OR_COMMIT")
}
```

Gradle Groovy DSL:

```groovy
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.zmuxio.zmux-java:zmux:TAG_OR_COMMIT'
}
```

Optional Netty QUIC adapter:

```kotlin
dependencies {
    implementation("com.github.zmuxio.zmux-java:zmux-netty-quic:TAG_OR_COMMIT")
}
```

Use a release tag or commit hash for repeatable builds.

### Maven From GitHub

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.zmuxio.zmux-java</groupId>
    <artifactId>zmux</artifactId>
    <version>TAG_OR_COMMIT</version>
</dependency>
```

Optional Netty QUIC adapter:

```xml
<dependency>
    <groupId>com.github.zmuxio.zmux-java</groupId>
    <artifactId>zmux-netty-quic</artifactId>
    <version>TAG_OR_COMMIT</version>
</dependency>
```

### Gradle From A Local Maven Install

Install once:

```bash
mvn -DskipTests install
```

Then use `mavenLocal()`:

```kotlin
repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("io.github.zmuxio:zmux:VERSION")
}
```

### Maven From A Local Maven Install

If you already have the source checkout and want to install it into your local
Maven cache:

```bash
mvn -DskipTests install
```

Then use the Maven Central coordinates shown above. Maven checks the local
cache before downloading remote artifacts.

### IDE Or Source Module Import

If your application imports the source modules directly, add the `zmux` module
as a dependency of your application module. Add `zmux-netty-quic` only if you
need the Netty QUIC adapter.

In this mode the source is compiled by your application's build, so it follows
your project's Java version and compiler settings.

## Supported Interfaces

Core session and stream APIs:

- `Zmux`: opens client, server, or auto-role sessions over `Socket`,
  `InputStream` / `OutputStream`, `ByteChannel`, split NIO channels, or a custom
  `DuplexConnection`.
- `ZmuxSession`: accepts and opens bidirectional streams, accepts and opens
  unidirectional streams, provides `openAndSend` helpers for whole arrays,
  array slices, and `ByteBuffer`, reports state/stats, closes gracefully or
  with an application error, and waits for termination.
- `ZmuxStream`: bidirectional stream interface combining send and receive
  operations.
- `ZmuxSendStream`: write side interface for `byte[]`, `ByteBuffer`,
  `OutputStream` adaptation, `writeFinal`, `writevFinal`, metadata updates,
  write deadlines, graceful write close, and write cancellation.
- `ZmuxRecvStream`: read side interface for `byte[]`, `ByteBuffer`,
  `readAllBytes`, `InputStream` adaptation, read deadlines, local read close,
  and read cancellation.
- `ZmuxStreamInfo`: common stream metadata such as stream id, open info,
  priority/group metadata, and local/remote addresses.

Native transport APIs:

- `ZmuxNativeSession`: `ZmuxSession` plus native ping, GOAWAY, local/peer
  preface, negotiated settings, peer GOAWAY, and peer close error details for
  the native framing transport.
- `ZmuxNativeStream`, `ZmuxNativeSendStream`, `ZmuxNativeRecvStream`: native
  stream views that expose the same stream operations plus open direction,
  stream direction, and read/write closed state.
- `DuplexConnection`, `BasicDuplexConnection`, `JoinedDuplexConnection`, and
  `ZmuxConnections`: adapters for sockets, streams, and channels.

Configuration and metadata APIs:

- `ZmuxConfig`: session configuration for settings, limits, capabilities,
  scheduler hints, keepalive, graceful close, flow-control budgets, memory
  budgets, abuse protection, and event handlers.
- `Settings` and `Limits`: negotiated protocol limits, frame payload limits,
  stream limits, data windows, and control/extension payload budgets.
- `OpenOptions`: initial stream priority, group, and open metadata, with
  constructor, factory, and builder APIs.
- `MetadataUpdate`: runtime priority/group updates, with constructor, factory,
  and builder APIs.
- `StreamMetadata`: accepted stream metadata snapshot.
- `SchedulerHint`: unspecified/balanced, latency, balanced fair, bulk
  throughput, and group-fair scheduling hints.

Events, stats, and errors:

- `ZmuxEventHandler`, `ZmuxEvent`, and `ZmuxEventType`: session and stream event
  callbacks.
- `SessionState` and `SessionStats`: runtime state and diagnostics snapshots.
- `ZmuxException` and typed exceptions such as `OpenTimeoutException`,
  `AcceptTimeoutException`, `ReadTimeoutException`, `WriteTimeoutException`,
  `SessionClosedException`, `ReadClosedException`, and `WriteClosedException`.
- `ZmuxErrorDetails`, `ZmuxErrorSource`, `ZmuxErrorScope`,
  `ZmuxErrorDirection`, `ZmuxTerminationKind`, and `ZmuxErrors`: structured
  error metadata and helper lookup methods.

Codec and conformance APIs:

- `ZmuxCodec`: public helpers for varints, TLVs, frames, settings, prefaces,
  and negotiation.
- `Frame`, `FrameType`, `ParsedFrame`, `Preface`, `Tlv`, `DecodedVarint`,
  `Protocol`, `Role`, and `ErrorCode`: protocol value types and constants.
- `ZmuxConformance`, `ZmuxClaim`, `ZmuxConformanceSuite`,
  `ZmuxImplementationProfile`, and `ZmuxCoreConformance`: advertised
  implementation claims and conformance metadata.

Netty QUIC adapter APIs:

- `NettyQuic.wrapSession(QuicChannel)`: wraps a Netty `QuicChannel` as a
  `ZmuxSession`.
- `NettyQuicSessionOptions`: adapter options for accepted-stream prelude
  handling.
- `NettyQuicConformance`: conformance metadata for the QUIC adapter module.

## Supported Features

- Bidirectional and unidirectional logical streams over one underlying
  connection.
- Client, server, and auto-role native session establishment.
- Socket, stream, NIO channel, custom duplex connection, and Netty QUIC
  transports.
- Per-stream read/write APIs with `byte[]`, `ByteBuffer`, `readAllBytes`,
  `InputStream`, and `OutputStream` adapters.
- Final writes through `writeFinal` and multipart final writes through
  `writevFinal`.
- Open metadata, stream priority hints, stream groups, and runtime priority
  updates when negotiated by both peers.
- Fair ordinary-data scheduling with priority, group fairness, latency hints,
  bulk hints, and retained batch bias.
- Session and stream flow control with window updates, blocked notifications,
  queued-data watermarks, and memory-budget enforcement.
- Graceful stream close, local read cancellation, write cancellation, stream
  application errors, graceful session close, and session application close.
- GOAWAY-style graceful drain and local-open refusal after drain starts.
- Keepalive ping/pong, ping timeout handling, read/write idle tracking, and
  native-session explicit ping support.
- Per-operation timeouts and deadlines for open, accept, read, write, ping, and
  termination waits where the operation is supported by the chosen session
  interface.
- Structured protocol, transport, timeout, interruption, local, and remote
  errors with stable metadata.
- Peer reason and open-info budget accounting to avoid unbounded retained
  memory.
- Abuse protection for no-op control floods, hidden terminal churn, and priority
  rebucket churn.
- Event callbacks for stream open/accept and session close.
- Session statistics for flow control, queued data, progress, keepalive,
  graceful close, terminal reasons, diagnostics, and adapter state.
- Public codec helpers for integration tests, proxies, diagnostics, and
  conformance tools.

## Usage

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

### Unidirectional Streams

```java
import io.zmux.ZmuxRecvStream;
import io.zmux.ZmuxSendStream;

import java.nio.charset.StandardCharsets;

try (ZmuxSendStream send = session.openUniStream()) {
    send.writeFinal("event".getBytes(StandardCharsets.UTF_8));
}

try (ZmuxRecvStream recv = session.acceptUniStream()) {
    String event = new String(recv.readAllBytes(), StandardCharsets.UTF_8);
}
```

### Existing Buffers

If your payload already lives in a slice or `ByteBuffer`, you can send it
directly without opening the stream and writing in two separate steps:

```java
import java.nio.ByteBuffer;

byte[] frame = ...;
ZmuxStream bidi = session.openAndSend(frame, 4, 128);
ZmuxSendStream uni = session.openUniAndSend(ByteBuffer.wrap(frame, 132, 64));
```

### Open Metadata And Priority

Both peers must enable the capabilities they intend to use.

```java
import io.zmux.MetadataUpdate;
import io.zmux.OpenOptions;
import io.zmux.Protocol;
import io.zmux.Zmux;
import io.zmux.ZmuxConfig;
import io.zmux.ZmuxSession;
import io.zmux.ZmuxStream;

import java.net.Socket;

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
        .openInfo("ssh")
        .build();

try (Socket socket = new Socket("127.0.0.1", 9000);
     ZmuxSession session = Zmux.clientSession(socket, config);
     ZmuxStream stream = session.openStream(options)) {
    stream.updateMetadata(MetadataUpdate.of(3L, 2L));
    stream.writeFinal("hello".getBytes(StandardCharsets.UTF_8));
}
```

The peer can read `stream.openInfo()` and `stream.metadata()` after accepting
the stream.

### Timeouts And Deadlines

```java
import io.zmux.ZmuxStream;

import java.time.Duration;

ZmuxStream stream = session.openStreamWithTimeout(Duration.ofSeconds(2));
stream.setReadTimeout(Duration.ofSeconds(5));
stream.setWriteTimeout(Duration.ofSeconds(5));
stream.clearReadDeadline();
stream.clearWriteDeadline();
```

### Closing

```java
import java.time.Duration;

stream.closeWrite();                  // graceful write-half close
stream.closeRead();                   // local read cancellation
stream.closeWithError(0x100L, "bye"); // stream application error
session.close();                      // graceful session close
session.closeWithError(0x100L, "bye");
session.awaitTerminationOrThrow(Duration.ofSeconds(5));
```

### Netty QUIC Adapter

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

The QUIC adapter returns the same `ZmuxSession`, `ZmuxStream`,
`ZmuxSendStream`, and `ZmuxRecvStream` interfaces as the native transport. Use
`zmux-netty-quic` only when the underlying transport is already a Netty
`QuicChannel`.
