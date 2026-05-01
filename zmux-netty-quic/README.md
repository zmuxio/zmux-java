# zmux-netty-quic

`zmux-netty-quic` wraps an established Netty QUIC `QuicChannel` behind the
stable `ZmuxSession` API. Wrapped sessions and streams also expose the
transport-agnostic `ZmuxAsync*` interfaces plus Netty-specific async escape
hatches.

It is an adapter over QUIC streams. It does not create QUIC connections and it
does not expose the native ZMux wire-session API such as `ZmuxNativeSession`,
`ping(...)`, `goAway(...)`, prefaces, or negotiated native settings.

## Installation

Add the adapter module when your application already uses Netty QUIC:

<details>
<summary>Gradle</summary>

```kts
dependencies {
    implementation("io.github.zmuxio:zmux-netty-quic:VERSION")
}
```

If your application does not already provide Netty QUIC native runtime
artifacts, add the platform native runtime too:

```kts
dependencies {
    runtimeOnly("io.netty:netty-codec-native-quic:NETTY_VERSION:OS_CLASSIFIER")
}
```

</details>

<details>
<summary>Maven</summary>

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

`zmux-netty-quic` depends on the Netty QUIC classes it compiles against. The
platform native runtime is separate so applications can choose the classifier
that matches their deployment target.

## Usage

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

try (ZmuxSession session = NettyQuic.wrapSession(channel, options);
     ZmuxStream stream = session.openStream()) {
    stream.writeFinal("hello".getBytes(StandardCharsets.UTF_8));
}
```

Call blocking ZMux APIs from application or worker threads, not from the Netty
event loop.

Use the optional async surface when shared upper-layer code should not block the
caller:

```java
import io.zmux.ZmuxAsync;

import java.nio.charset.StandardCharsets;

ZmuxAsync.optionalSession(session).ifPresent(async -> {
    async.openStreamAsync()
            .thenCompose(stream -> stream.writeFinalAsync("hello".getBytes(StandardCharsets.UTF_8)));
});
```

`writeAsync(...)` completes when the Netty QUIC write primitive accepts or
rejects the data. It does not mean the peer received or acknowledged the
stream bytes.

## Constructors

```java
NettyQuic.wrapSession(channel);
NettyQuic.wrapSession(channel, acceptedPreludeReadTimeout);
NettyQuic.wrapSession(channel, acceptedPreludeMaxConcurrent);
NettyQuic.wrapSession(channel, acceptedPreludeReadTimeout, acceptedPreludeMaxConcurrent);
NettyQuic.wrapSession(channel, options);
NettyQuic.wrapSessionWithOptions(channel, options);
```

`wrapSession(null, ...)` returns a closed `ZmuxSession`, which is useful for
null-safe integration paths.

## Options

```java
NettyQuicSessionOptions.defaults();
NettyQuicSessionOptions.ofAcceptedPreludeReadTimeout(timeout);
NettyQuicSessionOptions.ofAcceptedPreludeMaxConcurrent(maxConcurrent);
options.withAcceptedPreludeReadTimeout(timeout);
options.withAcceptedPreludeMaxConcurrent(maxConcurrent);

NettyQuic.defaultAcceptedPreludeMaxConcurrent();
NettyQuic.setDefaultAcceptedPreludeMaxConcurrent(maxConcurrent);
```

`acceptedPreludeReadTimeout`:

- `null` or `Duration.ZERO`: use `NettyQuic.DEFAULT_ACCEPTED_PRELUDE_READ_TIMEOUT`
- negative duration: disable the adapter-managed read timeout
- accepted QUIC streams whose adapter prelude never arrives in time are
  discarded instead of blocking later ready streams

`acceptedPreludeMaxConcurrent`:

- `0`: use `NettyQuic.defaultAcceptedPreludeMaxConcurrent()`
- `> 0`: per-session upper bound for concurrently parsing accepted adapter
  preludes
- values above the adapter safety cap are clamped

Accepted prelude parsing runs off the event loop. The worker pool can be tuned
with JVM system properties:

```text
io.zmux.netty.acceptedPreludeWorkers
io.zmux.netty.acceptedPreludeWorkerQueueCapacity
```

## Stable API Coverage

The wrapper returns the stable `ZmuxSession` surface, so callers can share the
same application code with native ZMux sessions:

- `acceptStream(...)` / `acceptUniStream(...)`
- `openStream(...)` / `openUniStream(...)`
- `openAndSend(...)` / `openUniAndSend(...)`
- `close()`, `closeWithError(...)`, `awaitTermination(...)`, `isClosed()`,
  `state()`, and `stats()`

Wrapped streams expose the stable `ZmuxStream`, `ZmuxSendStream`, and
`ZmuxRecvStream` methods:

- `streamId()`, `openInfo()`, `metadata()`, and `updateMetadata(...)`
- `write(...)`, `writeFinal(...)`, and `writevFinal(...)`
- `read(...)`, `readAllBytes(...)`, `asInputStream()`, and `asOutputStream()`
- `closeRead()`, `cancelRead(...)`, `closeWrite()`, `cancelWrite(...)`, and
  `closeWithError(...)`
- read and write deadline helpers

The same wrapped objects also implement the optional core async interfaces:

- `ZmuxAsyncSession`: `openStreamAsync(...)`, `openUniStreamAsync(...)`,
  `acceptStreamAsync()`, `acceptUniStreamAsync()`, `closeAsync()`, and
  `closeWithErrorAsync(...)`
- `ZmuxAsyncStream`, `ZmuxAsyncSendStream`, and `ZmuxAsyncRecvStream`:
  async write, close, cancel, and stream-error operations

Netty-specific async extensions are available through
`NettyQuicAsyncSession`, `NettyQuicAsyncStream`, `NettyQuicAsyncSendStream`,
and `NettyQuicAsyncRecvStream`:

```java
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.quic.QuicChannel;
import io.zmux.ZmuxAsync;
import io.zmux.ZmuxAsyncStream;
import io.zmux.adapter.quic.netty.NettyQuicAsyncSendStream;
import io.zmux.adapter.quic.netty.NettyQuicAsyncSession;

import java.nio.charset.StandardCharsets;

NettyQuicAsyncSession nettySession = (NettyQuicAsyncSession) ZmuxAsync.session(session);
QuicChannel rawChannel = nettySession.unsafeQuicChannel();

ZmuxAsyncStream asyncStream = nettySession.openStreamAsync().toCompletableFuture().join();
NettyQuicAsyncSendStream nettyStream = (NettyQuicAsyncSendStream) asyncStream;
ByteBuf byteBuf = rawChannel.alloc().buffer().writeBytes("hello".getBytes(StandardCharsets.UTF_8));
ChannelFuture future = nettyStream.writeNettyAsync(byteBuf);
```

`writeNettyAsync(...)` and `writeFinalNettyAsync(...)` still run through the
adapter stream state, but return Netty `ChannelFuture` values. The `unsafe*`
methods expose raw Netty channels for advanced integration.

## Mapping

- `acceptStream(...)` / `openStream(...)` map to QUIC bidirectional streams.
- `acceptUniStream(...)` / `openUniStream(...)` map to QUIC unidirectional
  streams.
- Open-time ZMux metadata is carried in an adapter prelude:
  `varint(metadata_len)` followed by metadata TLVs.
- `OpenOptions` supports open info, initial priority, and initial group.
- `openInfo()` and `metadata()` expose decoded opener metadata on accepted
  streams.
- `updateMetadata(...)` works only before the local stream prelude is emitted.
  Later updates fail with `PriorityUpdateUnavailableException`.
- A fresh locally opened bidirectional stream submits the adapter prelude before
  read-side terminal control such as `closeRead()` or `cancelRead(...)`.
- `closeRead()` maps to QUIC read-side cancellation with `ErrorCode.CANCELLED`.
- `cancelRead(code)` maps to QUIC read-side cancellation with that code.
- `closeWrite()` maps to QUIC send-side graceful close.
- `cancelWrite(code)` maps to QUIC send-side reset with that code.
- `closeWithError(code, reason)` is best-effort at stream scope: bidirectional
  streams close both local directions; unidirectional streams close the locally
  meaningful direction QUIC exposes.
- QUIC application error codes are 32-bit. Codes outside that range fail with
  `AdapterUnsupportedException`.

Fresh write-side reset or abort visibility is not a portable adapter guarantee
because QUIC can discard previously written but unacknowledged stream data,
including a just-submitted metadata prelude.

## Errors

- QUIC connection application closes are normalized to `ApplicationError`.
- QUIC stream reset/cancel codes are surfaced as `ApplicationError` where Netty
  exposes the numeric code.
- QUIC stream-limit failures are normalized to `OpenLimitedException`.
- QUIC transport or channel closure is normalized into the stable ZMux error
  surface.

Use `ZmuxErrors` helpers such as `applicationError(...)`, `openLimited(...)`,
`adapterUnsupported(...)`, `priorityUpdateUnavailable(...)`,
`sessionClosed(...)`, `readClosed(...)`, `writeClosed(...)`, and
`timeout(...)` instead of depending on Netty exception classes.

## Reduced Behavior

- No native ZMux session helpers such as `ping(...)`, `goAway(...)`,
  `peerGoAwayError()`, `peerCloseError()`, `localPreface()`, `peerPreface()`,
  or `negotiated()`.
- No post-open native advisory frames such as native `PRIORITY_UPDATE`.
- No QUIC datagram, packet acknowledgement, RTT, or loss-state API.
- `stats()` reports adapter-visible session and stream counters. It cannot
  expose native ZMux runtime internals that do not exist in QUIC.

## Conformance

`NettyQuicConformance` provides adapter conformance support for implementation
tests that need to exercise the stable ZMux session contract over Netty QUIC.
