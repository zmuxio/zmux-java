# zmux-java

[![CI](https://github.com/zmuxio/zmux-java/actions/workflows/ci.yml/badge.svg)](https://github.com/zmuxio/zmux-java/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.zmuxio/zmux?label=maven%20central)](https://central.sonatype.com/artifact/io.github.zmuxio/zmux)
[![Javadoc](https://javadoc.io/badge2/io.github.zmuxio/zmux/javadoc.svg)](https://javadoc.io/doc/io.github.zmuxio/zmux)
[![Java](https://img.shields.io/badge/Java-25%2B-blue)](pom.xml)
[![License](https://img.shields.io/github/license/zmuxio/zmux-java)](LICENSE)

[![zmux-quic](https://img.shields.io/maven-central/v/io.github.zmuxio/zmux-quic?label=zmux-quic)](https://central.sonatype.com/artifact/io.github.zmuxio/zmux-quic)
[![zmux-http](https://img.shields.io/maven-central/v/io.github.zmuxio/zmux-http?label=zmux-http)](https://central.sonatype.com/artifact/io.github.zmuxio/zmux-http)
[![zmux-shaping](https://img.shields.io/maven-central/v/io.github.zmuxio/zmux-shaping?label=zmux-shaping)](https://central.sonatype.com/artifact/io.github.zmuxio/zmux-shaping)

Java 25 stream and datagram multiplexing over reliable carriers, compatible
with QMux draft-02. The core has no runtime dependencies. A shared NIO event
loop owns each session; blocking application calls park their calling thread.
Virtual threads are a good fit. Calls that would block an event loop or the
same session's hook context fail with `IllegalStateException`.

The core module is `io.zmux`, exporting `io.zmux`, `io.zmux.carrier`,
`io.zmux.helper`, `io.zmux.seal` and `io.zmux.shaping`.

## Installation

The artifacts need Java 25 or newer; the transcript signatures use the JDK's own ML-DSA.

Maven:

```xml
<dependency>
    <groupId>io.github.zmuxio</groupId>
    <artifactId>zmux</artifactId>
    <version>VERSION</version>
</dependency>
```

Gradle: `implementation("io.github.zmuxio:zmux:VERSION")`. The optional
modules are separate artifacts of the same version: `zmux-quic` (QUIC
carriers on Netty, see [`zmux-quic/README.md`](zmux-quic/README.md)),
`zmux-http` (HTTP and WebSocket carriers on Netty) and `zmux-shaping` (shaping
hooks read from JSON).

## Sessions and streams

```java
import io.zmux.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

try (ZmuxSession session = Zmux.connect(
        new InetSocketAddress("example.com", 9000), ZmuxConfig.defaults());
     ZmuxStream stream = session.openStream()) {
    stream.write(ByteBuffer.wrap(request));
    stream.finish(); // FIN
    ByteBuffer response = ByteBuffer.allocate(65536);
    while (stream.read(response.clear()) >= 0) consume(response.flip());
}
```

`Zmux.connectTls(address, TlsOptions.client(sslContext, serverName), config)`
verifies the server name and negotiates the ALPN list of `ZmuxConfig.alpn()`.
`Zmux.open(pipeIo, Role.CLIENT | SERVER | AUTO, config)` accepts external
byte-stream or message carriers (`AUTO` negotiates the two roles over a carrier
without transport roles). `Zmux.connectAsync` and `connectTlsAsync` return a
`ZmuxFuture`; `Zmux.dialTlsPipe` and `dialTlsPipeAsync` dial a TLS carrier as a
`PipeIo`, for `Zmux.open` or `addPipe`. `io.zmux.carrier.PipeIo` describes
separate receiving and sending halves (`PipeReader`, `PipeWriter`) and their
`Framing`; `PipeChannel` presents one as blocking channels and builds one from
channels, and `MemoryPipe` supplies an in-memory pair.

A server uses `Zmux.listen(address[, listenerConfig])`; `ListenerConfig.tls`
set to `TlsOptions.server(sslContext)` accepts TLS. Its `accept()` returns an
established session. `openStream` and `acceptStream` handle
bidirectional streams; `openSendStream` and `acceptReceiveStream` handle
unidirectional streams. Reads and writes of a stream are independent; concurrent
reads (or writes) of one stream are served in FIFO order; both have optional
timeouts. `finish()` sends FIN (the clean end); `reset(code)` resets the sender,
and `stop(code)` stops receiving. A stream is `AutoCloseable`: `close()` leaves a
finished sending part and a receiving part read to its end as they are, so
`write`, `finish`, read the reply, `close` (the end of try-with-resources) ends
the stream cleanly. **Closing a part left open ends it abruptly**: a sending
part not finished is reset with ZMUX_CANCELLED (0x5a4d0001), so the peer sees
an abandoned stream and never a clean end, and a receiving part not read to its
end is stopped with code 0. Unlike `Socket.close()`, `close()` does not flush or
send FIN, so call `finish()` after the last write; a handle the garbage
collector finds unclosed is released the same way. A write returns once its bytes are in the
stream's send buffer; a STOP_SENDING, reset or session end racing it can drop
them, and the next write or `finish()` reports that error (as TCP reports an
RST). `finish()` waits for the session to take the FIN, so it is the point where
every earlier write's fate is known. `tryWrite` returns 0 while another write
owns the stream. `readAll(maxBytes)` reads to FIN; past `maxBytes` it stops
the stream and throws `LimitExceededException`. A preamble that would exceed
the peer's maximum preamble size is refused when it is set
(`PreambleTooLargeException`). Setters that cannot fail
(`setPriority`, `setSendBuffer`, `setReceiveWindow`, ...) validate their
arguments and are applied without waiting; `setPeerPriority` waits and throws
when the peer did not negotiate priorities. Of the `java.io` views, closing
`outputStream()` sends FIN and closing `inputStream()` stops only the
receiving part. Session `close(code, reason)` closes gracefully with an
application error code and reason, which the peer sees as
`ApplicationClosed(code, reason, remote)`; `close()` (`AutoCloseable`) is
`close(0, "")`. The data the streams have written is sent first, as far as
flow control and the carriers allow, so it reaches the peer ahead of the code,
then CONNECTION_CLOSE is handed to the carrier of every usable outbound pipe.
It returns once that is done, without waiting for the peer or for open
streams (they fail locally) and after one second at most (what has not been
handed over by then is discarded); each pipe is then shut down and drained in
the background until the peer ends it or the linger timeout passes, so a
process may exit right after it. `abort(code, reason)` ends the session at once
with an application error and discards what is unsent. The first call of
either decides the code, the reason and what is sent; a later call sends
nothing more, except that an abort during a close's flush ends the flush at
once. A reason is cut to 1024 bytes of UTF-8. `closed()` is a `ZmuxFuture`
completed with the session's `SessionError` at its whole end: once every
pipe's inbound side has ended (the peer read the close and ended its side) or
the linger timeout has expired, and the closed carriers were released; when it
completes before the linger timeout, the peer has read the close. To wait for
"delivered or given up after d", bound the wait itself: `closed().get(d, unit)`
or `orTimeout`; the timeout changes nothing in the session. After the peer's
close, the data that arrived before it stays readable on the streams the
application opened or accepted, until it is read or the linger timeout passes. `awaitClosed()` (and
`awaitClosed(Duration)`) waits for the same end. `shutdown(code, reason)` closes
gracefully (GOAWAY, then CONNECTION_CLOSE with code and reason once no stream
remains); `shutdown(code, reason, Duration)` closes at once when its bound
passes, and `shutdownAsync(code, reason)` returns a `ZmuxFuture<Void>` whose
`orTimeout` is that bound.

Every blocking wait (`openStream`, `openSendStream`, `acceptStream`,
`acceptReceiveStream`, `receiveDatagram`, `datagrams().receive`,
`datagrams().sendWait`, `ping`, `nextEvent`, `awaitSendCapacity`,
`awaitPeerAttached`, `awaitClosed`, `listener.accept`) has a form without a bound
and a `Duration` overload that throws `SocketTimeoutException` when the bound
passes; no wait takes `null` for "no bound", and none returns `null` on a
timeout. The TCP dial of a session is bounded by the handshake timeout, that of
an added pipe by the join timeout. A session's end is never a timeout: an idle,
handshake or resume timeout ends the session, and every later call throws
`SessionClosedException`.

Streams expose receive-window/send-buffer controls, send-capacity waiting,
preambles and RFC 9218 priorities (`setPriority(Priority)` and `setPeerPriority`).
`session.stats()` returns counters. `ping()` measures a round trip;
`nextEvent()` waits for a session event.

## Owned chunks and nested sessions

`readChunk(max)` returns an owned, read-only `Chunk`, or null at FIN. Close
it after use, or pass it to `writeChunk(chunk)` on any sending stream. A
write takes ownership on success, failure and interruption. `writeChunk(buffer)`
takes ownership of the buffer's remaining bytes; do not mutate them afterwards.
A chunk held by the application remains valid after the source session closes.
`Chunk.of(buffer, release)` supports pooled buffers; release is resource cleanup
and must not block, as the last reference may be dropped on an event loop.
A chunk that is neither closed nor transferred keeps its buffer until it is.
As a safety net one chunk in 128 is watched: when the garbage collector finds a
watched chunk dropped, zmux logs a warning and releases it.
`-Dzmux.leakDetection=paranoid` watches every chunk, with the stack that created
it; `=off` watches none.

`session.openStreamAsPipe(info, tlvs)` (OPEN_INFO and application TLVs, both optional) opens a bidirectional stream marked as
PIPE_CARRIER (when the preamble extension is in use) and returns it as a
`PipeIo`; pass that to `Zmux.open` or a listener to run another session over
the stream. `stream.asPipe()` converts a stream opened otherwise.
The conversion transfers stream ownership: use the returned carrier, not the
original stream's read/write methods. No forwarding thread blocks on stream I/O;
the new session inherits the outer loop, while its records and payloads use owned
buffers. Locally visible nesting cycles are rejected. Closing the inner session
leaves the outer session usable. Automatic nested lanes and relay helpers are
not yet exposed.

## Multiple pipes

Multipath and resume are settings of `ZmuxConfig.Builder` like any other:

```java
ZmuxConfig config = ZmuxConfig.builder()
        .maxPipes(4)                                         // multipath: up to 4 pipes of our own
        .pipeAuth(PipeAuth.psk(secret).withEndToEnd())       // the join secret, and pipe 0 is end to end
        .build();
ZmuxSession session = Zmux.connect(serverAddress, config);   // the server: Zmux.listen(address, ListenerConfig.of(config))
ZmuxPipe second = session.addPipe(otherServerAddress, PipeOptions.defaults());
for (ZmuxPipe p : session.pipes()) System.out.println(p.id() + " " + p.stats());
```

Multipath and resume need a secret source: `PipeAuth.psk(secret)` (or
`PipeAuth.deferred()`, supplied later with `session.setJoinSecret`),
`PipeAuth.exporter()` (the exporter of the innermost end-to-end handshake, today
TLS that this process terminates), or both (`PipeAuth.exporter().withPsk(secret)`);
a session whose first pipe is on the secure carrier takes its secret from the
Establish and leaves `pipeAuth` unset. Without a source the session fails to
start with `IllegalArgumentException`, before any byte is written, and so does a
key or the exporter on a secure first pipe (`PipeAuth.none().withEndToEnd()` is
accepted there without effect, so that one configuration serves both kinds of
first pipe). A session has no join keys until the host states that its first
pipe is end to end: `PipeAuth.withEndToEnd()` when known before it starts, else
`session.adapter().declareEndToEnd()` once proven; until then local joins fail with
`JoinPendingException`, whose message names what is missing (the declaration,
the deferred PSK or both), and the peer's are held. A server session needs a
listener for either feature: `Zmux.open(io, Role.SERVER, config)` with
`maxPipes` above 1 or a `resumeTimeout` is a configuration error.

The secure carrier (`io.zmux.seal`) runs an Establish on a session's first
carrier. The server installs it as the listener's inbound transform; the client
runs it on its carrier and opens the session on the result:

```java
ServerConfig server = ServerConfig.builder(gate, StaticKey.generate()).build();
ListenerConfig lc = ListenerConfig.builder().session(config).inboundTransform(Seal.server(server)).build();

ClientConfig client = ClientConfig.builder()
        .anchor(new PinAnchor(KeyId.of(KeyAlg.X25519, nodePublicKey)))
        .psk(handle, psk)
        .handshakeTimeout(Duration.ofSeconds(5))   // 10 s unless set; noHandshakeTimeout() for none
        .build();
PipeIo io = Seal.establish(carrier, client).get();     // a ZmuxFuture
ZmuxSession session = Zmux.open(io, Role.CLIENT, config);
Peer peer = Seal.peerOf(io).orElseThrow();
```

Both configurations bound the application data a peer sends in the handshake
(`maxAppData(bytes)`, 256 unless set, or `noMaxAppData()`). Failures of the
protocol are `SecureCarrierException`, a `ZmuxException` carrying the error
class.

`session.addPipe(address, options)` joins another TCP carrier and waits
until the peer has attached it (`PipeOptions.Builder.returnOnReply(true)`
returns at the verified Reply; `ZmuxPipe.awaitPeerAttached` waits later),
within the join timeout (`ZmuxConfig.Builder.joinTimeout`, 10 s by default,
at most 60 s, independent of the handshake timeout); `addTlsPipe` and `addPipe(pipeIo, options)`
for external carriers are available, and a pipe input handed to zmux is zmux's
to close whatever the outcome. A join the peer refuses for lack of room fails
with `JoinRefusedException` and starts the join refusal backoff. A `ZmuxPipe`
is identified by a `PipeId`. `ZmuxPipe.update(PipeUpdate)` changes its
settings together: send priority and standby (the local outbound direction),
receive priority and standby (the request for the peer's outbound direction),
weight, probation, `PipeLiveness` and the pipe's own `QualityBand`; a setting is
kept when its methods are not called, set with `x(value)` or cleared with
`clearX()`. `update` is the only way to change them; `PipeOptions.Builder` takes
the same settings with the same methods when the pipe is added, where keeping
and clearing both mean the default. `pipe.setShaper(shaper)` replaces the
pipe's own shaper. A pipe can also revalidate, retire gracefully (`close()`) or
`abort()`; `closed()` (a `ZmuxFuture`) and `awaitClosed()` wait until its
removal finished. Lower priority numbers are preferred. The local outbound override takes precedence over the
peer's request; clearing it follows the peer again. Standby means failover,
not ordinary overflow. The priority field spans the full wire varint62 range.

The join keys evolve in session epochs: a multipath session exchanges a new
epoch every hour or every 65536 joins (`ZmuxConfig.Builder.epochPeriod`,
`epochJoins`; `noEpochPeriod()` and `noEpochJoins()` turn either off), or when
`session.advanceEpoch()` asks; a session whose secret comes from a pre-shared
key or an exporter also advances once when its first Sealed or Bound pipe exists
(`epochAfterFirstPipe(boolean)`, on by default). `EpochCommitted` and
`EpochDeleted` report it, and `session.replacePipes()` also replaces every
carrier through the redial hook, make before break; `orTimeout` on its future
bounds it.

### Resume after a lost connection

`resumeTimeout(d)` keeps a session through the loss of all its pipes for the
grace period `d`. Something has to dial a new pipe in that time: a `redial`
hook for a session that runs on one pipe at a time, or a pool
(`session.maintainPipes`, below) for one that keeps several.

```java
ZmuxConfig config = ZmuxConfig.builder()
        .pipeAuth(PipeAuth.psk(secret).withEndToEnd())
        .resumeTimeout(Duration.ofSeconds(30))
        .redial((need, cancellation) -> {                    // any PipeIo; zmux owns it
            SocketChannel ch = SocketChannel.open(serverAddress);
            return PipeChannel.pipe(ch, ch, Framing.BYTE_STREAM, 1 << 16);
        })
        .build();
try (ZmuxSession session = Zmux.connect(serverAddress, config)) {
    // Streams survive a lost connection: writes buffer and block, reads wait, until a redial resumes it.
}
```

The hook is called as soon as the session suspends or lacks a direction, and
again after a failure with an exponential backoff (`redialBackoff(Backoff)`,
by default from 1 s to 60 s with jitter 0.2), measured from the start of the
previous attempt; a join refused by the peer also waits for the refusal
backoff, so a dial waits for the longer of the two. The server needs a
listener (`Zmux.listen`), which holds the suspended session. `nextEvent()`
reports `SessionSuspended` and `SessionResumed`; a session that can rejoin and
suspends with neither a hook nor a running pool also reports
`ResumeUnattended` (and logs a warning through `ZmuxConfig.Builder.logger`, a
`System.Logger`), since nothing will dial for it unless the application calls
`addPipe` itself.

### Telling session ends apart

```java
try {
    stream.write(data);
} catch (SessionClosedException e) {
    switch (e.error()) {
        case SessionError.ApplicationClosed a when a.remote() -> log("peer closed: " + a.code() + " " + a.reason());
        case SessionError.ResumeTimeout t -> log("no pipe came back in time", e.getCause()); // the last pipe's error
        case SessionError.PipesLost l -> log("every pipe lost", e.getCause());
        case SessionError.ResumeFailed f -> log("resume could not continue safely");
        default -> log(e.error().message());
    }
} catch (StreamResetException e) {
    log("the peer reset the stream: " + e.code());
}
```

`SessionError` is sealed: `ApplicationClosed`, `TransportClosed`,
`IdleTimeout`, `HandshakeTimeout`, `EarlyDataRejected`, `ResumeTimeout`,
`ResumeFailed` and `PipesLost`. For `ResumeTimeout` and `PipesLost` the
exception's cause is the error of the last pipe lost, for example a join the
peer refused while the session tried to resume.

Each joined carrier has a record mode. `RecordMode.SEALED`, the default, protects
records with the keys of the Join handshake; `BOUND` leaves them to a carrier that
declares the contract (`PipeIo.withSecure(true)`, and TLS this process terminates) and binds
its exporter into the handshake; `TRUSTED` leaves them to the deployment.
`ZmuxConfig.Builder.recordPolicy` or `PipeOptions.Builder.recordMode` chooses the mode of a
local join; `allowRecordMode` is the acceptor's check and when unset only Sealed passes;
`authorizeJoin` may refuse a join after it. `session.weakestRecordMode()` and
`PipeStats.recordMode()` report the result, and `joinEarlyHello` sizes the first batch
a Hello carries while no other pipe is usable (default 8 KiB; `noJoinEarlyHello()` turns it
off). A join exchanges its keys in a key group, 1 or 2 (`PipeOptions.Builder.keyGroup`; by
default 1 when the session can use it).

The optional `zmux-http` module provides HTTP and WebSocket carriers (see
[HTTP carriers](#http-carriers)). Other integrations (an HTTP stack of the
application's own, HTTP/3, WebTransport) use the same external `PipeIo`
contract. A stream of an HTTP/3 request or a WebTransport session can read the
exporter of its QUIC connection, so such a pipe declares the contract with
`withExporter(...)` over the connection's exporter and `withSecure(true)`: the Binding names
the connection, not the stream. The application then owns HTTP routing, authentication and the HTTP connection;
the supplied pipe halves follow `PipeIo` ownership and completion rules.
Keep `carrierConn` identical for streams sharing one underlying connection,
and preserve it when wrapping a pipe. `PipeWriter.write(OutboundBatch, Completion)`
receives the records of one write as read-only segment views plus record
boundaries (`messageCount()`, `messageEnd(m)`): a byte-stream writer gathers all
segments, a message writer sends each record's segments as one message. zmux
copies nothing for the writer. The batch is valid until the completion is
reported; completion means the carrier took the bytes, not that the peer
acknowledged them. A writer that keeps the payload after completing calls
`batch.retain()` first and closes the returned handle when done: one handle per
write, no copy. Writes outside zmux (a fallback target) use `OutboundBatch.of(buffers)`.
A writer that can tell how many bytes it took but has not yet transmitted (its own
buffer, or the socket's unsent count) overrides `PipeWriter.unsent()`
(`OptionalLong`, empty by default). The driver queries it on the event loop,
outside the session, when it hands over a batch and when it takes statistics, so
it must not block. The count is reported as the pipe's `queuedBytes` and is part
of the pipe's queue Q_p (the batch whose write is in progress plus that count),
which widens the suspicion and failure timeouts by min(1 s, queue / delivery
rate). The bundled TCP and TLS carriers report none: a write completes only once
the socket took every byte, and the JDK offers no query for the kernel's unsent
bytes.
On the receiving side `PipeReader.Sink.onData(buffer, release)` lends a batch:
zmux runs `release` once it has copied or parsed it, and a reader delivers more
before earlier batches are released only within the `receiveMemory` it declares
in `PipeIo.Capabilities` (`Capabilities.builder().receiveMemory(bytes)`, or
`PipeIo.withReceiveMemory`; charged to the memory budget); a reader that queues
beyond that fails its carrier. `onChunk` transfers owned buffers: zmux parses
them in place, keeps payload references instead of copying, and copies only the
part of a record cut at the chunk's end.
An HTTP implementation must support
concurrent reads and writes (or use split requests), forward cancellation and
failure, and keep response/request bodies alive for the pipe lifetime.

External HTTP handlers pass `JoinHint.inBand(source)` to
`listener.adapter().handle(pipeIo, hint)` and keep their request/response bodies
alive until the returned stage completes. `source` is a stable client identity
for admission, usually the remote IP. Malformed, unauthenticated and replayed
joins are closed through the same protocol admission path.

When no bidirectional bootstrap carrier exists, `PipeIo.split(uplink, downlink)`
combines complementary halves, in initiator uplink/downlink order. Send the
same fresh 16-byte random token, unpadded base64url, in `JoinHint.PAIR_HEADER`
for both requests; the listener takes `JoinHint.pair(token, uplink, source)`.
The halves must be confidential end to end and use matching framing/ALPN;
the token is a bearer secret and must never be placed in a URL. With exporter
authentication the split helper binds **both** handshakes, in that order.
Pairing limits and timeouts apply before a session exists, and the token
stays used until that session ends. Waiters hold one parsed first record and
bounded unparsed read-batch input, releasing both halves on failure or timeout.

The built-in `Scheduler` constants are `AFFINITY`, `SESSION_AFFINITY`,
`SPREAD_ETA`, `SPREAD_ROUND_ROBIN` and `SPREAD_RATE_WEIGHTED`. Set a policy in
configuration (`ZmuxConfig.Builder.scheduler`) or change it with `session.setScheduler`; a `ZmuxStream`
can override its own sending policy and redundancy (`setScheduler`, `setRedundant`; `clearScheduler` and
`clearRedundant` follow the session again). `Scheduler.custom(picker)` receives immutable request
and pipe values and returns pipe indices with byte allowances, or a wait for
new input. Pickers run synchronously on the session's event loop: they must be
fast and must never block. Failed, missing, invalid or stale answers
use the core's affinity fallback.

`pipe.stats()` and `session.pipeStats()` return immutable snapshots
(`pipe.stats()` and `pipe.state()` return an `Optional`, empty once the pipe is
retired; operations on a retired pipe throw `PipeGoneException`). A
`PipeStats` holds the pipe's states, settings (priorities, standby, weight and
their `SettingSource`), health (suspicion, probation, revalidation), RTT
estimates and their `RttSource`, sending state (eligibility, queued and
in-flight bytes, delivery rate), traffic and reinjection counters, and
`sinceLastTraffic()` (time since a record with stream, reset, stop-sending or
datagram frames crossed the pipe; probes, acknowledgements and padding do not
count). Loss is reported as `bytesLost` (sent but unacknowledged when the pipe
failed), `recordsLost` (that in records, estimated from the pipe's mean record
size), `bytesRetransmitted` (the carrier's own retransmissions, where its
`PipeWriter.retransmitted()` reports them, as QUIC does) and `lossRate`
(smoothed, after the first samples a second apart); each is empty where it
cannot be known. Durations use `Duration`, rates bytes/s. Estimates
are observations, not guaranteed bandwidth. Quality bands and capacity floors
are opt-in through `session.setQualityBand` (a pipe's own band through
`PipeUpdate.qualityBand`) and `setCapacityFloor` (an integer rate in bytes/s,
as every rate zmux reports); reports appear in
`nextEvent()`. Tier saturation and effective-setting changes are events too.
Configuration also exposes spread size, overflow delay, redundancy and the
receive auto-tuner's latency budget.

The settings that judge whether a session and its pipes are alive are one
`Liveness` (`ZmuxConfig.Builder.liveness`; `session.liveness()` reads the settings
in effect and `session.setLiveness` replaces the whole object while the session
runs): the keep-alive period, the suspicion floor, the dead timeout, the standby
probe interval and the idle probe interval and jitter. A pipe has its own
`PipeLiveness` (suspicion floor, dead timeout, probe interval and jitter) in
`PipeOptions.Builder.liveness`, `PipeUpdate.Builder.liveness` and a pool
decision's `withLiveness`; a setting it does not write follows the session's.

The idle probe interval (`probeInterval`, default 15 s, at least 1 s) is measured
to the carrier: each probe waits a uniform draw from [k_eff(1 - j), k_eff] with
k_eff = max(1 s, k - the pipe's longest shaping hold) and j the probe jitter
(`probeJitter`, default 0.25), and a bootstrap pipe's first wait from [0, k_eff];
j = 0 gives a fixed schedule. A probe is answered on its own pipe pair (in a
quiet session only there), with a probe of the answering end once its pipe has
been idle for max(1 s, T_suspect). `noProbeInterval()` turns off only the idle
probes (of a session, or of one pipe through `PipeLiveness`), and `noStandbyProbe()`
the faster probes of standby pipes (`standbyProbe`, default 1 s). Probes keep
pipes and their carriers in use, never the session.

Keep-alive (`Liveness.Builder.keepAlivePeriod`) is on by default at 15 s, capped at
half the effective idle timeout and jittered by the probe jitter; the idle timer
counts only the peer's evidence, so an idle session stays open while the peer
answers and closes within the idle timeout once its carrier dies. `noKeepAlive()`
turns it off and makes the idle timeout a limit on application inactivity; a set
period is at least 1 s. The configuration is flat: one builder method per
setting. A setting that is not a value has a `noX()` method of its own and reads
back as empty: `noHandshakeTimeout()`, `noIdleTimeout()` (no idle timeout is
announced; a set `maxIdleTimeout` is at least 1 ms), `noProbation()`,
`noEpochPeriod()`, `noEpochJoins()` and `noJoinEarlyHello()`. An unlimited budget
is `ZmuxConfig.Builder.noMemoryBudget()` or `ListenerConfig.Builder.noMemoryBudget()`
(a budget of 0 lets nothing grow beyond the floors).

## Application hooks and budgets

`ZmuxConfig.Builder.tracer(tracer, queueCapacity)` installs an ordered,
asynchronous `Tracer`. A slow tracer drops events and reports the loss
count; its terminal notification is retained. Window growth can be approved
by `allowConnectionWindowIncrease`; the predicate runs off-loop and stale or
now-unaffordable answers cannot increase credit.

`session.setShaper(factory)` and `ZmuxConfig.Builder.shaper(factory)` install a
`ShaperFactory`, within `ZmuxConfig.Builder.shapingLimits` and `shapingBudget`.
`ShapingLimits` bounds what a shaper may do: its maximum delay
(`withMaxDelay`, or `withNoMaxDelay()` for none) and whether it may pad
(`withPadding`), pause (`withPause`) or inject records (`withInjection`).

`PipeIo.carrier` (set with `withCarrier`) is the single declaration of a pipe's
carrier kind. The bundled adapters fill it on both sides: HTTP streaming
requests are `STREAMING`, both halves of a split bootstrap or a joined half are
`SPLIT`, the exchange carrier is `EXCHANGE`, WebSocket is `WEBSOCKET` and QUIC
streams are `QUIC`; a `Candidate`'s carriers take the candidate's kind. Raw TCP
and TLS carriers, and pipe inputs that leave it unset, have none (null). The
kind is reported in `ShapedPipe`, `PipeStats.carrier` and the pipe events
(`PipeAdded`, `PipeJoined`, `PipeSuspect`, `PipeRecovered`, `PipeFailed`, `PipeClosed`,
each with the direction it is about, `PipePriorityChanged`, with the source of the
change, and `PipeQualityChanged`). Dropped datagrams are counted in the statistics,
not reported as events. Retirement has one report only,
`PipesRetired(retired, failed, closed, dropped, last)`: the pipes retired since it
was last read, of those the ones whose failure or end was still unread, the unread
events of retired pipes that left the queue, and the last four retired IDs. It
accumulates until read, so the event queue stays bounded however many pipes a
session goes through; compare `session.pipes()` with the handles you hold to find
every retired ID.

`io.zmux.helper.Limiter` supplies asynchronous `ready`, immediate `tryReady`
and `charge`. Readiness reserves nothing. `LimitedStreams.input/output`
wrap application streams, wait outside the event loop, and charge bytes
actually forwarded, including partial interrupted writes. Datagram limiters
are installed with `datagramLimiters(send, receive)`: they admit a whole
payload or drop it. Received datagrams waiting for a hook remain within the
same bounded receive queue and cannot return after eviction.

A stream's datagram queue is bounded per stream and direction by a count
(`datagramStreamQueue`, default 32) and by payload bytes
(`datagramStreamBytes`, default 512 KiB; `noDatagramStreamBytes()` leaves the
count alone): a datagram that would exceed either drops the oldest datagrams of
that queue until both hold, each counted as dropped, and a single datagram
larger than the byte bound is kept alone. `datagramReceiveBytes` bounds the
session queue and the datagrams held for streams not opened yet; stream queues
do not count against it.

`Zmux.MAX_STREAM_CREDIT` (2^60) is the largest stream credit, the value that
grants the peer as many streams as the protocol can count, for
`incomingStreams`, `incomingUniStreams` and `raiseIncomingStreams`; a larger
value is a configuration error.

`session.datagrams().send(payload)` does not wait for send-queue space and
returns false when the datagram limiter dropped the payload; `send(payload, timeout)` can wait
for that space with a deadline, but never waits for limiter tokens. Enable
datagrams in both peers' configurations before using them.

`ListenerConfig` sets admission, instance-ID, memory and suspended-session
limits, and the bounds of carriers before a session: `maxPending` (default
1024, `noMaxPending()` for none) for the whole listener and
`maxPendingPerSource` (default 8) per source. A source is counted by its
prefix: `sourcePrefixV4` (1 to 32, default 32, the address on its own) and
`sourcePrefixV6` (1 to 128, default 64), ports ignored, an IPv4-mapped IPv6
address as IPv4; a value outside its range is a configuration error. The same
aggregation serves everything the listener keeps per source: the per-source
slots, the count of a source's stalled carriers and the stall duration. A
shorter prefix groups more clients into one share. A carrier holds a slot of both
from its arrival, through its handshake, admission, transform, pairing,
classification, join verification or new session's parameters, and any stall,
until it enters a session, is closed or goes to a fallback. When the listener's
slots are all taken, a new carrier closes the oldest stalled carrier, or is
closed itself without a byte when none is stalled (counted `rejected`);
stalled carriers hold at most half of `maxPending`. A shared budget stops new growth/admission under pressure; suspended
sessions holding the most memory are evicted first. `maxSuspended` evicts the
oldest suspension when exceeded. `memoryPressureListener` reports pressure
entry/recovery on a serial callback context. Existing advertised credit is
never withdrawn.

## Carrier admission

The `Admission` of `ListenerConfig` decides on every new carrier of that
listener before anything is written on it: its TCP and TLS carriers, those
given to `listener.adapter().handle(...)` (the HTTP, WebSocket and exchange carriers of
an `HttpEndpoint`, a host's own pipes), and QUIC connections by their first
bidirectional stream (the connection's other streams wait for it and then
reach the listener without a new decision), joins and resumptions included.
A carrier first takes a slot of its source (`maxPendingPerSource`, counted by
its source prefix) and one of the listener (`maxPending`), then completes its TLS handshake, then the
decider gets up to `peek` bytes and the carrier's `Admission.CarrierInfo`: its
source and the peer it arrived from, the negotiated application protocol, the
server name (SNI) the client sent, the TLS exporter, and for HTTP carriers the
request's header fields (`info.header(name)`, case-insensitive) and whether
the carrier is decided on its request head. It returns a `CompletionStage` of
a decision whose `AdmissionVerdict` is `ACCEPT` (skipping a prefix), `FALLBACK`,
`STALL`, `CLOSE` or `MORE` (a longer prefix), due within the peek
timeout (`Admission.Deadline`); a stage still pending then, or when the
listener closes, is cancelled and the carrier closed.
`Admission.Decider.immediate(...)` wraps a decision taken at once, and
`PrefixAuth` provides a token decider. `listener.stats().admission()` counts
the outcomes.

```java
var auth = new PrefixAuth(key, Duration.ofSeconds(30), true, 1, 0);
var config = ListenerConfig.builder()
        .admission(Admission.of(PrefixAuth.TOKEN_LENGTH, auth.verifier()))
        .fallback((carrier, info) -> serveOtherProtocol(carrier))
        .build();
```

A fallback target receives the carrier as a byte-stream `PipeIo` with every
byte read replayed in front. `listener.adapter().handle(io, hint, fallback)`
takes its own target and returns an `io.zmux.adapter.Handled` with the verdict and whether a session
took the carrier (`taken()`: false when the listener refused it, having
written nothing), and `QuicServer` takes one for QUIC connections. Without a
target a fallback stalls: nothing is sent, input is discarded, and the carrier
closes after a duration that depends on its source alone and keeps its slot
until then, or when the listener closes; a source keeps one slot free of
stalls.

`Admission.Decision.accept(skip, value)` attaches a host value to the carrier:
`session.admitted()` returns the value of the carrier the session started on
(the uplink's for a split bootstrap), and `pipe.admitted()` that of the carrier
that carried each pipe's join. zmux does not compare the two; a host that
requires a join to come from the session's own client compares them.

`ListenerConfig.Builder.inboundTransform(transform)` runs once for every
carrier the listener takes, after its admission and before any session
processing: it gets the carrier as a `PipeIo` (with the bytes the admission
read and did not skip in front) and an `InboundTransform.Context` (the hint,
the admission value and the deadline), and returns a stage of the pipe input to
continue with, such as a layer of the host's own over the carrier. It must not
block; the carrier keeps its source slot meanwhile, and the session's handshake
timeout bounds the stage. The result keeps the carrier's halves and framing,
and a layer maps each of its writes to exactly one carrier write.
`io.zmux.carrier.PipeChannel` presents a pipe input as blocking channels and
`PipeChannel.pipe(in, out, framing, readSize)` turns channels back into a
`PipeIo`, both one write per write. `PipeIo.withWriteOverhead(bytes)` declares
what a layer adds to each carrier write (at most 65535); `ShapedPipe` reports
it, and the session counts it in the record lengths a shaper plans, which are
lengths on the carrier.

`ListenerConfig.Builder.onReject` answers a rejected Hello (`RejectInfo`: its mode and
a local reason: `malformed`, `pending_limit` (the source is over its bound of carriers
waiting to authenticate), `unknown_locator`, `unknown_suite` (not usable in the found
session), `binding_unavailable`, `memory_pressure` or `auth`) with `RejectInfo.Action.CLOSE`, the default, or
`STALL`, which holds the carrier silently; nothing is written either way. Every
refusal before authentication gets this one answer, so the response and the way the
carrier ends are the same whatever the reason; the time spent before the refusal may
differ.

### Carriers from an HTTP server

An application that runs its own HTTP server gives each request carrier to
`listener.adapter().handle(io, hint, fallback)`. What only such an adapter or
an own accept loop needs (`reserve` and the `Slot` it returns, `handle`,
`knownJoin`, `stallDuration`, `countSetupFailure`, `firstRecordTimeout`,
`memoryUsed`) is in `io.zmux.adapter.ListenerAdapter`, reached through
`listener.adapter()`; a session's counterpart (`isJoinReady`,
`declareEndToEnd`, `releaseFirstFlight`) is
`session.adapter()`. The `JoinHint` carries the request:
`JoinHint.of(RequestHead.of(headers, head), uplink, peer)` takes the
`Zmux-Pipe-Pair` field from the headers (`JoinHint.of(head, pairField, uplink, peer)` another field), and `head` is
true for a carrier decided on its request head: an HTTP upgrade (WebSocket,
WebTransport) or a request without a body. Such a carrier is decided at once,
without reading a prefix. `withSource(address)` gives the client address the
host resolved itself (the peer otherwise); with `HttpEndpoint`, a host sets
`HttpEndpoint.SOURCE` on the connection's channel instead. The pipe's
capabilities supply the TLS metadata of the connection that carries the
request: `withExporter`, `withProtocol` and `withServerName`.

`listener.shutdown(code, reason)` refuses new sessions at once
(`accept()` throws `ListenerClosedException`), shuts every session of the
listener down with code and reason, and lets them join and resume until they
have all ended; then the listener closes. `shutdown(code, reason, Duration)`
closes the sessions still running when its bound passes, and
`shutdownAsync(code, reason)` returns a `ZmuxFuture<Void>` whose `orTimeout` is
that bound. `close()` stops the listener at once and leaves started sessions
running, without new pipes from it.

On a carrier decided on its head, `PrefixAuth`'s verifier takes the token
from the request field `Zmux-Prefix-Auth` (`PrefixAuth.withHeaderField` agrees
on another name), 43 characters of base64url without padding; a missing,
repeated or malformed field fails like a wrong token, and the token is bound
to the exporter of the connection carrying the request.

### Client credentials

A `CarrierCredential` is what a client presents to a server's admission: the bytes a
carrier starts with (`prefix(info)`), or the request header field of a carrier
decided on its head (`header(info)`). `PrefixAuth` is one; it presents a fresh
token on every carrier, bound to the carrier's TLS exporter with `bindTls`.

```java
var client = ZmuxConfig.builder().carrierCredential(auth).build();
ZmuxSession session = Zmux.connectTls(address, TlsOptions.client(trust, "example.com"), client);
```

Session dialers take `ZmuxConfig.carrierCredential()`: `Zmux.connect`,
`Zmux.connectTls`, `Zmux.dialTlsPipe` and their `Async` forms, `listener.connect`
and `listener.connectTls` (with the listener's session configuration), and
`QuicClient.connect` and `connectAsync`; so do the TCP
and TLS pipes a session dials itself (`addPipe(address, ...)`,
`addTlsPipe(...)`). The prefix goes after the TLS handshake and before
the first zmux record. Pipe dialers take one
per call: `QuicClient.dial(address, serverName, session, credential)` (and `dialAsync`) and
`new QuicPool(client, address, serverName, limits, credential)`, which present
it on the first stream of each connection they open. A pipe the application
dials itself (`Zmux.open`, `listener.open`, `addPipe(io, ...)`) presents
its credential where it is dialed: the prefix written first, or
`credential.header(info)` in its HTTP request.

## Carrier selection

`Zmux.dial(choice, config, policy)` dials a client session over the carrier a
`CarrierChoice` selects. Each adapter offers a `Candidate` of its kind (`CarrierKind`:
websocket, streaming, split, exchange, quic, http3, webtransport) wrapping its
own dialer. `CarrierChoice.explicit(candidate)` uses that kind only and reports
a failure as a `CarrierChoiceException`, never replacing it. `CarrierChoice.auto(...)`
chooses among candidates in order of preference: without early data and
without racing, skipping kinds the `CarrierCache` holds as failed. A candidate
is usable when the peer's parameters arrive within its timer; one that misses
it is closed without CONNECTION_CLOSE. When the configuration allows more
than one pipe and the peer offers multipath, the session is established on
the most robust candidate (a kind remembered as usable, else the exchange
carrier, else the last) and a pool of one member over the same choice
(`PipePool.choice`) takes the bootstrap pipe: it upgrades to a preferred kind
make before break, retiring the old member once the new one has settled, and
replaces a lost one. `PipePool.choice(choice, policy)` serves further pools the
same way; pools may share a cache. `Candidate.wrapDial` wraps a candidate's
carriers. `CarrierChoicePolicy`
holds the cache and its key (server and local network), the timer
(`clamp(3 x open RTT, 1 s, 3 s)`), the settle time (1 s), the drop window
(30 s) and whether the session joins further pipes (`withJoin(boolean)`); `CarrierCache` keeps a usable kind for 6 h and backs a
failed one off from 1 min to 6 h. `cache.snapshot()` and `cache.restore(...)`
carry it across runs (`CarrierCache.Snapshot.toJson()`/`fromJson`, the JSON
form every zmux implementation shares); a restore merges, keeping the newer
entry per key and kind. Every try is reported as a
`SessionEvent.CarrierCandidate` (usable, timed_out, refused, failed), the
choice as a `SessionEvent.CarrierChosen`, and both in `SessionStats.carrier()`
and `carrierCandidates()`.

## HTTP carriers

The optional `zmux-http` module serves and dials HTTP carriers on Netty 4.2
([zmux-http/README.md](zmux-http/README.md)). A runnable minimum, a cleartext
server and a client in one process:

```java
var group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
try (ZmuxListener listener = ZmuxListener.of(ListenerConfig.defaults());
     HttpEndpoint endpoint = HttpEndpoint.builder(listener).streaming().build()) {
    var server = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
            .childHandler(endpoint.initializer(null, null))      // cleartext HTTP/1.1 and h2c
            .bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
    int port = ((InetSocketAddress) server.localAddress()).getPort();

    Thread.ofVirtual().start(() -> {                              // the server: echo one stream
        try (ZmuxSession session = listener.accept(); var stream = session.acceptStream()) {
            stream.write(stream.readAll(1 << 20));
            stream.finish();
            session.awaitClosed();
        } catch (Exception e) {
            // The client ended first.
        }
    });

    try (HttpCarrierClient client = HttpCarrierClient.builder().group(group).build();
         ZmuxSession session = Zmux.open(client.pipe(URI.create("http://127.0.0.1:" + port + "/zmux"), Map.of(),
                 PipeOptions.Direction.BIDIRECTIONAL, null), Role.CLIENT, listener.config());
         var stream = session.openStream()) {
        stream.write("hello".getBytes(StandardCharsets.UTF_8));
        stream.finish();
        System.out.println(new String(stream.readAll(1 << 20), StandardCharsets.UTF_8));
    }
    server.close().sync();
} finally {
    group.shutdownGracefully();
}
```

`HttpEndpoint.builder(listener)` enables each adapter on its own:
`streaming()` (bidirectional requests, split carriers' uplinks and downlinks), `exchange(ExchangeConfig)` (the exchange carrier) and
`webSocket(protocols...)`. Every request is classified on its head
(`HttpClass.classify`); one that is not a carrier, of a class without an
adapter, or given back by its admission goes to the host's handler with every
body byte replayed. `endpoint.handler()` mounts it in an existing pipeline
after the HTTP codec; `endpoint.initializer(ssl, host)` builds a server
pipeline (TLS with ALPN h2/http/1.1, or cleartext HTTP/1.1 and h2c by prior
knowledge, PROXY protocol from `TrustedProxies`). Streaming and WebSocket
carriers pass the listener's admission before anything is written; a request
forwarded from early data is answered 425. Without an admission, a streaming
carrier's response header goes out with the first downlink bytes (an uplink's
once a session took it), and a request the listener does not take (an
unrecognised first record, an unknown join, a full pending queue, a closed
listener) goes to the host's handler with its body replayed.

`HttpCarrierClient` dials them: `pipe(url, headers, direction, credential)`,
`splitPipe(url, headers, credential)`, `exchangePipe(urlTemplate, config)` and
`webSocketPipe(url, headers, protocols, credential)`, each also as a
candidate for `Zmux.dial` (`streamingCandidate`, `splitCandidate`,
`exchangeCandidate`, `webSocketCandidate`). A split bootstrap's pairing makes
pipe pair 0 only, so `addPipe` refuses it (`IllegalArgumentException`);
`splitJoin(session, url, headers, credential, options)` joins a split carrier as
one further pipe: one Join across a paired POST and GET.

Streaming, split and WebSocket carriers follow redirects of their requests on
the same host name only (another path, or a scheme upgrade from http to https,
ws to wss), at most 5 hops (`Redirects.DEFAULT_MAX`); a redirect to another
host, a downgrade, another port without an upgrade, or a POST a 301, 302 or 303
would turn into a GET fails the carrier with `HttpRedirectException`, which a
`CarrierChoice` counts as refused. A streaming request is repeated whole at the new
URL with a fresh credential and the uplink bytes sent so far, kept up to 64 KiB
(`Redirects.MAX_REPLAY`) until the first response that is not a redirect. A
candidate's carriers start at the URL redirects last led it to (`HttpTarget`),
and the choice keeps that URL per server in its `CarrierCache` until the
kind fails there. Every exchange request follows a 307 or 308 within the same
checks, at most 5 hops, its body sent again unchanged; a 301, 302 or 303 of its
POST, or any other refused redirect, fails the carrier with
`HttpRedirectException`. A pure scheme upgrade is kept for the carrier's later
requests; another redirect is followed per request, the next one starting at
the configured URL again.

A split bootstrap (`splitPipe`, `splitCandidate`) holds its first half, with
its slots of `maxPendingPerSource` and `maxPending`, until the other half
arrives or the handshake timeout ends; each half takes slots of its own.
Without an admission, a few sources can keep the listener's slots busy and
delay other clients' split bootstraps; serve a split bootstrap that the open
Internet reaches with an admission, such as `PrefixAuth`.

- **Exchange carrier**: one carrier made of bounded
  POST exchanges, for paths that buffer whole bodies. Both ends share a PSK
  (`ExchangeConfig.builder(key)`) and the body
  encoding (`base64(boolean)`); the default piece size keeps every body at most
  1,000,000 bytes after the encoding. The URL template may hold `{instance}`
  and `{nonce}`; `downUrl` sends DOWN requests elsewhere; `headers` adds fields to every
  request, `cacheBust(false)` stops the nonce query parameter being added to a URL
  without `{nonce}`, and `backoff(Backoff)` sets the wait before a request
  without a result is repeated. Every request
  starts with a fixed 66-byte authentication header (88 characters in
  `base64`), which the server reads and verifies (MAC, time, nonce, declared
  length) within the listener's `firstRecordTimeout` before it reads the rest
  of the body within the handshake timeout; a request whose header does not
  verify goes to the host with the bytes read replayed, and buffers grow with
  the bytes received, not with the declared length. Each request holds a slot
  of `maxPendingPerSource` and of `maxPending` from its arrival until its
  header is authenticated (a request without a free slot is answered 503 with
  an empty body); one that creates a carrier keeps them until the carrier's
  session is established, so `maxPendingPerSource` must be at least 2. A
  request authenticated after its header and then failing (body cut short,
  late, or a Data MAC that does not verify) is answered 400 with an empty
  body and changes nothing. The session's credential is not
  presented. The MACs do not encrypt: a TLS-terminating intermediary can read
  the carrier.
- **WebSocket**: HTTP/1.1 upgrades; each binary
  message is one record's Frames field. The server selects the subprotocol in
  its own order of preference, each identifier also as its draft-01 alias
  (default: the listener's ALPN identifiers); the client offers its
  identifiers, then their aliases, and presents a credential in its header
  form.

## QUIC carriers

The optional [`zmux-quic`](zmux-quic/README.md) module adapts Netty 4.2 QUIC
streams to the same carrier SPI. It supports bootstrap pipes, PSK-authenticated
joins on additional connections, and independent unidirectional lanes in
both directions. `ZmuxListener.of(listenerConfig)` accepts external carriers via
`handle`.
A session over QUIC streams runs on its connection's Netty event loop: it reads
and writes the streams there directly, parsing Netty's read buffers in place,
with no hand-off between threads. A QUIC stream given to a session on another
loop (a TCP-started session joined over QUIC, or a stream wrapped by the
application) works through its `PipeIo` halves instead.
A QUIC connection closes after its last zmux close with that session's code;
Netty's QUIC interface takes a 32-bit code, so a code above 2^31 - 1 closes it
with 0.
Native dependencies stay outside the core. Transport 0-RTT and TLS exporter
binding are unavailable in this adapter; see its setup and ownership rules.

With datagrams enabled on both sides, a datagram that fits the QUIC
connection's datagrams (RFC 9221) goes there instead of in a record, named by
its pipe's stream ID, so that a lost packet loses that datagram instead of
delaying every later one; larger datagrams, shaped pipes and peers without
the channel use records. Several sessions may share a connection's
datagrams. `QuicOptions` negotiates QUIC datagrams by default
(`QuicOptions.builder().datagrams(false)` turns them off), and `SessionStats` counts the
datagrams that went on channels (`datagramsSentChannel`,
`datagramsReceivedChannel`, included in the totals). Only a session on the
connection's own loop uses the channel.

## Build

With JDK 25 or newer and Maven:

```sh
mvn verify
```

Tests include wire corpus/fuzzing, conformance traces, TCP/TLS integration,
concurrent close/cancellation, listener resource policies and multipath
black-hole recovery with payload-integrity checks. The optional QUIC module
also tests real native connections, lanes, join/failover, authentication and
endpoint cancellation.

## Stream relays

`Zmux.splice(inbound, outbound)` transfers both streams to a bidirectional relay
and returns a `ZmuxFuture<SpliceResult>`. Prepare the outbound preamble
first, copying or adjusting application TLVs and PIPE_CARRIER as appropriate.
After transfer, let the relay own stream I/O and release both streams.

Each direction waits for downstream capacity and its optional limiter before
taking a chunk. It holds at most one chunk (64 KiB by default), forwards its
buffer ownership, and preserves independent FIN, RESET and STOP behavior.
Reliable reset tails keep their error code and boundary; a plain QMux peer
receives RESET_STREAM after the forwarded tail has reached the carrier.
Session failure interrupts capacity and limiter waits.

`SpliceOptions` supplies the chunk size and the two direction limiters.
Cancelling the returned future, or completing it from outside (`orTimeout`),
cancels the relay: both streams are reset and stopped with code 0, and there is
no result. Otherwise the future completes after all relay workers, limiter waits,
chunks and owner-loop registrations have been cleaned up, with each direction's
byte counts and how it ended, a sealed `SpliceResult.End`: `Finished`,
`Reset(code)`, `Stopped(code)`, `UpstreamLost` or `DownstreamLost`. A failing
limiter completes the future exceptionally with a `CallbackFailedException`.

Stream datagrams preserve context IDs and use the sessions' existing bounded
queues. They remain eligible after a reliable half-close until the association
ends. A direction limiter's whole-payload rejection is counted as a drop;
accepted datagram bytes count local queue admission, not peer delivery. Shared
limiters must safely support concurrent byte and datagram calls. Helper work uses
virtual threads outside the owner loops, while the existing core remains the
only authority for credit, buffered bytes, stream state and errors.

## Pipe maintenance

A pool maintains pipes for one route without taking ownership of the session.
Use the existing join operations through the attempt scope:

```java
PipePool route = PipePool.of(attempt -> attempt.join(remoteAddress))
    .withCount(2)
    .withOptions(PipeOptions.defaults())
    .withRotation(new PipePool.Rotation(512L * 1024 * 1024,
                                       Duration.ofMinutes(30), 0.2));
PipeMaintenance maintenance = session.maintainPipes(route);
// Later: stop creation and rotation, then wait for all attempt cleanup.
maintenance.close();
PipeMaintenance.Result result = maintenance.completion().join();
```

Multipath must be negotiated. `count` includes only this pool's members, not the
bootstrap or other callers' pipes. The session's `maxPipes` target and admission
limits still apply; leave room for replacements if rotation is enabled.
Probationary members occupy a slot. Recovery takes precedence over rotation,
and the old pipe drains only after its replacement becomes usable.

`withPolicy(PoolPolicy)` is asked on every pass with a `View` of the members
(age, idle time as `sinceLastTraffic`, byte counts, usability), consecutive dial
failures and the time since the last dial. Its `Decision` can replace `count`
(members beyond it close, retired ones first, then the longest idle), replace the
backoff by `dialAfter` (bounded only by the backoff maximum), retire members
(`withRetirePipes`; replaced make before break), ask again no later than
`withRecheck`, and set a `PipeLiveness` for the pipes dialled after it
(`withLiveness`). Backoff and `dialAfter` run from the previous dial attempt. The
pool acts on pipe failures and session suspension as they happen, and keeps
dialling while the session is suspended.
Each item is unset in `Decision.KEEP` (the pool's own behaviour) and set with a
`with` method
(`KEEP.withCount(2).withLiveness(PipeLiveness.builder().noProbeInterval().build())`
turns the idle probes of those pipes off). `KEEP`, or an
exception, keeps the pool's own behaviour. `QuicPool`
takes the same policy for its connections (see `zmux-quic`).

Module `zmux-shaping` reads such hooks from JSON (`Machines`, `Bundle`).

The dial callback runs off the session loop and creates exactly one pipe through
its `Attempt`: `join` or `joinTls`. Return that pipe. The scope reclaims an uncommitted creation
on failure or cancellation without touching unrelated callers' pipes.
`PipePool.carriers` adapts a raw `PipeIo` factory for joins. `PipePool.join(join)` keeps members that are
groups of pipes, such as
`PipePool.join(a -> List.of(client.splitJoin(a.session(), url, headers, cred, a.options())))`:
a member is usable while all its pipes are, counts once, and is redialled,
rotated (make before break) and retired whole. The backoff, or a policy's
`dialAfter` (at most the backoff maximum), runs from the start of the previous
dial attempt, or of the pool; pipe failures, lost directions and suspension make
the pool decide again at once. Preserve the supplied pipe options and use
`attempt.cancellation()` for application connection setup. Callbacks must
cooperate with cancellation and thread interruption: completion waits for their
exit, so a callback that ignores both can delay it indefinitely.

Rotation is disabled by default. Byte and age thresholds, downward jitter,
retry backoff, inspection interval and attempt timeout are configurable. The
inspection interval defaults to 250 ms, retry backoff to 1–60 seconds with 20%
jitter, and attempt timeout to the session's join timeout. `pipes()` publishes
an immutable membership snapshot including pending replacements; its handles
still query the core for current state. Completion reports joins, committed
rotations, failures and retained handles. Closing maintenance or cancelling its
optional `Cancellation` stops background work; committed pipes remain usable
and owned by the session. Session termination also stops maintenance. The stage
completes after the dial worker and removable subscriptions have been cleaned up.

## Sessions that can receive joins

When either endpoint may initiate new paths, create both sides through their
listener endpoint. The listener can accept new sessions and also initiate them:

```java
try (ZmuxListener endpoint = Zmux.listen(localAddress, ListenerConfig.of(config))) {
    ZmuxSession session = endpoint.connect(peerAddress);
    // The peer can now add a pipe to localAddress for this same session.
}
```

`endpoint.connectTls` and `endpoint.open(pipeIo, Role.CLIENT | AUTO)` use
the same bootstrap and drivers as the corresponding `Zmux` entry points. The
listener installs the join locator before the first transport parameters are
sent, registers join/lane routing, and applies its shared memory and suspension
policies to these sessions too. Its `accept()` queue contains only sessions
started by incoming bootstrap carriers. An externally supplied adapter forwards
new incoming carriers to the same `endpoint.handle` entry. Authentication, replay and attachment admission are shared
with accepted sessions.

Ordinary `Zmux.open` or `Zmux.connect` sessions remain suitable when only that
endpoint initiates joins. An established session cannot acquire an incoming
join locator later: use the endpoint creation methods from the beginning.
Keeping the listener alive enables incoming joins; closing it removes routing
and stops future admission, while already handed-out sessions and their attached
carriers remain usable. Closing a session unregisters its routes and resource
tracking. This makes duplicate-session route rejoin possible in either direction
without reusing the losing carrier.

Pooled adapters can set `PipeIo.withFateGroup` to supply their connection's
non-zero varint62 group. Joins adopt it when options leave the group
unset; an explicit application group has priority. All carrier
capability builders preserve this metadata. The optional QUIC module provides
`QuicPool` for connection reuse, creation limits and graceful retirement; see
its [pool ownership rules](zmux-quic/README.md#connection-pools).

## Duplicate sessions

After authenticating that two established sessions connect the same endpoints,
call `Zmux.resolveDuplicate(first, second, options)` at both endpoints. Supply
the same endpoint identity byte strings in opposite local/peer order:

```java
DuplicateOptions options = DuplicateOptions.defaults()
    .withIdentities(localIdentity, authenticatedPeerIdentity);
DuplicateResult result = Zmux.resolveDuplicate(first, second, options);
ZmuxSession active = result.survivor();
```

The session dialled by the smaller unsigned identity survives. Without supplied
identities, the helper compares dialler Session Nonces only when both sessions
use multipath, otherwise role nonces only when both negotiated auto roles.
The comparison domain is common to both sessions. The application must identify
the peer; the helper does not authenticate identities or infer peer equivalence.
On a symmetric carrier, call `session.designateDialer(localDialled)` first;
bootstrap dialler metadata is independent of the negotiated stream role and
cannot be reversed once known.

The losing session rejects new local opens immediately, wakes blocked openers,
and drains through its existing GOAWAY/close logic. Already opened streams,
including ones not yet written, keep their I/O until they end or are released.
The survivor accepts new work normally. Same-session, same-dialler, unknown,
equal or incomparable inputs throw `DuplicateResolutionException` without
changing either session.

`options.withRejoin(route)` optionally makes one best-effort join through a
`PipePool` dial policy, only when this endpoint dialled the loser and the
survivor has negotiated multipath with admission room. The losing carrier is
never reused. `result.rejoinStarted()` reports a started worker, not a completed
join; `result.rejoin()` is its nullable `PipeMaintenance` handle. Inspect its
completion result, close it or supply `withCancellation` to stop the attempt.
It performs no redial after failure and no rotation. A race for admission can
skip the dial, and any skipped, failed or cancelled rejoin leaves the original
resolution in effect. The same scoped ownership and cooperative cancellation
rules as pipe maintenance apply. A successful joined pipe remains owned by the
survivor after the one-shot worker ends.

## Asynchronous application operations

Every blocking call has an asynchronous twin returning `ZmuxFuture`:
`ZmuxSession` provides `openStreamAsync`, `openSendStreamAsync`,
`acceptStreamAsync`, `acceptReceiveStreamAsync`, `pingAsync`,
`nextEventAsync`, `shutdownAsync` and `replacePipes`;
`Zmux`, `ZmuxListener` and `ZmuxPipe` provide `connectAsync`, `connectTlsAsync`,
`dialTlsPipeAsync`, `acceptAsync`, `shutdownAsync` and `peerAttachedAsync`; streams provide
`readAsync(ByteBuffer)`, `readChunkAsync(max)`, `writeAsync(ByteBuffer)`,
`writeChunkAsync(Chunk)`, `finishAsync` and `idAsync`. A blocking call is the
same operation plus a bounded wait. An asynchronous form takes no timeout:
`orTimeout` (or `completeOnTimeout`) on its future bounds it and withdraws the
wait. Splice, the secure carrier's `Seal.establish`, pool maintenance's
`completion()` and the adapter's `Handled` return `ZmuxFuture` too; only code the
application supplies (an admission `Decider`, a `LaneOpener`) returns a
`CompletionStage`. Submission never waits for network I/O and
is safe on an event-loop thread; pending operations do not hold a thread. An
operation that can complete at once returns a completed future, and its
dependents run on the calling thread; later completions run outside the event
loop, one at a time and in the order the session decided them, on the
session's completion context: a dependent stage that blocks delays the
session's later completions, so never wait there for another future of the
same session.

Blocking and asynchronous I/O share one FIFO turn per stream direction, so a
FIN cannot overtake an earlier asynchronous write; reads and writes remain
independent. Read/write timeouts set on the stream are captured at submission
and include time waiting for the turn.

```java
var incoming = session.acceptStreamAsync().orTimeout(5, TimeUnit.SECONDS);
var opened = session.openStreamAsync();
var sent = opened.thenCompose(stream -> {
    var writing = stream.writeAsync(payload);
    return writing.thenCompose(ignored -> stream.finishAsync());
});
```

`readAsync` and `writeAsync` borrow their ByteBuffer, including its position,
until the returned future completes. Do not access or reuse the buffer before
that point. `writeChunkAsync` consumes the Chunk immediately, including on
cancellation or failure; `readChunkAsync` hands ownership to the caller, who
must close or forward it. FIN is reported as `-1` by read and `null` by chunk
read. A write can have transferred a prefix when it fails or is cancelled.

Cancel the **original returned future**, rather than a dependent stage, to
cancel the operation; the contract is on `ZmuxFuture`. `cancel` never waits for
the event loop: it returns true when the operation will not complete any more,
and from then on it no longer touches its borrowed buffer; it returns false
once the outcome was decided. Completing the future yourself (`complete`,
`completeExceptionally`, `orTimeout`, ...) cancels the operation first, so the
buffer rule holds for every completion. `get(timeout)` limits only your wait.
Dependent stages are plain `CompletableFuture`s; cancelling a dependent does
not cancel its source.

## Explicit interoperability gate

`tools/verify-interop.sh` builds the scripted Rust zmux peer and independent
QMux peers in Rust and Go, then runs eight Java-driven role cases and four
Go-driven cases against the Java peer. Set `ZMUX_RS_ROOT` and `ZMUX_GO_ROOT`
to matching sibling checkouts and use the repository's Java and Go toolchain
requirements. All commands are bounded. Optional `CARGO_TARGET_DIR` reuses an
existing local build cache.

The explicit `PeerInterop` and `QmuxInterop` suites require their peer binaries;
missing binaries fail, and the runner verifies four non-skipped cases per
suite. `SealPeerInterop` runs Establish with the Go peer in
`interop/seal-go-peer` (`ZMUX_SEAL_INTEROP_PEER`) for every Profile in both
roles, sixteen cases. They are separate from the default Maven unit/integration suite.
`InteropProcessTest` runs by default and checks oversized output and interrupted
child-process cleanup. Multipath scenarios verify traffic on both pipes in
both directions and continued transfer after aborting the bootstrap pipe.
The plain QMux cases verify negotiation disables zmux-only features.

The zmux scripted `close` command consumes the request FIN and closes the
session without a stream response. Tests assert the remote close code/reason
and child exit status, avoiding a fixed response-delay assumption. QMux tests
initiate session close locally and verify the independent peer's decoded
application close. This TCP gate does not imply HTTP/2, HTTP/3 or
cross-platform transport coverage.

## Session send-buffer growth

`ZmuxConfig.Builder.sendBuffer(n)` sets the initial written-but-unreleased byte budget (default
4 MiB). With multipath, it grows to the retransmit budget as rate/RTT estimates
grow. `sendBufferMax(n)` sets a fixed maximum, at least the initial budget (a
smaller one is rejected, never raised); not calling it (the default) is automatic: at least 64 MiB and above that within the existing
listener/session memory allowance. Set an explicit maximum when a fixed bound
is wanted. Sessions without negotiated multipath retain the initial buffer behavior.

All sending directions share the effective budget through the existing fair
share calculation. The total is bounded except for the small-direction floor;
a direction's explicit `setSendBuffer(n)` still applies, including limits below
64 KiB (values below 16 KiB are raised to 16 KiB). Unsent and retained bytes
already included in the memory account are not charged a second time when
computing the absolute growth maximum.
