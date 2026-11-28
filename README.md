# zmux-java

[![CI](https://github.com/zmuxio/zmux-java/actions/workflows/ci.yml/badge.svg)](https://github.com/zmuxio/zmux-java/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.zmuxio/zmux?label=maven%20central)](https://central.sonatype.com/artifact/io.github.zmuxio/zmux)
[![Javadoc](https://javadoc.io/badge2/io.github.zmuxio/zmux/javadoc.svg)](https://javadoc.io/doc/io.github.zmuxio/zmux)
[![Java](https://img.shields.io/badge/Java-25%2B-blue)](pom.xml)
[![License](https://img.shields.io/github/license/zmuxio/zmux-java)](LICENSE)

[![zmux-quic](https://img.shields.io/maven-central/v/io.github.zmuxio/zmux-quic?label=zmux-quic)](https://central.sonatype.com/artifact/io.github.zmuxio/zmux-quic)
[![zmux-http](https://img.shields.io/maven-central/v/io.github.zmuxio/zmux-http?label=zmux-http)](https://central.sonatype.com/artifact/io.github.zmuxio/zmux-http)

Java 25 stream and datagram multiplexing over reliable carriers, compatible
with QMux draft-02. The core has no runtime dependencies. A shared NIO event
loop owns each session; blocking application calls park their calling thread.
Virtual threads are a good fit. Calls that would block an event loop or the
same session's hook context fail with `IllegalStateException`.

The core module is `io.zmux`, exporting `io.zmux`, `io.zmux.carrier`,
`io.zmux.helper` and `io.zmux.seal`.

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
`zmux-http` (HTTP/1.1, HTTP/2 and WebSocket carriers), `zmux-http3` (optional
HTTP/3 over QUIC).

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
owns the stream; `awaitWritable` waits, consuming nothing, until `tryWrite`
takes bytes (send-buffer room, not credit: `sendCapacity` is a different
quantity), bounded by the write timeout or a `Duration`. The wait holds the
stream's write turn: other writes and `finish()` of that stream queue behind it,
and `tryWrite` usually, not always, takes the returned bytes (0: wait again). `readAll(maxBytes)` reads to FIN; past `maxBytes` it stops
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
`awaitAttached`, `awaitClosed`, `listener.accept`) has a form without a bound
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
Use `Chunk.withBuffer(action)` for scoped NIO access. Keep the view and any
derived slices inside the action.
Close or transfer every chunk to return its storage. When a new backing or lease
would exceed the session's chunk allowance, the read throws `ChunkBudgetException`
before consuming bytes. Release held chunks and retry, or use a normal read.
The default allowance covers 8 MiB of distinct backings and up to 32,768 live leases.

`session.openPipeStream(info, tlvs)` (OPEN_INFO and application TLVs, both optional) opens a bidirectional stream marked as
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
        .sessionPolicy(new SessionPolicy(new CapacityLimits(4)))            // four steady local attachments
        .pipeAuth(PipeAuth.psk(secret).withEndToEnd())       // the join secret, and pipe 0 is end to end
        .build();
ZmuxSession session = Zmux.connect(serverAddress, config);   // the server: Zmux.listen(address, ListenerConfig.of(config))
ZmuxPipe second = session.addPipe(otherServerAddress, PipeOptions.defaults());
for (ZmuxPipe p : session.pipes()) System.out.println(p.id() + " " + p.stats());
```

Multipath and resume need a secret source: `PipeAuth.psk(secret)` (or
`PipeAuth.deferred()`, supplied later with `session.setJoinSecret`),
`PipeAuth.exporter()` (the exporter of the innermost end-to-end handshake, today
a TLS 1.3 connection that this process terminates), or both (`PipeAuth.exporter().withPsk(secret)`);
a session whose first pipe is on the secure carrier takes its secret from the
Establish and leaves `pipeAuth` unset. Without a source the session fails to
start with `IllegalArgumentException`, before any byte is written, and so does a
key or the exporter on a secure first pipe (`PipeAuth.none().withEndToEnd()` is
accepted there without effect, so that one configuration serves both kinds of
first pipe). A session has no join keys until the host states that its first
pipe is end to end: `PipeAuth.withEndToEnd()` when known before it starts, else
`session.declareEndToEnd()` once proven; until then local joins fail with
`JoinPendingException`, whose message names what is missing (the declaration,
the deferred PSK or both), and the peer's are held. A server session needs a
listener for either feature: `Zmux.open(io, Role.SERVER, config)` with
a steady attachment limit above 1 or a `resumeTimeout` is a configuration error.

The secure carrier (`io.zmux.seal`) runs an Establish on a session's first
carrier. The server installs it as the listener's inbound transform; the client
runs it on its carrier and opens the session on the result:

```java
ServerConfig server = ServerConfig.builder(gate, StaticKey.generate()).build();
ListenerConfig lc = ListenerConfig.builder().session(config).inboundTransform(Seal.server(server)).build();

ClientConfig client = ClientConfig.builder()
        .anchor(new PinAnchor(KeyId.staticKeyId(nodePublicKey)))
        .psk(handle, psk)
        .handshakeTimeout(Duration.ofSeconds(5))   // 10 s unless set; noHandshakeTimeout() for none
        // .staticKey(myKey): the client's StaticKey; unset, a key generated once per process
        .build();
PipeIo io = Seal.establish(carrier, client).get();     // a ZmuxFuture
ZmuxSession session = Zmux.open(io, Role.CLIENT, config);
Peer peer = Seal.peerOf(io).orElseThrow();
```

Both configurations bound the application data a peer sends in the handshake
(`maxAppData(bytes)`, 256 unless set, or `noMaxAppData()`). Failures of the
protocol are `SecureCarrierException`, a `ZmuxException` carrying the error
class.

`StaticKey` is `AutoCloseable`: `close()` zeroes the private scalar it holds
(the JDK's own key object keeps a copy until garbage collection), after which
the key can no longer take part in a handshake; close it once no configuration
or session using it remains. Its `toString()` shows only the public key.

`session.addPipe(address, options)` joins another TCP carrier and waits
until the peer has attached it (`PipeOptions.Builder.returnOnReply(true)`
returns at the verified Reply; `ZmuxPipe.awaitAttached` waits later),
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
and clearing both mean the default. A pipe can also revalidate, retire gracefully (`close()`) or
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
`EpochDeleted` report the exchange and retirement of epoch keys.

### Resume after a lost connection

`resumeTimeout(d)` keeps a suspended session available for a later Join for
the grace period `d`. A `SessionController` can maintain a target on the
dialing side. The listener holds the suspended session on the receiving side.
`SessionSuspended` and `SessionResumed` report the resulting state, and
`ResumeUnattended` indicates that the application must supply a carrier.

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
`IdleTimeout`, `HandshakeTimeout`, `ResumeTimeout`,
`ResumeFailed` and `PipesLost`. For `ResumeTimeout` and `PipesLost` the
exception's cause is the error of the last pipe lost, for example a join the
peer refused while the session tried to resume.

Each joined carrier has a record mode. `RecordMode.SEALED`, the default, protects
records with the keys of the Join handshake; `BOUND` leaves them to a carrier that
declares the contract (`PipeIo.withCarrierSecure()`, and a TLS 1.3 connection this process terminates) and binds
its exporter into the handshake; `TRUSTED` leaves them to the deployment.
`ZmuxConfig.Builder.recordPolicy` or `PipeOptions.Builder.recordMode` chooses the mode of a
local join. The acceptor first calls the authorizer from its session policy for the authenticated Hello.
Publish changes with the session's
`setSessionPolicy` method. Increase both the policy revision and the authorization
generation when permissions change. A Join authorized under an older generation
is rejected if the change commits before its attachment does.

Its `allowRecordMode` policy checks the mode and defaults to Sealed. An outgoing
Join returns a pipe or an error. When returning on Reply, call `pipe.attached()`
to observe confirmation. A capacity change exposes Reply, confirmation and
resource release through its `Attempt`. For a carrier accepted by an adapter,
`Handled.awaitSetup()` reports the setup result.

The optional `zmux-http` module provides HTTP and WebSocket carriers (see
[HTTP carriers](#http-carriers)). Other integrations (an HTTP stack of the
application's own, HTTP/3, WebTransport) use the same external `PipeIo`
contract. A stream of an HTTP/3 request or a WebTransport session can read the
exporter of its QUIC connection, so such a pipe declares the contract with
`withExporter(...)` over the connection's exporter and `withCarrierSecure()`: the Binding names
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
On the receiving side `PipeReader.Sink.onLent(buffer, consumed)` lends a batch:
zmux copies it before the call returns (the reader may reuse the buffer at once)
and runs `consumed` once it parsed the copy, as for `onChunk`: that is the
carrier's backpressure. A reader delivers more before earlier batches are
consumed only within the `receiveMemory` it declares
in `PipeIo.Capabilities` (`Capabilities.builder().receiveMemory(bytes)` or
`PipeIo.withReceiveMemory`). Queue occupancy includes each distinct retained
backing allocation and each delivery's bookkeeping. `onChunk` transfers
ownership, so zmux can parse in place and retain payload views. A reader that
exceeds its queue limit fails its carrier.
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
is reserved until the paired attachment releases its carrier, lanes and workers. Waiters hold one parsed first record and
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
size). `carrierSample()` is an optional, cached observation of the local sender
on the underlying connection. Pipes and sessions on the same connection share
its identity and sample. Netty QUIC reports its native sent and lost packet
counts. Java TCP reports unknown counts. An HTTP Exchange can send through
multiple physical connections and also reports unknown counts. An unknown pair
is empty, while a measured zero is present. Samples use the `System.nanoTime()`
time domain. Attachment starts acquisition and the native sample becomes available
when that query completes. A stats read returns the cached
sample immediately and requests a background refresh when it is a second old.
Durations use `Duration`, rates bytes/s. Estimates are observations,
not guaranteed bandwidth. Quality bands and capacity floors
are opt-in through `session.setQualityBand` (a pipe's own band through
`PipeUpdate.qualityBand`) and `setCapacityFloor`, and off again through
`clearQualityBand` and `clearCapacityFloor`. A band is built with
`QualityBand.builder()`: `rtt(d)` and `rateRatio(r)` each turn one half on
(a half not set is off, and a band needs one), `minInterval(d)` (default 1 s)
or `noMinInterval()` bound the reports per pipe. `new CapacityFloor(rate)`
takes an integer rate in bytes/s, as every rate zmux reports, with the
recovery ratio `CapacityFloor.DEFAULT_RECOVERY_RATIO` (1.25) unless given.
Invalid values are refused where they are set. Reports appear in
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

`noDeadTimeout()` (on `Liveness.Builder` and `PipeLiveness.Builder`) turns
stall-based failure off: a stalled pipe still turns suspect and traffic moves
away from it (reinjection, probes), but only its carrier's error or end, its
drain deadline, its removal or the inbound silence of a receive-only pipe fail
it. It is off by default on carriers that report their own failure
(`PipeIo.withReportsFailure()`), such as the nested-session carriers of
`openPipeStream` and `ZmuxStream.asPipe`, whose outer session ends them when its
path fails (a `PipeIo.split` carrier only when both halves report it); a
pipe's own dead timeout (a value or off) overrides that, and the
carrier's report overrides the session's. Any large `deadTimeout` is safe.

The idle probe interval (`probeInterval`, default 15 s, at least 1 s) is measured
to the carrier: each probe waits a uniform draw from `[k_eff(1 - j), k_eff]` with
k_eff derived from the configured interval and j the probe jitter
(`probeJitter`, default 0.25), and a bootstrap pipe's first wait from `[0, k_eff]`;
j = 0 gives a fixed schedule. A probe is answered on its own pipe pair (in a
quiet session only there), with a probe of the answering end once its pipe has
been idle for max(1 s, T_suspect). `noProbeInterval()` turns off only the idle
probes (of a session, or of one pipe through `PipeLiveness`), and `noStandbyProbe()`
the faster probes of standby pipes (`standbyProbe`, default 1 s). Probes keep
pipes and their carriers in use, never the session.

Keep-alive (`Liveness.Builder.keepAlivePeriod`) is on by default at 15 s, capped at
half the effective idle timeout and jittered by the probe jitter; the idle timer
counts only the peer's evidence, so an idle session stays open while the peer
answers and closes within the idle timeout once its carrier dies. `noKeepAlivePeriod()`
turns it off and makes the idle timeout a limit on application inactivity; a set
period is at least 1 s. The configuration is flat: one builder method per
setting. A setting that is not a value has a `noX()` method of its own and reads
back as empty: `noHandshakeTimeout()`, `noMaxIdleTimeout()` (no idle timeout is
announced; a set `maxIdleTimeout` is at least 1 ms), `noProbation()`,
`noEpochPeriod()`, `noEpochJoins()` and `noJoinEarlyHello()`. An unlimited budget
is `ZmuxConfig.Builder.noMemoryBudget()` or `ListenerConfig.Builder.noMemoryBudget()`
(a budget of 0 lets nothing grow beyond the floors).

## Application hooks and budgets

`ZmuxConfig.Builder.tracer(tracer)` installs an ordered, asynchronous
`Tracer`. Every callback names its session (`SessionId`, also `session.id()`),
so one tracer can serve many sessions. It sees each occurrence of an event
(a failed Hello with `count` 1, one `PipesRetired` per retired pipe), apart
from the event stream. Each session has a trace backlog of 256 events, fixed:
a slow tracer loses that session's events once it is full and learns the
count from `dropped(session, count)`, called where the lost events stood,
before the next event it receives or before `closed`. `established` is the
first call and `closed` the last (nothing follows it); both and `dropped` take
no room from the 256. An application that must not lose events reads
`nextEvent()`. Window growth can be approved
by `allowConnectionWindowIncrease`; the predicate runs off-loop and stale or
now-unaffordable answers cannot increase credit.

`PipeIo.carrier` (set with `withCarrier`) is the single declaration of a pipe's
carrier kind. The bundled adapters fill it on both sides: HTTP streaming
requests are `STREAMING`, both halves of a split bootstrap or a joined half are
`SPLIT`, the exchange carrier is `EXCHANGE`, WebSocket is `WEBSOCKET` and QUIC
streams are `QUIC`; a `Candidate`'s carriers take the candidate's kind. Raw TCP
and TLS carriers, and pipe inputs that leave it unset, have none (null). The
kind is reported in `PipeStats.carrier` and the pipe events
(`PipeAdded`, `PipeJoined`, `PipeSuspect`, `PipeRecovered`, `PipeFailed`, `PipeClosed`,
each with the direction it is about, `PipePriorityChanged`, with the source of the
change, and `PipeQualityChanged`). Dropped datagrams are counted in the statistics,
not reported as events. Retirement has one report only,
`PipesRetired(retired, failed, closed, last)`: the pipes retired since it
was last read, of those the ones whose failure or end was still unread, and the
last four retired IDs; the unread events of a retired pipe leave the queue with
it. It accumulates until read, so the event queue stays bounded however many
pipes a session goes through; compare `session.pipes()` with the handles you hold
to find every retired ID.

The event stream never drops an event: it is a keyed queue holding at most one
unread event per key (`SessionEvent` lists the keys: for example a pipe event by
pipe and direction, `TierSaturation` by priority, `JoinAuthFailed` by reason).
An event whose key has an unread one updates it (the latest values replace it;
`JoinAuthFailed.count` counts the failures since it was last read) and moves it
to the tail, so events are read in the order of their keys' latest occurrences.
`SessionResumed` removes an unread `ResumeUnattended`. The queue needs no
configured bound: its keys are those of the pipes, tiers and carrier candidates
the session holds plus a fixed few. Once the session has ended nothing more
enters it; what is queued is still read, then `nextEvent()` throws the
session's error.

`io.zmux.helper.Limiter` is the reserve-then-refund interface shared by every
zmux implementation: `CompletionStage<Long> acquire(long min, long max)` waits
for a grant, `long tryAcquire(long min, long max)` grants without waiting or
returns 0, and `void refund(long n)` gives back granted bytes that were not
used. Units are bytes. Arguments are clamped: `max <= 0` takes nothing,
`min < 1` is 1, `min > max` is `max`; `(n, n)` asks for a whole unit and
`(1, max)` for as much as is available. A grant lies between `min` (or the
limiter's burst, if smaller) and `max`, and a whole unit granted `(n, n)` may
be sent even when fewer bytes were returned. Grants are taken when handed out,
so streams, splices and sessions sharing one limiter never exceed it together.
A limiter that fails fails the operation in `acquire` (an exception, a failed
stage, or a grant outside `1..max`), counts as nothing taken in `tryAcquire`,
and is ignored in `refund`. `Permit` does the bookkeeping:
`Permit.acquire`/`tryAcquire` take a grant, `consume` marks bytes used, and
`close` refunds the rest. `Permit.acquire` returns a `ZmuxFuture`; cancelling
it refunds a grant that arrives after the cancellation. `LimitedStreams.input/output` wrap application streams, wait outside the
event loop, and keep only bytes actually forwarded, including partial
interrupted writes. Datagram limiters are installed with
`datagramLimiter(new DatagramLimiter(send, receive))`: each datagram costs one
`tryAcquire(n, n)` for its whole payload, on send before it is queued and on
receive before it reaches the application's queue; one that is refused is
dropped and counted, and a granted one that is then not queued is refunded. Received datagrams waiting for a hook remain within the
same bounded receive queue and cannot return after eviction.

A stream's datagram queue is bounded per stream and direction by a count
(`datagramStreamQueue`, default 32) and by payload bytes
(`datagramStreamBytes`, default 512 KiB; `noDatagramStreamBytes()` leaves the
count alone): a datagram that would exceed either drops the oldest datagrams of
that queue until both hold, each counted as dropped, and a single datagram
larger than the byte bound is kept alone. `datagramReceiveBytes` bounds the
session queue and the datagrams held for streams not opened yet; stream queues
do not count against it.

Every datagram queue, the send queue included, also bounds how long its
datagrams wait (`datagramQueueDelay`, default 20 ms; `noDatagramQueueDelay()`
turns it off): once the datagrams leaving a queue have waited longer for
100 ms, CoDel drops some as they leave, counted, so a slow reader gets fresh
datagrams instead of stale ones. `ConnectionInfo.pathDatagramPayload()` (and
`ZmuxStream.pathDatagramPayload(context)`) reports the largest payload on
eligible record and native paths. `nativeDatagramPayload()` narrows the snapshot
to native channels. Check `nativeDatagramAvailable()` to distinguish an empty
datagram that fits from an unavailable channel. `SessionEvent.DatagramPathChanged`
reports changes to the session snapshot.

`Zmux.MAX_STREAM_CREDIT` (2^60) is the largest stream credit, the value that
grants the peer as many streams as the protocol can count, for
`maxIncomingStreams`, `maxIncomingUniStreams` and `raiseIncomingStreams`; a larger
value is a configuration error.

`session.datagrams().send(payload)` queues a datagram, evicting the oldest one
when the send queue is full. `sendWait(payload, timeout)` waits for space.
Both report local drops with `DatagramDroppedException`, whose `reason()`
identifies deadline, no native path, limiter, queue overflow or sequence
exhaustion. Enable datagrams on both peers before using them.

`io.zmux.helper.PacketStream` carries packets (a context ID and a payload) on
a bidirectional stream, each as a datagram of the stream's flow or as an
RFC 9297 DATAGRAM capsule in the stream's bytes:

```java
PacketStream packets = PacketStream.open(session);          // or PacketStream.of(acceptedStream)
packets.send(ByteBuffer.wrap(ipPacket));                     // datagram if it fits, else a capsule
StreamDatagram next = packets.receive();                     // null once the peer closed it
packets.close();
```

`Options` choose `PREFER_DATAGRAM` (the default), `STREAM_ONLY` or
`DATAGRAM_ONLY`, and the largest accepted capsule payload (65535 by default).
`PREFER_DATAGRAM` uses a capsule when the negotiated datagram representations
cannot carry the packet. `STREAM_ONLY` paired with effective `REQUIRE_NATIVE`
is an invalid configuration. A larger received capsule, or a stream ending
inside one, resets and stops the stream
with `PacketStream.PACKET_ERROR`. `finishAsync()` sends the FIN after the queued
capsules and keeps receiving; `close()` releases the stream. `Zmux.splicePackets(a, b)` relays two packet
streams like `Zmux.splice`, writing a datagram the next hop cannot take as a
capsule. Splice results count dropped datagrams in total and by cause
(`datagramsTooLarge`, `datagramsLimited`) and the datagrams written as capsules
(`datagramsAsCapsules`).

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
decider gets up to `peek` bytes of a byte stream, or the whole first message of a
message-framed carrier (WebSocket). A positive peek bounds that message by
`max(peek, maxPeek)`. The separate `maxMessage` limit also applies. Each call receives the carrier's
immutable `Admission.CarrierInfo` with its source, the peer it arrived from, the negotiated application
protocol, the server name (SNI) the client sent, the TLS exporter, its `stage`
(`CARRIER`, the carrier's start, or `HEAD`, an HTTP request head), its `framing`,
whether it is an HTTP `upgrade`, and for HTTP carriers the request's header
fields (`info.header(name)`, case-insensitive). It returns a `CompletionStage`
of a decision whose `AdmissionVerdict` is `ACCEPT` (skipping a prefix),
`FALLBACK`, `TAKEOVER`, `STALL`, `CLOSE`, `MORE` (a longer prefix of a byte stream, at most
`maxPeek`) or `DEFER` (on an upgrade's head only: complete the upgrade and
decide again on the carrier's first message), due within the peek timeout
(`Admission.Deadline`); a stage still pending then, or when the listener closes,
is cancelled and the carrier closed. `MORE` on a head or a message carrier, and
`DEFER` anywhere but an upgrade's head, close the carrier. Exceeding the prefix bound counts as `closed`. Exceeding a tighter
`maxMessage` limit counts as `tooLarge`. The carrier also closes on a first message
that fails, is not a binary message or is empty (`closed`).
`Admission.Decider.immediate(...)` wraps a decision taken at once, and
`PrefixAuth.Verifier` accepts a 64-byte ticket presentation. Use one
`PrefixStore` for every entry in the same admission domain. The store owns
outer-key generations, ticket versions, the rollback floor, nonce redemption
and single-use tickets. A `MemoryPrefixStore` keeps this state in one process
and bounds both the ticket IDs and the live nonce records. Ticket ID tombstones
remain for the lifetime of the store, so the ticket limit covers every distinct
ID installed or revoked. A reusable ticket holder can fill the shared nonce
capacity with valid presentations in the skew window. New presentations are
then refused until nonce claims expire. A durable host store can preserve the
domain's state across processes and restarts.

```java
var slot = Duration.ofSeconds(30);
var carrierForm = new PrefixAuth.CarrierOptions(true, false);
var generation = new PrefixStore.Generation(outerKey, 0, -1L);
var id = PrefixStore.Id.of(ticketId);
var store = new MemoryPrefixStore(new PrefixStore.Window(slot, 1), List.of(generation));
store.addTicket(new PrefixStore.Ticket(id, ticketKey, 1, firstSlot, lastSlot, true));

var verifier = new PrefixAuth.Verifier(carrierForm, store);
var config = ListenerConfig.builder().admission(verifier.admission().build()).build();
var auth = new PrefixAuth(carrierForm, slot, id, ticketKey, List.of(generation));
```

Use independent random 32-byte keys for the domain generation and each ticket,
and a fresh random 16-byte ID for each ticket. Slot ranges are inclusive unsigned
64-bit integers. A scheduled rotation ends the old interval just before the new
one begins. The listener defaults to silent close on prefix refusal. Use
`verifier.admission().refusal(AdmissionVerdict.FALLBACK)` to hand a supported
request or byte stream back to its host with the original input. Admission
outcomes are counted by `listener.stats().admission()`.

`Decision.takeover(value)` gives the carrier itself to `ListenerConfig.fallback`.
`Decision.fallback(value)` selects the normal host service for the carrier.
For TCP, TLS, an upgraded WebSocket, QUIC or an application-provided stream,
that service is also `ListenerConfig.fallback`. For an HTTP request or an upgrade
decided on its request head, the HTTP handler behind `HttpEndpoint` receives
the request with all body bytes replayed. The `Takeover` has the carrier's
`info()`, the decision's `value()`, and `pipe()`, a `PipeIo` with what was read
replayed in front (the first message whole, then the following ones, on a
message carrier, whose halves `messages()` also gives typed); the view keeps
every property of the carrier (label, kind, variant, connection, the
`PipeIo.Capabilities` but the lane opener, `unsent()`). A
plain TCP carrier also offers `socket()`, its blocking `SocketChannel` with the
bytes read, as long as the pipe view is unused. The host can use the carrier while its fallback stage runs.
Completion (including failure, a throw, or a null stage) closes an attached
takeover. Withdrawal during setup or shutdown of the carrier’s event loop can
close it earlier. Withdrawal leaves the host’s stage running. A host that calls
`detach()` before withdrawal or completion takes responsibility for closure,
including the QUIC connection or HTTP request a carrier adapter gives it.
Each half closes once. `detach()` returns false for a carrier already closed
by the host and throws once zmux has reclaimed it.

```java
.fallback(t -> {
    if (!t.detach()) return null;                      // the host closed it already
    var socket = t.socket();
    if (socket.isPresent()) tunnels.hand(t.value(), socket.get());  // plain TCP: closed by the host
    else if (t.messages().isPresent()) relays.hand(t.value(), t.messages().get());  // WebSocket
    else streams.hand(t.value(), t.pipe());            // TLS, a QUIC stream, an HTTP request carrier, ...
    return null;                                       // the carrier outlives the call
})
```

Without a fallback a
carrier given back stalls: nothing is sent, input is discarded, and the carrier
closes after a duration that depends on its source alone and keeps its slot
until then, or when the listener closes; a source keeps one slot free of
stalls. For `TAKEOVER` on a streaming HTTP request, the adapter writes its
acceptance response head before calling the host. The host reads the request
body (including any prefix) and writes the response body. A WebSocket
takeover completes the upgrade first. The initial authenticated request of
an HTTP exchange can go to the HTTP handler on `FALLBACK`; `TAKEOVER` hands
the host the exchange's logical byte stream. The listener and per-variant
statistics count `fallback()` or `takeover()` when the handback starts.
Withdrawal before the host starts counts as `closed()`. A missing target
counts as a stall or stall overflow. A failed acceptance head counts in `handshakeFailed()`,
and expiry of its admission deadline counts in `peekTimedOut()`.

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
what a layer adds to each carrier write (at most 65535).

`ListenerConfig.Builder.rejection` selects one silent outcome for unauthenticated
Establish and Join refusals. `RejectInfo.Action.CLOSE` is the default. `STALL` uses
the configured handshake or Join timeout and the listener's stall limits.
`ListenerConfig.Builder.onReject` observes local reasons independently of the
wire outcome. The observer and session configuration callback must return
promptly. The asynchronous admission decision receives its deadline.

### Handing a carrier to a listener

Call `listener.handlePipe(io, hint)` from an application thread. Ownership
passes on entry, including on failure. The call succeeds when a bootstrap is
queued for accept, a join is confirmed, or a lane is attached. Refusal throws
`IOException`. A fallback that completes normally succeeds, including when the host
detached the carrier. A failed stage reports its error. Cleanup may still be running. Thread interruption
withdraws the request if it wins the handoff decision. If publication already owns
the result, the caller receives that result with its interrupt status restored. The session owns the carrier and its
bounded close flush after handoff. Stopping Serve leaves attached pipes usable.

For an arrival with a reserved slot, use `slot.handle(io, hint).awaitSetup()`.
An adapter can observe admission and permission to write earlier, via
`Handled.verdict()` and `taken()`. Explicitly waiting on `Handled.closed()`
waits for actual carrier and worker release. A removed lane completes its own
handle while the main pipe continues serving streams. The main pipe waits for
each child lane, including its transform and adapter cleanup. A pipe handle’s `closed()` also waits
for protocol retirement. For a paired split, either half’s notification means
the attachment has released its transformed carrier, all lanes and workers,
and the token can be reused. Withdrawal before a match prevents a later
complementary half from reviving the request. Once matched, either request
can sustain the combined setup. HTTP split requests also wait for buffered input
and the final response write to be released. Blocking channel halves include
their workers, and QUIC halves include cleanup on the stream event loop. The streaming HTTP adapter keeps its request alive
internally. Java's process-wide `Zmux.handleLane` dispatches asynchronously.

### Carriers from an HTTP server

An application that runs its own HTTP server gives each request whose body (or
nothing after its head) is the carrier to
`listener.adapter().handleRequest(io, hint, onHead, fallback, writeHead)`: decided on its
head (`Stage.HEAD`, no prefix) or on its body's start, and a request given back
goes to that fallback, with the body bytes read replayed, for an HTTP answer.
The required `writeHead` operation commits the acceptance response for `TAKEOVER`.
A null step closes the supplied halves and throws `NullPointerException` before
admission. An asynchronous step can implement `Upgrade.released()` to report
when the actual write has finished. After withdrawal the source slot and
`Handled.closed()` wait for that signal. For steps without one, the wait is
bounded by one linger interval. For an accepted upgrade without a release signal,
that bound starts when the adapter opens the carrier. A pending explicit signal
logs once at DEBUG after one linger and continues to hold the attachment until
the write is released.
Its stage completes after the head write and flush succeed.
An upgrade (WebSocket) goes to `listener.adapter().admitFirst(io, hint,
upgrade, headFallback)` before it is answered: the head is decided first; on
`ACCEPT` the listener runs `upgrade` (the 101) and takes the carrier, on `DEFER`
it runs `upgrade`, waits for it and for the first message within the rest of
the peek timeout and while the listener runs (`peekTimedOut`, `closed`,
`handshakeFailed` otherwise), and decides again; a head given back goes to
`headFallback`. On `TAKEOVER`, the adapter completes the upgrade before the listener hands the carrier to its fallback. Other carriers go to `listener.adapter().handle(io, hint)`.
`Handled` has the final verdict and whether a session took the carrier
(`taken()`: false when the listener refused it, having written nothing);
`takenOver()` and `detached()` say whether the listener's fallback took it over
and its host detached it, in which case the adapter leaves what the carrier
runs on (a connection, a request) to the host. What
only such an adapter or an own accept loop needs (`reserve` and the `Slot` it
returns, `handle`, `handleRequest`, `admitFirst`,
`knownJoin`, `stallDuration`, `countSetupFailure`, `firstRecordTimeout`,
`memoryUsed`) is in `io.zmux.adapter.ListenerAdapter`, reached through
`listener.adapter()`; a session's counterpart (`isJoinReady`,
`declareEndToEnd`, `releaseFirstFlight`) is
`session.adapter()`. The `JoinHint` carries the request:
`JoinHint.of(RequestHead.of(headers), uplink, peer)` takes the
`Zmux-Pipe-Pair` field from the headers (`JoinHint.of(head, pairField, uplink, peer)` another field).
A carrier decided on its request head is decided at once, without reading a
prefix. `withSource(address)` gives the client address the
host resolved itself (the peer otherwise); with `HttpEndpoint`, a host sets
`HttpEndpoint.SOURCE` on the connection's channel instead. The pipe's
capabilities supply the TLS metadata of the connection that carries the
request: `withExporter`, `withProtocol` and `withServerName`.

`listener.drain(reason)` refuses new sessions after the zmux handshake with
`CONNECTION_REFUSED` and the supplied reason, including split bootstraps once
paired. The dialer can inspect `ConnectionRefusedException.reason()`, or
`SessionError.TransportClosed.isConnectionRefused()` and `reason()` if it
already received its session. Existing joins, lanes and resumptions continue.
`undrain()` reopens new-session admission. A later `drain` replaces the reason.
Changes take effect at bootstrap classification and return immediately.

`listener.shutdown(code, reason)` drains permanently with its reason
(`accept()` throws `ListenerClosedException`), shuts every session of the
listener down with code and reason, and lets them join and resume until they
have all ended; then the listener closes. `shutdown(code, reason, Duration)`
closes the sessions still running when its bound passes, and
`shutdownAsync(code, reason)` returns a `ZmuxFuture<Void>` whose `orTimeout` is
that bound. `close()` stops the listener at once and leaves started sessions
running, without new pipes from it.

On an encrypted request-head entry, the verifier reads one `Zmux-Prefix-Auth`
field containing 86 characters of canonical unpadded base64url. Set the agreed
field name in `PrefixAuth.CarrierOptions`. On a byte stream the ticket occupies
the first 64 bytes, and on a binary message carrier it fills the first message.
An authenticated exchange uses its own request envelope. A QUIC host that
buffers streams before adapting them calls `QuicStreamPipe.observe` at stream
arrival to retain the original handshake state. For WebTransport CONNECT, pass
the credential to `Http3Client.openWebTransportAsync` and configure admission
on the listener. Both sides use the actual QUIC connection.

### Client credentials

A `CarrierCredential` is what a client presents to a server's admission: the bytes a
carrier starts with (`prefix(info)`), or the request header field of a carrier
decided on its head (`header(info)`). `PrefixAuth` presents a fresh ticket
token for every independent admission. Choose the bound form for TLS 1.3 or
QUIC with the actual connection's completed handshake exporter. Unbound
presentations on cleartext byte or message carriers require explicit bearer
opt-in. Header admission always requires encryption. Early data is refused.
`PrefixAuth.boundPrincipal(session)` returns a verified bound ticket ID after
the listener delivers the session. An inner Establish independently verifies
its own peer.

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
`QuicPool.builder(client, address, serverName).credential(credential)`, which present
it on the first stream of each connection they open. A pipe the application
dials itself (`Zmux.open`, `listener.open`, `addPipe(io, ...)`) presents
its credential where it is dialed: the prefix written first, or
`credential.header(info)` in its HTTP request.

## Session controller

A `SessionController` opens and maintains sessions in one host isolation
domain. Register each candidate with a stable ID, a cache key and its exact
Join layout. An opener receives the layout, credential and cancellation signal
and gives the controller the carriers it has opened. The session performs the
Join and owns confirmed attachments.

```java
var layout = List.of(new AttachmentSpec(Directions.BOTH, PipeOptions.defaults()));
var route = new SessionController.Candidate("primary", "region-a", CarrierKind.STREAMING,
        "", 0, false, true, layout, (setup, request) ->
                new CarrierOpener.Opened(List.of(http.pipe(url, headers, PipeOptions.Direction.BIDIRECTIONAL,
                        request.credential()))));
var target = new SessionTarget(1, List.of(new SessionTarget.Group("online", List.of(
        new SessionTarget.Quota("primary", 1, Directions.BOTH, List.of("primary"))), SessionTarget.Rotation.NONE)), List.of());
var limits = SessionController.Limits.DEFAULT.withAttemptTimeout(Duration.ofSeconds(30));
var options = new SessionController.Options("tenant-a", List.of(route),
        SessionController.Choice.STATIC, SessionController.Limits.DEFAULT.withMaximumOpeners(limits), null, null, java.util.Map.of());
try (var controller = new SessionController(options)) {
    var opened = controller.open(config, target);
    try (var session = opened.session()) {
        // Use session and update the target through opened.control().
    }
}
```

Use `manage(session, target)` to bind an existing multipath session. With
`manage(session, target, "primary")`, the confirmed bootstrap member on pipe 0
counts toward a matching quota for the registered candidate `primary`. Its
carrier kind and variant must match the registration. The member keeps its
identity, directions, and accounting source.

A `SessionTarget` specifies member quotas and optional member
replacements with a minimum Join epoch. Update it with `control.setTarget`
and an increased revision. Each batch handles one logical member, and the
controller rotates among ready groups. The controller can rank candidates by registration
order or by the `CarrierCache` score within the candidate's tier and
standby level. The cache supports v2 export and import.

## HTTP carriers

The optional `zmux-http` module serves and dials HTTP carriers on Netty 4.2
([zmux-http/README.md](zmux-http/README.md)). A runnable minimum, a cleartext
server and a client in one process:

```java
var group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
try (ZmuxListener listener = Zmux.listener(ListenerConfig.defaults());
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
adapter candidate (`streamingCandidate`, `splitCandidate`,
`exchangeCandidate`, `webSocketCandidate`). Use `splitOpener(url, headers)`
to open the paired POST/GET carriers for a controller or a manual capacity
change. Each pair carries one Join.

For HTTP/3, add the optional `io.github.zmuxio:zmux-http3` artifact and use
`io.zmux.http3.Http3Server`. Bind UDP with
`Http3Server.bind(address, quicTls, quicOptions, endpoint, hostHandlerFactory)`.
The server owns its UDP socket and QUIC connections. It uses the same endpoint
for streaming, Split, Exchange and HTTP fallback. Applications with their own
QUIC server can install `Http3Server.initializer` before the first stream, or
use `Http3Server.requestInitializer` with their own HTTP/3 connection handler.
Configure the host's QUIC TLS with ALPN `h3`, early data disabled and
`QuicExporter.KEYLOG` when channel-bound credentials are required.

Select HTTPS over HTTP/3 with `io.zmux.http3.Http3Client.builder()`. For custom
trust or transport limits use `Http3Client.builder(clientQuicTls, quicOptions)`. The existing
request-carrier and Exchange methods use this transport on the client.
Include Netty's `netty-codec-native-quic` for your operating system and
architecture at runtime, at the same version as the Netty HTTP/3 codec.

For draft-16 WebTransport, build an HTTP/3 client and call
`Http3Client.openWebTransportAsync(client, target, headers, credential, timeout)`.
The returned `WebTransportSession` owns the CONNECT. Its `openPipeAsync()`
opens a bidirectional data stream for `Zmux.open` or a subsequent pipe.
Use `draining()` to observe DRAIN or connection GOAWAY, `drainAsync()`
to signal DRAIN, and `closeAsync(code, reason)` to close the session.
`closed()` reports the unsigned 32-bit code, UTF-8 reason and initiating side.

On the server, enable `HttpEndpoint.builder(listener).webTransport()` and use
`Http3Server.bind` or the connection-level initializer. The listener decides
admission on the CONNECT, including any channel-bound ticket, before the 200
response. The connection must negotiate QUIC and HTTP/3 datagrams. With the bundled
Netty transport, one WT session can be active per connection. QUIC limits apply
to its streams. A stream closed normally delivers FIN. Cancelling a stream can
discard its header before the peer opens a pipe. After the header arrives, a
reset fails the corresponding stream. Stream cancellation codes outside the
transport's range use H3_REQUEST_CANCELLED and the mapping is logged. A peer
that requires partial delivery on reset can refuse the connection. For a
host-managed connection, pass the QUIC receive options used by the host to the
connection-level builder, initializer or `serve` overload. For an already active
connection, install `QuicServer.observeDatagramCapacity(connection)` in its QUIC
initializer before activation.

Streaming, split and WebSocket carriers follow redirects of their requests on
the same host name only (another path, or a scheme upgrade from http to https,
ws to wss), at most 5 hops (`Redirects.DEFAULT_MAX`); a redirect to another
host, a downgrade, another port without an upgrade, or a POST a 301, 302 or 303
would turn into a GET fails the carrier with `HttpRedirectException`, which a
the session controller counts as refused. A streaming request is repeated whole at the new
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
both directions. `Zmux.listener(listenerConfig)` accepts external carriers via
`handle`. On an endpoint without a listener, a stream the peer opens on a
lane-capable connection goes to `Zmux.handleLane(io)`, one process-wide entry
that routes it by its Lane Key over the sessions of the process (the QUIC
adapter calls it itself).
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
delaying every later one; larger datagrams and peers without
the channel use records. Several sessions may share a connection's
datagrams. A QUIC connection always offers its datagrams; the session's
`enableDatagrams` is the one switch. `SessionStats` counts the
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

Each direction preserves its own FIN, RESET and STOP. Reliable data and
injected capsules share one writer. Stream datagrams use the next hop's
Flow with their Context ID intact. A packet splice can insert a datagram as
a reliable capsule when the peer lacks stream datagram support or a payload
exceeds the hop's negotiated datagram limit. A reliable
reset retains its code and the last complete output boundary. Without
RESET_STREAM_AT, the relay drains the protected prefix before sending RESET_STREAM.

Set direction-specific chains with `SpliceOptions.Builder.hooksAToB` and
`hooksBToA`. A chain can handle bytes on `splice`, reliable capsules on
`splicePackets`, and stream datagrams on either entry. Each callback runs once
per admitted unit and sees its metadata and the current ordered outputs. It can
pass the group through, replace it with outputs from `item.slice` or
`unit.allocate`, drop it, or reset the direction with an application code.
Only copy payloads that need to outlive the callback.

```java
SpliceHooks hooks = new SpliceHooks(List.of(unit -> {
    var input = unit.item(0);
    if (input.get(0) == 0) return new SpliceHook.Drop();
    return new SpliceHook.Pass();
}), List.of(), List.of());
SpliceOptions options = SpliceOptions.builder().hooksAToB(hooks).build();
SpliceController relay = Zmux.spliceControlled(inbound, outbound, options);
```

A controlled splice exposes `replaceHooks`, `pause`, `resume`, and typed
injection on each direction. A replacement applies at the next unit boundary.
A pause acknowledges when the current reliable write reaches a safe boundary.
An injection reports admission and local disposition separately, passes through
the installed chain, and is refused once the direction observes upstream FIN.
`SpliceWait` holds a group until its delay and resume signals are satisfied
or its deadline expires. `SpliceLimits` bounds the number of members, payload
segments, distinct backing bytes and queued items. The total wait defaults to
30 seconds. A negative duration disables this cap. A paused direction leaves
new datagrams in the source Flow, where `datagramReceiveLifetime` bounds their
age. An optional `SpliceMirror` copies samples or shares independent immutable
ranges under its own byte and item limits.

A direction limiter must declare its maximum burst through `maxBurstBytes()`
before installation. A transformed datagram group reserves all payload bytes
for direct datagrams or all encoded bytes for capsule fallback. The fallback
deadline applies until the first byte commits to the target writer. A direction's
`settlement().snapshot()` continues to report pending handoffs and final
refunds after `finished()`.
`SpliceResult` reports local submission counters and one end reason per direction,
including `HookReset`, `HookFailed`, `PacketError` and `WaitTimeout`.
`SpliceAbort.abort(code)` or `relay.abort(code)` propagates a chosen code.
Cancelling the result future resets both sends with ZMUX_CANCELLED and sends
STOP with code 0. Application hooks and limiter callbacks run outside the
session event loops and must return promptly.

## Capacity changes

A host can add or drain members explicitly. Start from `session.capacity()`
and pass its policy revision and observation watermark in a `ChangeRequest`.
`session.beginChange(request)` reserves the requested attachments. Use
`change.startMember(index, opener)` for each addition, then
`change.commit()` to validate and drain the whole batch together. A replacement
waits for confirmation and stability. The remaining members must preserve
direction coverage before any planned drain starts.

`Attempt.completion()` observes one attempt. Cancel the attempt explicitly to
stop its setup or cancel the change to discard uncommitted work. Closing the
optional controller stops its work while the session continues to own attached
members. Quotas for the physical QUIC connection pool remain in
[zmux-quic](zmux-quic/README.md#connection-pools).

A target group can set `SessionTarget.Rotation` limits for member age, sent bytes
and received bytes. A replacement confirms and settles in every expected direction
before the old member drains. A quota can override each candidate's local send
preference and the tier and standby state it requests for local receive.

A `SessionPolicy` topology entry restricts the directions a named candidate may
serve. Raising the Join authorization generation drains older peer-joined
attachments. A controller, its cache, and each managed session share a
`ClockDomain`; the process clock is used by default.

To collect completed record counts by physical carrier half, enable `AccountingPolicy`
on the session config and set the half identities with `PipeIo.withAccountingSources`.
Keep the `PipeAccounting.Ticket` from `pipe.accounting()` to read totals after the
attachment and its lane I/O are released.

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
new incoming carriers through `endpoint.adapter().handle`. Authentication, replay and attachment admission are shared
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
equal or incomparable inputs throw `DuplicateUndecidedException` without
changing either session.

`options.withRejoin(member, opener)` optionally starts a capacity change
on the multipath survivor when this endpoint dialed the loser. Use
`result.rejoin()` to observe or cancel the one-shot attempt when it starts.
The duplicate resolution takes effect independently of that attempt.

## Asynchronous application operations

Every blocking call has an asynchronous twin returning `ZmuxFuture`:
`ZmuxSession` provides `openStreamAsync`, `openSendStreamAsync`,
`acceptStreamAsync`, `acceptReceiveStreamAsync`, `pingAsync`,
`nextEventAsync` and `shutdownAsync`;
`Zmux`, `ZmuxListener` and `ZmuxPipe` provide `connectAsync`, `connectTlsAsync`,
`dialTlsPipeAsync`, `acceptAsync`, `shutdownAsync` and `attached`; streams provide
`readAsync(ByteBuffer)`, `readChunkAsync(max)`, `writeAsync(ByteBuffer)`,
`writeChunkAsync(Chunk)`, `finishAsync` and `idAsync`. A blocking call is the
same operation plus a bounded wait. A stream's blocking reads and writes,
its `InputStream`/`OutputStream` and channel views included, are bounded as a
socket's are, by `setReadTimeout(Duration)` and `setWriteTimeout(Duration)`
(positive; `clearReadTimeout()`/`clearWriteTimeout()` for no bound, the
default; read back as `readTimeout()`/`writeTimeout()`). An asynchronous form takes no timeout:
`orTimeout` (or `completeOnTimeout`) on its future bounds it and withdraws the
wait. Splice, the secure carrier's `Seal.establish`
and the adapter's `Handled` return `ZmuxFuture` too; only code the
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
