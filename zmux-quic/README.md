# zmux-quic

Netty 4.2 QUIC streams as reliable carriers for zmux. A bidirectional QUIC
stream carries the bootstrap or a joined pipe; extra unidirectional QUIC
streams carry zmux lanes. Logical streams still use the zmux protocol and API.

Requires Java 25 or newer. Add `io.github.zmuxio:zmux-quic` and a runtime
`io.netty:netty-codec-native-quic:4.2.19.Final` dependency with the native
classifier for the deployment platform (for example `osx-aarch_64` or
`linux-x86_64`). Native support must be available; failures are reported rather
than silently selecting a different transport. On the module path, enable
native access with `--enable-native-access=io.netty.common`; on the class path
use `--enable-native-access=ALL-UNNAMED`.

## Client

```java
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.zmux.*;
import io.zmux.quic.*;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

var tls = QuicSslContextBuilder.forClient().applicationProtocols("qmux-02");
try (var endpoint = new QuicClient(tls, QuicOptions.defaults());
     var session = endpoint.connect(new InetSocketAddress("example.com", 9000),
             "example.com", ZmuxConfig.defaults())
             .toCompletableFuture().get(10, TimeUnit.SECONDS)) {
    try (var stream = session.openStream()) {
        stream.write(request);
        stream.finish();
        // Read the response with stream.read(...).
    }
}
```

The client uses the supplied trust configuration and enforces HTTPS server
name verification. The negotiated ALPN must match the session configuration.
`connect` and `openPipe` are cancellable; closing the endpoint fails pending
operations and closes its native connections. Close sessions before their
endpoint: an endpoint hosts the sessions of its connections on its native event
loop, so closing it ends them. Blocking zmux calls are refused on that loop, as
on zmux's own loops; call them from application (for example virtual) threads. `connections()` gives a snapshot of native connections for
transport-specific statistics and control.

## UDP endpoint address families

A client owns an IPv4 UDP endpoint and creates an IPv6 endpoint lazily when
first needed. Both use the same native event loop; concurrent IPv6 dials share
one binding, and client close also reclaims an endpoint whose bind finishes
late. IPv4-mapped addresses use the IPv4 route. Public constructors and pool
limits are unchanged; one client can still dial both address families.

Selecting the actual address family avoids a macOS dual-stack wildcard port
allocation corner case: an IPv6 wildcard socket can receive the same ephemeral
port as an existing IPv4 loopback listener. Handshake replies can then return
to the listener itself. The fix changes socket selection rather than extending
handshake or test deadlines.

## Bidirectional path creation

Bind an application-owned zmux listener when the peer may initiate joined pipes.
The listener owns the session configuration and advertises its locator before
bootstrap parameters are sent:

```java
try (var listener = Zmux.listener(ListenerConfig.of(config));
     var client = new QuicClient(tls, QuicOptions.defaults(), listener)) {
    ZmuxSession session = client.connect(peerAddress, serverName)
            .toCompletableFuture().get(10, TimeUnit.SECONDS);
    // Incoming joined pipes and lanes on every connection use this listener.
}
```

Use the two-argument `connect(address, serverName)` for a listener-backed client;
supplying a second session configuration is rejected. The original constructor
and `connect(address, serverName, config)` remain available for standalone
clients that initiate joined pipes and receive reverse lanes. The client does
not close the application listener. Keep that listener alive while accepting new
incoming carriers. Existing attached pipes remain usable when the listener stops.

A server can open a new bidirectional carrier on a connection it already owns:

```java
var connection = server.connections().getFirst(); // Choose the authenticated peer.
var carrier = server.openPipe(connection).toCompletableFuture()
        .get(10, TimeUnit.SECONDS);
var pipe = session.paths().add(carrier, PipeOptions.defaults());
```

The peer's client must use a bound listener to classify the incoming PIPE_JOIN.
`openPipe` rejects connections owned by another endpoint. Completion delivery
runs outside the native event loop, in order, in the endpoint's completion context. Cancelling the returned future reclaims only
the new stream, including a stream created after cancellation; it does not close
the shared QUIC connection. Once a carrier is returned, transfer it to `paths().add`
or close its halves if it will not be used. This is an explicit carrier opener,
not a connection pool or an independent-path guarantee. Pipes and lanes on one
connection share its transport failure and receive-window estimate. When joining
several pipes of the same joiner on one connection, supply their shared non-zero
`PipeOptions.fateGroup`. For automatic placement and fate-group assignment,
use `QuicPool` below.

A listener-backed client's `openPipe(address, serverName, session)` also keeps
that same incoming routing on the new connection. This enables the ordinary
`Zmux.resolveDuplicate` rejoin callback to join a fresh carrier to an initiated
session. Use the attempt's cancellation when waiting for carrier creation; if
an interrupted wait races a completed future, reclaim the completed carrier too.

## Server

```java
// keys is a configured javax.net.ssl.KeyManagerFactory.
var tls = QuicSslContextBuilder.forServer(keys, keyPassword)
        .applicationProtocols("qmux-02");
try (var listener = Zmux.listener(ListenerConfig.of(config));
     var endpoint = new QuicServer(new InetSocketAddress("0.0.0.0", 9000),
             tls, QuicOptions.defaults(), listener)) {
    ZmuxSession session = listener.accept();
    // Accept and serve logical zmux streams.
}
```

The ordinary listener remains application-owned and can also accept other
carrier kinds. Its existing admission checks, locator map, Lane Key map,
source quotas and shared budget govern QUIC streams too. Its `Admission`
decides on a connection by the client's stream 0, whichever stream arrives
first; the connection's other streams wait and then reach the listener
without a new admission. A client presents a `Credential` on stream 0 of
every connection it opens, in 1-RTT data: `connect` takes the session
configuration's (`ZmuxConfig.credential()`, the bound listener's for a
listener-backed client), `openPipe` and `QuicPool` take one per call. The
Netty QUIC handshake offers no TLS exporter here, so a token bound to it
(`PrefixAuth` with `bindTls`) cannot be presented or verified over QUIC. A source is the
remote IP address, not the QUIC connection ID. Native stream credit bounds
the number of streams awaiting classification. Listener closure leaves
already accepted sessions application-owned; endpoint closure closes every
connection that endpoint owns.

A host that runs its own QUIC endpoint hands each established connection to
`QuicServer.serve(connection, options, listener, fallback)`, on the
connection's loop once its handshake completes: the connection takes a slot of
its source, its first bidirectional stream the listener's admission, then the
inbound transform, as on a `QuicServer` endpoint. Its codec hands streams on
with `QuicServer.streamHandler()`. The returned stage completes when the
connection ends; cancelling it stops handing new streams to the listener and
closes neither the connection nor its sessions. Netty offers no query for
whether a connection used 0-RTT, so a host whose listener has an admission
refuses early data in its TLS configuration.

## Multiple connections and lanes

Enable multipath and configure the same PSK on both peers. Then use
`endpoint.openPipe(address, serverName, session)` and
`session.paths().add(pipeIo, PipeOptions.defaults())` to join another QUIC
connection. A failed connection can be replaced while the zmux session and
its logical streams survive. Connection identity is checked before admitting
lanes; lane IDs are independent in each direction. Lane refusal resets only
the outbound lane it names.

`QuicOptions` configures native receive stream windows, incoming bidirectional
stream credit, incoming lane credit and the native idle timeout. Connection
initial credit is the sum of the configured stream windows. The associated
initial receive-window estimate is reserved once per connection against the
listener budget (or a standalone session budget), shared across attached
pipes and lanes and released when the last attachment leaves. This is an
estimate for attached carriers, **not a bound on total native memory**:
Netty's native flow-control tuning, UDP buffers, TLS state, pending streams
and native send queues also consume memory. Limit native connections and
size deployment budgets accordingly. The zmux protocol still enforces its
own logical stream/session flow control.

## Connection pools

`QuicPool` opens carriers over a route's reusable QUIC connections. It uses a
listener-backed client so one connection can route joins and lanes for several
sessions through the existing listener:

```java
try (var listener = Zmux.listener(ListenerConfig.of(config));
     var client = new QuicClient(tls, QuicOptions.defaults(), listener);
     var pool = new QuicPool(client, peerAddress, serverName,
             new QuicPool.Limits(8, 2000, Duration.ofMinutes(30)))) {
    var carrier = pool.openPipe().toCompletableFuture().get(10, TimeUnit.SECONDS);
    session.paths().add(carrier, PipeOptions.defaults());
}
```

The limits are concurrent pipes per connection (default 8), pipes over a
connection's lifetime (default 0, unlimited), and connection age (default zero,
unlimited). There is no additional connection-count cap. The pool picks the
connection with the most remaining room, considering outstanding creation
reservations too. When none fits, concurrent callers share one pending dial;
additional connections are opened as that connection fills. Native stream
creation and application completion run outside the pool lock.

Each pooled connection supplies one non-zero varint62 fate group through its
`PipeIo` capabilities. In-band `paths().add` uses that group when options leave it
unset; an explicit application group wins, allowing a broader shared path group.
For an out-of-band reservation, choose a non-zero group before signing (the
carrier's default or an application override). A reservation signed with group
zero cannot acquire the pool's default at attach time. Bootstrap pipe 0 keeps its protocol-defined
group 0. `connections()` returns immutable policy snapshots with the native
connection, group, active reservation count, total, age and retirement flag.
`total` conservatively includes failed native opening attempts; it counts a
created stream even if its application notification was cancelled.

A stream's native close future releases its active slot, including cancelled or
failed creation. Returning both carrier halves completes their shutdown before
closing the local channel; one half alone does not close the other. Reaching an
age or lifetime limit stops new allocation on that connection. Existing streams
continue and the connection closes after the last one ends. An idle connection
past its age limit also closes without another pool call.

`new QuicPool(client, address, serverName, limits, credential, policy)` takes a
`PoolPolicy`, the same `View`/`Decision` model as `PipePool.withPolicy`. Each
member is a connection that takes new pipes: its fate group as ID, its age, the
time since its last pipe ended as idle, its active pipes, and zero byte counts.
The policy is asked when a pipe is taken or released and after a connection is
added. A nonzero `count` is the connections to spread pipes over: while fewer
are open and `dialAfter` has passed since the last dial started, the next pipe
goes to a new connection, or the pool opens one itself. `dialAfter` spaces only
these policy dials; a pipe with no room anywhere still dials at once. A retired
connection takes no new pipe and a replacement is dialled; its sessions keep
their streams, and it closes after the last one ends. A null decision or an
exception keeps the pool's own behaviour.

Pool `close()` stops new requests, fails uncommitted requests and retires its
connections. A concurrently completed carrier may have committed before close;
it remains the caller's responsibility. Already handed-out carriers and their
sessions continue until their streams end. The pool does not close its client
endpoint or application listener. To force all underlying connections to stop,
close their owning client. Use explicit cancellation of an unfinished open and
return the carrier's halves when it will not be joined.

Pipe maintenance can use `PipePool.carriers` with a factory that awaits
`pool.openPipe()` and propagates cancellation/interruption to that future. If
cancellation races a completed open, reclaim the completed carrier too. This
reuses maintenance's existing scoped ownership and retry policy.

These are connection-lifecycle and wire-grouping guarantees. Creating more
streams does not establish more independent physical bandwidth. Existing
per-pipe rate estimation and scheduling are unchanged; shared-connection
aggregate prediction and loaded multipath performance remain separate work.

## Current boundaries

The adapter deliberately operates after the QUIC handshake. Constructors
accept TLS builders and force `earlyData(false)` on both peers; transport
0-RTT is unavailable. The adapter does not invent an early-data outcome from
a successful handshake. Protocol-level early opening over an established
carrier remains governed by the zmux configuration.

Netty's supported public API does not provide this adapter with a TLS
exporter. Use PSK authentication for multipath joins; exporter-bound joins
are unavailable. Do not use the explicitly insecure mode on untrusted paths.

Standalone clients accept reverse unidirectional lanes. Listener-backed clients
also route server-initiated joined pipes; servers can open them on their accepted
connections. The connections of an externally owned endpoint are served with
`QuicServer.serve` (see [Server](#server)).
Cross-language interoperability and remote platform validation are separate
release gates.
