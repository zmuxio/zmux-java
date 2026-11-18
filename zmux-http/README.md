# zmux-http

HTTP carriers for zmux on Netty 4.2: a zmux session runs over HTTP requests
where a plain TCP or TLS connection does not get through. The server side is an
`HttpEndpoint` in front of a `ZmuxListener`; the client side is an
`HttpCarrierClient` that dials carriers and hands them to `Zmux.open`. Logical
streams still use the zmux protocol and API.

Requires Java 25 or newer. Add `io.github.zmuxio:zmux-http`; it brings Netty's
HTTP/1.1, HTTP/2 and WebSocket codecs.

Carrier kinds, each enabled on its own on the endpoint:

- **streaming** (`streaming()`): one full-duplex request, HTTP/1.1 or HTTP/2;
  also the uplink and downlink of a **split** carrier (a POST and a GET paired
  by a header), for paths that buffer one direction.
- **exchange** (`exchange(ExchangeConfig)`): a sequence of bounded POST
  exchanges authenticated with a pre-shared key
  (`ExchangeConfig.builder(psk)`), for paths that buffer whole bodies.
- **WebSocket** (`webSocket(protocols...)`): an HTTP/1.1 upgrade, one binary
  message per record.

## Minimal example

A cleartext server and a client in one process; the client opens a streaming
carrier, the server echoes one stream.

```java
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.zmux.*;
import io.zmux.http.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

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

`endpoint.initializer(ssl, host)` builds the server pipeline: with an
`SslContext` (`HttpEndpoint.serverSsl(builder)` sets ALPN h2 and http/1.1) it
serves TLS, without one cleartext HTTP/1.1 and HTTP/2 by prior knowledge;
`host` supplies the handler for every request that is not a carrier (null: a
404). `endpoint.handler()` mounts the endpoint in an existing pipeline after
the HTTP codec instead.

## Client

`HttpCarrierClient` dials one carrier per call and returns a `PipeIo`:
`pipe(url, headers, direction, credential)` (streaming),
`splitPipe(url, headers, credential)`, `exchangePipe(urlTemplate, config)` and
`webSocketPipe(url, headers, protocols, credential)`. Each kind is also a
`Candidate` for `Zmux.dial`, which picks the first kind that works
(`streamingCandidate`, `splitCandidate`, `exchangeCandidate`,
`webSocketCandidate`). `splitJoin(session, url, headers, credential, options)`
joins a split carrier as a further pipe of a multipath session; a split
bootstrap is pipe 0 only. `tls(SslContextBuilder)` sets the trust
configuration, `http1()` or `http2()` fixes the version, and `timeout(Duration)`
bounds a dial.

`close()` fails its exchange carriers with the client's close as the cause,
closes every connection the client opened (those of carriers in use and those
still connecting too), and shuts down its own event loop group; it returns
once that is done, except when called on one of the group's event loops. A client built on your
group with `group(EventLoopGroup)` leaves the group running: close the client
first, then shut the group down, as the example above does. When the group
shuts down first, calls on the client's carriers fail at once and the group's
shutdown closes their connections.

Streaming, split and WebSocket requests follow redirects on the same host name
(another path, or an upgrade from http to https), at most five hops; any other
redirect fails the carrier with `HttpRedirectException`, a status other than
2xx with `HttpStatusException`. A carrier that fails after it was set up (an
exchange carrier's peer forgot it, its deadline passed, its framing was
violated) reports `CarrierFailedException`. All of them are `ZmuxException`s.

## Server

A request is classified on its head (`HttpClass.classify`): a carrier of an
enabled kind passes the listener's admission before anything is written; any
other request, and one the admission gives back, goes to the host's handler
with every body byte replayed. Every carrier takes the listener's source and
pending slots like its TCP carriers; a split bootstrap holds one slot per half
until the pair is complete, so `maxPendingPerSource` is at least 2. Behind a
proxy, `trusted(TrustedProxies)` takes the client address from the PROXY
protocol or a forwarding header of a trusted hop, and a host that resolves the
address itself sets `HttpEndpoint.SOURCE` on the connection's channel.

An application that runs another HTTP server hands each carrier to the listener
itself, through `listener.adapter().handle(io, hint, fallback)`
(`io.zmux.adapter.ListenerAdapter`), with a `JoinHint` built from the request
head.
