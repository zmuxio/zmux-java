# qmux-go-peer

A scripted plain QMux peer built on qmux-go (QMux draft-02) for the Java
interoperability tests. It sends no zmux transport parameters. Its command
line, output and command protocol are those of the zmux-rs
`interop/qmux-peer` helper (see its README), with these differences:

- `--rtt-ms <ms>` sets qmux-go's keep-alive period, which is also how it
  measures the RTT (default 1000);
- the peer does not send `reset_stream_at`, so a reset towards it is a plain
  RESET_STREAM;
- `datagram-size` replies `max=unknown`.

```
go build -o ../../zmux/target/interop/qmux-go-peer .
```

The Java test `QmuxInteropTest` builds it when `ZMUX_INTEROP=1` and Go is
installed.
