#!/usr/bin/env sh
# Requires sibling repositories; never silently skips missing peers.
set -eu
repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
: "${ZMUX_RS_ROOT:?set ZMUX_RS_ROOT to the zmux-rs checkout}"
: "${ZMUX_GO_ROOT:?set ZMUX_GO_ROOT to the zmux-go checkout}"
# One spelling of the Go checkout: a relative or symlinked path would name the same module
# twice in the generated go.work (ambiguous import).
ZMUX_GO_ROOT=$(CDPATH='' cd -- "$ZMUX_GO_ROOT" && pwd -P)
mkdir -p "$repo_root/target/interop"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$repo_root/target/interop/rust-target}"
timeout --kill-after=5s 600s cargo build --release --locked --manifest-path "$ZMUX_RS_ROOT/interop/peer/Cargo.toml"
timeout --kill-after=5s 600s cargo build --release --locked --manifest-path "$ZMUX_RS_ROOT/interop/qmux-peer/Cargo.toml"
# The WebSocket and HTTP streaming peers: one crate with two binaries, one Go build with two commands.
timeout --kill-after=5s 600s cargo build --release --locked --manifest-path "$ZMUX_RS_ROOT/interop/web-peer/Cargo.toml" --target-dir "$CARGO_TARGET_DIR"
# The zmux-go peer modules replace zmux-go with the checkout they live in; a released tree may
# still require the previous release, so let go settle the requirement instead of failing.
GOFLAGS=-mod=mod "$ZMUX_GO_ROOT/interop/webpeer/build.sh" "$repo_root/target/interop/web-go"
# The QUIC datagram-channel peers (zmux-go interop/dgpeer, zmux-rs interop/datagram-peer); a checkout without one
# (an older release tag) skips its QuicChannelInteropTest cases with a notice.
if [ -f "$ZMUX_GO_ROOT/interop/dgpeer/build.sh" ]; then
  GOFLAGS=-mod=mod "$ZMUX_GO_ROOT/interop/dgpeer/build.sh" "$repo_root/target/interop/dgpeer-go"
  export ZMUX_GO_QUIC_CHANNEL_PEER="$repo_root/target/interop/dgpeer-go/dgpeer"
else
  echo "notice: zmux-go has no interop/dgpeer; the Go QuicChannelInteropTest cases are skipped" >&2
fi
if [ -f "$ZMUX_RS_ROOT/interop/datagram-peer/Cargo.toml" ]; then
  timeout --kill-after=5s 600s cargo build --release --locked --manifest-path "$ZMUX_RS_ROOT/interop/datagram-peer/Cargo.toml" --target-dir "$CARGO_TARGET_DIR"
  export ZMUX_RS_QUIC_CHANNEL_PEER="$CARGO_TARGET_DIR/release/zmux-datagram-peer"
else
  echo "notice: zmux-rs has no interop/datagram-peer; the Rust QuicChannelInteropTest cases are skipped" >&2
fi
(cd "$repo_root/interop/qmux-go-peer" && timeout --kill-after=5s 180s go build -o "$repo_root/target/interop/qmux-go-peer" .)
# Use the standalone peer lock to exercise the published Go Establish implementation.
(cd "$repo_root/interop/seal-go-peer" && GOWORK=off timeout --kill-after=5s 180s go build -mod=readonly -o "$repo_root/target/interop/seal-go-peer" .)
export ZMUX_INTEROP_PEER="$CARGO_TARGET_DIR/release/zmux-interop-peer"
export ZMUX_QMUX_RS_PEER="$CARGO_TARGET_DIR/release/qmux-peer"
export ZMUX_QMUX_GO_PEER="$repo_root/target/interop/qmux-go-peer"
export ZMUX_SEAL_INTEROP_PEER="$repo_root/target/interop/seal-go-peer"
export ZMUX_RS_WS_PEER="$CARGO_TARGET_DIR/release/zmux-ws-peer"
export ZMUX_RS_HTTP_PEER="$CARGO_TARGET_DIR/release/zmux-http-peer"
export ZMUX_GO_WS_PEER="$repo_root/target/interop/web-go/wspeer"
export ZMUX_GO_HTTP_PEER="$repo_root/target/interop/web-go/httppeer"
export ZMUX_GO_TLS_FRONT="$repo_root/target/interop/web-go/tlsfront"
(cd "$repo_root" && timeout --kill-after=5s 300s "$repo_root/mvnw" -B -pl zmux -am -Dzmux.bufferTracking=true -Dtest=PeerInterop,QmuxInterop,SealPeerInterop test)
# Maven must have actually executed every role in both suites.
python3 - "$repo_root" <<'CHECK'
from pathlib import Path
import sys, xml.etree.ElementTree as ET
for name, tests in (('PeerInterop', 4), ('QmuxInterop', 4), ('SealPeerInterop', 8)):
    root = ET.parse(Path(sys.argv[1]) / f'zmux/target/surefire-reports/TEST-io.zmux.{name}.xml').getroot()
    assert (int(root.attrib['tests']), int(root.attrib['failures']), int(root.attrib['errors']), int(root.attrib['skipped'])) == (tests, 0, 0, 0), name
CHECK
# QUIC against the Rust scripted peer (ZMUX_INTEROP_PEER) and the datagram-channel peers, and WebSocket and HTTP streaming against both peers,
# with the Bound joins over TLS and the Sealed ones behind a TLS front.
(cd "$repo_root" && timeout --kill-after=5s 300s "$repo_root/mvnw" -B -pl zmux-quic,zmux-http -am -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='QuicPeerInterop,QuicChannelInteropTest,HttpInteropTest#webSocketWith*+streamingWith*+bound*+sealedBehindTlsFront' test)
python3 - "$repo_root" <<'CHECK'
from pathlib import Path
import os, sys, xml.etree.ElementTree as ET
# QuicChannelInteropTest has three cases per peer; the cases of a peer that was not built are skipped (notice above).
peers = sum(1 for v in ('ZMUX_GO_QUIC_CHANNEL_PEER', 'ZMUX_RS_QUIC_CHANNEL_PEER') if os.environ.get(v))
for module, name, tests, skipped in (('zmux-quic', 'io.zmux.quic.QuicPeerInterop', 4, 0),
                                     ('zmux-quic', 'io.zmux.quic.QuicChannelInteropTest', 6, 6 - 3 * peers),
                                     ('zmux-http', 'io.zmux.http.HttpInteropTest', 7, 0)):
    root = ET.parse(Path(sys.argv[1]) / module / f'target/surefire-reports/TEST-{name}.xml').getroot()
    assert (int(root.attrib['tests']), int(root.attrib['failures']), int(root.attrib['errors']), int(root.attrib['skipped'])) == (tests, 0, 0, skipped), name
CHECK
# Packet streams of the Go suite against the Rust peer (PeerInterop covers Java against it).
(cd "$ZMUX_GO_ROOT" && timeout --kill-after=5s 120s go test -race . -run '^TestInteropPackets$' -count=1 -timeout=100s)
# Existing Go scripted scenarios drive the Java peer in both ordinary and MP roles, and its packet streams.
export ZMUX_INTEROP_PEER="$repo_root/tools/interop-peer.sh"
(cd "$ZMUX_GO_ROOT" && timeout --kill-after=5s 180s go test -race . -run '^TestInterop(Client|Server|MultipathClient|MultipathServer|Packets)$' -count=1 -timeout=160s)
# The TLS exporter, secure carrier Establish and WebSocket carriers of the Go suites, driving the Java peer.
(cd "$ZMUX_GO_ROOT" && timeout --kill-after=5s 180s go test -race . -run '^TestInteropTLSExporter' -count=1 -timeout=160s)
(cd "$ZMUX_GO_ROOT/seal" && timeout --kill-after=5s 180s go test -race . -run '^TestInteropEstablish' -count=1 -timeout=160s)
# Not run, a gap of the Java library and not of the peer: the 0-RTT cases of zmuxquic (TestInteropQUICServer and the
# two TestInteropQUICExporterEarly; the QUIC adapter keeps 0-RTT off).
(cd "$ZMUX_GO_ROOT/zmuxws" && timeout --kill-after=5s 180s go test -race . -run '^TestInteropWSS(Client|Server)$' -count=1 -timeout=160s)
(cd "$ZMUX_GO_ROOT/zmuxquic" && timeout --kill-after=5s 180s go test -race . -run '^TestInteropQUICClient$' -count=1 -timeout=160s)
