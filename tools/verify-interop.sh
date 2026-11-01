#!/usr/bin/env sh
# Requires sibling redesign repositories; never silently skips missing peers.
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
: "${ZMUX_RS_ROOT:?set ZMUX_RS_ROOT to the zmux-rs checkout}"
: "${ZMUX_GO_ROOT:?set ZMUX_GO_ROOT to the zmux-go checkout}"
mkdir -p "$repo_root/target/interop"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$repo_root/target/interop/rust-target}"
timeout --kill-after=5s 600s cargo build --release --locked --manifest-path "$ZMUX_RS_ROOT/interop/peer/Cargo.toml"
timeout --kill-after=5s 600s cargo build --release --locked --manifest-path "$ZMUX_RS_ROOT/interop/qmux-peer/Cargo.toml"
(cd "$repo_root/interop/qmux-go-peer" && timeout --kill-after=5s 180s go build -o "$repo_root/target/interop/qmux-go-peer" .)
export ZMUX_INTEROP_PEER="$CARGO_TARGET_DIR/release/zmux-interop-peer"
export ZMUX_QMUX_RS_PEER="$CARGO_TARGET_DIR/release/qmux-peer"
export ZMUX_QMUX_GO_PEER="$repo_root/target/interop/qmux-go-peer"
(cd "$repo_root" && timeout --kill-after=5s 240s mvn -B -pl zmux -am -Dtest=PeerInterop,QmuxInterop test)
# Maven must have actually executed every role in both suites.
python3 - "$repo_root" <<'CHECK'
from pathlib import Path
import sys, xml.etree.ElementTree as ET
for name in ('PeerInterop', 'QmuxInterop'):
    root = ET.parse(Path(sys.argv[1]) / f'zmux/target/surefire-reports/TEST-io.zmux.{name}.xml').getroot()
    assert (int(root.attrib['tests']), int(root.attrib['failures']), int(root.attrib['errors']), int(root.attrib['skipped'])) == (4, 0, 0, 0), name
CHECK
# Existing Go scripted scenarios drive the Java peer in both ordinary and MP roles.
export ZMUX_INTEROP_PEER="$repo_root/tools/interop-peer.sh"
(cd "$ZMUX_GO_ROOT" && timeout --kill-after=5s 180s go test -race . -run '^TestInterop(Client|Server|MultipathClient|MultipathServer)$' -count=1 -timeout=160s)
