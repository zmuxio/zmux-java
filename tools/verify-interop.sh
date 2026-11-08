#!/usr/bin/env sh
# Requires sibling repositories; never silently skips missing peers.
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
: "${ZMUX_RS_ROOT:?set ZMUX_RS_ROOT to the zmux-rs checkout}"
: "${ZMUX_GO_ROOT:?set ZMUX_GO_ROOT to the zmux-go checkout}"
# One spelling of the Go checkout: a relative or symlinked path would name the same module
# twice in the generated go.work (ambiguous import).
ZMUX_GO_ROOT=$(CDPATH= cd -- "$ZMUX_GO_ROOT" && pwd -P)
mkdir -p "$repo_root/target/interop"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$repo_root/target/interop/rust-target}"
timeout --kill-after=5s 600s cargo build --release --locked --manifest-path "$ZMUX_RS_ROOT/interop/peer/Cargo.toml"
timeout --kill-after=5s 600s cargo build --release --locked --manifest-path "$ZMUX_RS_ROOT/interop/qmux-peer/Cargo.toml"
(cd "$repo_root/interop/qmux-go-peer" && timeout --kill-after=5s 180s go build -o "$repo_root/target/interop/qmux-go-peer" .)
# The Establish peer builds against the zmux-go checkout itself, through a workspace.
printf 'go 1.27\n\nuse (\n\t%s\n\t%s\n\t%s\n)\n' "$repo_root/interop/seal-go-peer" "$ZMUX_GO_ROOT" "$ZMUX_GO_ROOT/seal" > "$repo_root/target/interop/go.work"
(cd "$repo_root/interop/seal-go-peer" && GOWORK="$repo_root/target/interop/go.work" timeout --kill-after=5s 180s go build -o "$repo_root/target/interop/seal-go-peer" .)
export ZMUX_INTEROP_PEER="$CARGO_TARGET_DIR/release/zmux-interop-peer"
export ZMUX_QMUX_RS_PEER="$CARGO_TARGET_DIR/release/qmux-peer"
export ZMUX_QMUX_GO_PEER="$repo_root/target/interop/qmux-go-peer"
export ZMUX_SEAL_INTEROP_PEER="$repo_root/target/interop/seal-go-peer"
(cd "$repo_root" && timeout --kill-after=5s 300s mvn -B -pl zmux -am -Dtest=PeerInterop,QmuxInterop,SealPeerInterop test)
# Maven must have actually executed every role in both suites.
python3 - "$repo_root" <<'CHECK'
from pathlib import Path
import sys, xml.etree.ElementTree as ET
for name, tests in (('PeerInterop', 4), ('QmuxInterop', 4), ('SealPeerInterop', 8)):
    root = ET.parse(Path(sys.argv[1]) / f'zmux/target/surefire-reports/TEST-io.zmux.{name}.xml').getroot()
    assert (int(root.attrib['tests']), int(root.attrib['failures']), int(root.attrib['errors']), int(root.attrib['skipped'])) == (tests, 0, 0, 0), name
CHECK
# Existing Go scripted scenarios drive the Java peer in both ordinary and MP roles.
export ZMUX_INTEROP_PEER="$repo_root/tools/interop-peer.sh"
(cd "$ZMUX_GO_ROOT" && timeout --kill-after=5s 180s go test -race . -run '^TestInterop(Client|Server|MultipathClient|MultipathServer)$' -count=1 -timeout=160s)
