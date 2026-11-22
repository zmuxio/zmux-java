#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 2 ]]; then
    echo "usage: $0 ZMUX_RS_CHECKOUT OUTPUT_BINARY" >&2
    exit 2
fi

src=$(cd "$1" && pwd -P)
peer=$(cd -- "$(dirname -- "$0")" && pwd -P)
output=$(cd -- "$(dirname -- "$2")" && pwd -P)/$(basename -- "$2")
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
# Copy tracked files with their working-tree contents, leaving the checkout intact.
git -C "$src" ls-files -z | rsync -a --from0 --files-from=- "$src/" "$work/"
mkdir -p "$work/interop/java-h3-wire-peer"
cp "$peer"/{Cargo.toml,main.rs} "$work/interop/java-h3-wire-peer/"
python3 - "$work" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
manifest = root / "Cargo.toml"
text = manifest.read_text()
section = "[workspace]\nmembers = ["
assert text.count(section) == 1, "unrecognized Rust workspace manifest"
manifest.write_text(text.replace(section, section + '\n    "interop/java-h3-wire-peer",', 1))
lock = root / "Cargo.lock"
with lock.open("a") as f:
    f.write("""
[[package]]
name = "zmux-java-h3-wire-peer"
version = "0.0.0"
dependencies = [
 "bytes",
 "h3",
 "h3-quinn",
 "http",
 "quinn",
 "tokio",
 "zmux-h3",
 "zmux-webtransport",
]
""")
PY
# The extra lock entry has only workspace edges and cannot change the locked external crates.
target=${CARGO_TARGET_DIR:-"$work/target"}
cargo build --locked --manifest-path "$work/Cargo.toml" --target-dir "$target" -p zmux-java-h3-wire-peer
cp "$target/debug/zmux-java-h3-wire-peer" "$output"
