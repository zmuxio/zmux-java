#!/usr/bin/env sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
# The caller owns this process; exec keeps Process.destroy targeting the actual JVM.
# InteropPeer has its own 90-second watchdog; matrix commands also have hard timeouts.
exec java -cp "$repo_root/zmux/target/classes:$repo_root/zmux/target/test-classes" io.zmux.InteropPeer "$@"
