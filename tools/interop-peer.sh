#!/usr/bin/env sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
# The caller owns this process; exec keeps Process.destroy targeting the actual JVM.
# InteropPeer has its own 90-second watchdog; matrix commands also have hard timeouts.
classes="$repo_root/zmux/target/classes:$repo_root/zmux/target/test-classes"
case "${1:-}" in
quic-*)
  # The QUIC test classes keep their certificate under the module's target directory.
  cd "$repo_root/zmux-quic"
  classes="$classes:target/classes:target/test-classes"
  version=$(sed -n 's:.*<netty.version>\(.*\)</netty.version>.*:\1:p' "$repo_root/pom.xml")
  repo="${MAVEN_REPOSITORY:-$HOME/.m2/repository}/io/netty"
  for jar in "$repo"/netty-*/"$version"/*"$version"*.jar "$repo"/netty-tcnative-classes/*/*.jar; do
    case "$jar" in *-sources.jar|*-javadoc.jar) ;; *) classes="$classes:$jar" ;; esac
  done
  exec java --enable-native-access=ALL-UNNAMED -cp "$classes" io.zmux.InteropPeer "$@"
  ;;
esac
exec java -cp "$classes" io.zmux.InteropPeer "$@"
