#!/usr/bin/env sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
# The caller owns this process; exec keeps Process.destroy targeting the actual JVM.
# InteropPeer has its own 90-second watchdog; matrix commands also have hard timeouts.
classes="$repo_root/zmux/target/classes:$repo_root/zmux/target/test-classes"
case "${1:-}" in
quic-*|wss-*)
  # The QUIC and WebSocket test classes keep their certificate under their module's target directory.
  case "$1" in quic-*) module=zmux-quic ;; *) module=zmux-http ;; esac
  cd "$repo_root/$module"
  classes="$classes:target/classes:target/test-classes:$repo_root/zmux-netty-test/target/classes"
  version=$(sed -n 's:.*<netty.version>\(.*\)</netty.version>.*:\1:p' "$repo_root/pom.xml")
  repo="${MAVEN_REPOSITORY:-$HOME/.m2/repository}/io/netty"
  for jar in "$repo"/netty-*/"$version"/*"$version"*.jar "$repo"/netty-tcnative-classes/*/*.jar; do
    case "$jar" in *-sources.jar|*-javadoc.jar) ;; *) classes="$classes:$jar" ;; esac
  done
  exec java --enable-native-access=ALL-UNNAMED -cp "$classes" io.zmux.InteropPeer "$@"
  ;;
tls-*)
  # The TLS server's certificate is made under the working directory's target/tls.
  cd "$repo_root/zmux"
  ;;
esac
exec java -cp "$classes" io.zmux.InteropPeer "$@"
