#!/bin/sh
# The Java peer of the secure-carrier matrix: matrix-server <scenario> | matrix-client <scenario> <addr> ...
# Needs JDK 25 (java on PATH or JAVA_HOME) and a prior `mvn -pl zmux -am -DskipTests test-compile`.
set -e
dir=$(cd "$(dirname "$0")/../zmux" && pwd)
java=${JAVA_HOME:+$JAVA_HOME/bin/}java
cd "$dir"   # The test certificate is kept under target/tls.
exec "$java" -cp target/classes:target/test-classes io.zmux.MatrixPeer "$@"
