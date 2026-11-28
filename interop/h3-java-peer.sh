#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${2:-} == wt ]]; then
    module=zmux-http3
    test_class=io.zmux.http3.WebTransportProtocolTest
    main_class=io.zmux.http3.Http3ProductionPeer
elif [[ ${ZMUX_H3_PRODUCTION:-0} == 1 ]]; then
    module=zmux-http3
    test_class=io.zmux.http3.Http3CarrierTest
    main_class=io.zmux.http3.Http3ProductionPeer
else
    module=zmux-quic
    test_class=io.zmux.quic.H3WirePeerTest
    main_class=io.zmux.quic.H3WirePeer
fi
report="$root/$module/target/surefire-reports/TEST-$test_class.xml"

# Ask Maven/Surefire for the exact resolved test classpath, including the current
# platform's QUIC native classifier, rather than reconstructing it from ~/.m2.
if [[ ! -r "$report" ]]; then
    echo "Build the Java peer classpath first from the repository root:" >&2
    echo "  ./mvnw -T 2 -Dsurefire.forkCount=1 -pl $module -am test -Dtest=${test_class##*.} -Dsurefire.failIfNoSpecifiedTests=false" >&2
    exit 2
fi
classpath=$(python3 - "$report" <<'PY'
import sys
import xml.etree.ElementTree as ET

report = ET.parse(sys.argv[1])
entry = report.find("./properties/property[@name='java.class.path']")
if entry is None or not entry.get("value"):
    sys.exit("Surefire did not report the Java test classpath")
print(entry.get("value"))
PY
)

exec "${JAVA_HOME:+$JAVA_HOME/bin/}java" --enable-native-access=ALL-UNNAMED \
    -cp "$classpath" "$main_class" "$@"
