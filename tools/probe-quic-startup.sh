#!/usr/bin/env sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
rm -f zmux-quic/target/surefire-reports/TEST-io.zmux.quic.QuicStartupProbe.xml
timeout --kill-after=5s 660s mvn -B -pl zmux-quic -am \
  -Dtest=QuicStartupProbe -Dsurefire.failIfNoSpecifiedTests=false \
  -Dzmux.startup.diagnostics="$repo_root/target/quic-startup" test "$@"
python3 - <<'CHECK'
import xml.etree.ElementTree as ET
r = ET.parse('zmux-quic/target/surefire-reports/TEST-io.zmux.quic.QuicStartupProbe.xml').getroot()
assert tuple(int(r.attrib[k]) for k in ('tests','failures','errors','skipped')) == (1,0,0,0)
CHECK
