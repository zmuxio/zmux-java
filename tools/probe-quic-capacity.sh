#!/usr/bin/env sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
# This name intentionally does not match Surefire's default *Test pattern.
# A stale report must not satisfy the execution receipt below.
rm -f zmux-quic/target/surefire-reports/TEST-io.zmux.quic.QuicCapacityProbe.xml
timeout --kill-after=5s 600s mvn -B -pl zmux-quic -am \
  -Dtest=QuicCapacityProbe -Dsurefire.failIfNoSpecifiedTests=false test "$@"
# The upstream core module has no such test. Verify that the selected adapter
# module really executed it, so an absent/renamed probe cannot produce a pass.
python3 - <<'CHECK'
import xml.etree.ElementTree as ET
r = ET.parse('zmux-quic/target/surefire-reports/TEST-io.zmux.quic.QuicCapacityProbe.xml').getroot()
assert tuple(int(r.attrib[k]) for k in ('tests', 'failures', 'errors', 'skipped')) == (1, 0, 0, 0)
CHECK
