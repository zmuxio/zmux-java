#!/usr/bin/env sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
# Keep this independent of artifact versions and optional native transports.
timeout --kill-after=5s 600 mvn -B -pl zmux -am -DskipTests package
timeout --kill-after=5s 330 java --class-path zmux/target/classes tools/Benchmark.java "$@"
