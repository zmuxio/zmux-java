#!/usr/bin/env sh
set -eu
repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
# Keep this independent of artifact versions and optional native transports.
timeout --kill-after=5s 600 mvn -B -q -pl zmux-bench -am -DskipTests package dependency:build-classpath -Dmdep.outputFile=target/bench.classpath -Dmdep.includeScope=runtime
timeout --kill-after=5s 330 java --class-path "zmux-bench/target/classes:$(cat zmux-bench/target/bench.classpath)" io.zmux.bench.Benchmark "$@"
