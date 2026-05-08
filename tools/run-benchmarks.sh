#!/usr/bin/env sh
set -eu

includes='io.zmux.benchmarks.CodecBenchmark|io.zmux.OrdinaryBatchOrdererBenchmark|io.zmux.FlowControlRegistryBenchmark'
quick=0
result_file='zmux-benchmarks/target/jmh-result.json'

usage() {
    cat <<'USAGE'
Usage: tools/run-benchmarks.sh [options]

Options:
  --includes REGEX       JMH benchmark include regex
  --quick                use a short smoke profile
  --result-file PATH     JSON result file path
  -h, --help             show this help
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --includes)
            [ "$#" -ge 2 ] || { echo "--includes requires a value" >&2; exit 2; }
            includes=$2
            shift 2
            ;;
        --quick)
            quick=1
            shift
            ;;
        --result-file)
            [ "$#" -ge 2 ] || { echo "--result-file requires a value" >&2; exit 2; }
            result_file=$2
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

mvn -B -Pbenchmarks -pl zmux-benchmarks -am -DskipTests package

jar="$repo_root/zmux-benchmarks/target/benchmarks.jar"
if [ ! -f "$jar" ]; then
    echo "Benchmark jar was not generated: $jar" >&2
    exit 1
fi

set -- -jar "$jar" "$includes" -rf json -rff "$result_file"
if [ "$quick" -eq 1 ]; then
    set -- "$@" -wi 1 -i 2 -w 500ms -r 500ms -f 1
fi

java "$@"
