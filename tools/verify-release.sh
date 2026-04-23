#!/usr/bin/env sh
set -eu

go_root="${ZMUX_GO_ROOT:-}"
spec_root="${ZMUX_SPEC_ROOT:-}"
run_interop=0
run_benchmarks=0
skip_release_profile=0

usage() {
    cat <<'USAGE'
Usage: tools/verify-release.sh [options]

Options:
  --go-root PATH             zmux-go repository path
  --spec-root PATH           zmux-spec repository path
  --run-interop              run optional Java core <-> Go core smoke
  --run-benchmarks           run quick JMH benchmarks
  --skip-release-profile     skip Maven release profile verification
  -h, --help                 show this help
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --go-root)
            [ "$#" -ge 2 ] || { echo "--go-root requires a value" >&2; exit 2; }
            go_root=$2
            shift 2
            ;;
        --spec-root)
            [ "$#" -ge 2 ] || { echo "--spec-root requires a value" >&2; exit 2; }
            spec_root=$2
            shift 2
            ;;
        --run-interop)
            run_interop=1
            shift
            ;;
        --run-benchmarks)
            run_benchmarks=1
            shift
            ;;
        --skip-release-profile)
            skip_release_profile=1
            shift
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

step() {
    printf '\n==> %s\n' "$1"
}

resolve_dir() {
    [ -n "$1" ] || return 1
    CDPATH= cd -- "$1" && pwd
}

find_python() {
    if [ -n "${PYTHON:-}" ]; then
        printf '%s\n' "$PYTHON"
        return
    fi
    if command -v python3 >/dev/null 2>&1 && python3 --version >/dev/null 2>&1; then
        printf '%s\n' python3
        return
    fi
    if command -v python >/dev/null 2>&1 && python --version >/dev/null 2>&1; then
        printf '%s\n' python
        return
    fi
    echo "python3 or python was not found on PATH" >&2
    exit 1
}

cd "$repo_root"

step "Java clean compile"
mvn -B clean compile

step "Java reactor tests"
mvn -B test

if [ -n "$go_root" ]; then
    resolved_go_root=$(resolve_dir "$go_root")
    step "Go core tests"
    (cd "$resolved_go_root" && go test ./...)

    go_adapter_root="$resolved_go_root/adapter/quicmux"
    if [ -d "$go_adapter_root" ]; then
        step "Go QUIC adapter tests"
        (cd "$go_adapter_root" && go test ./...)
    fi
else
    echo "Skipping Go tests because --go-root / ZMUX_GO_ROOT was not provided."
fi

if [ -n "$spec_root" ]; then
    resolved_spec_root=$(resolve_dir "$spec_root")
    asset_validator="$resolved_spec_root/tools/validate_assets.py"
    if [ -f "$asset_validator" ]; then
        step "Spec asset validation"
        python_cmd=$(find_python)
        (cd "$resolved_spec_root" && "$python_cmd" tools/validate_assets.py)
    else
        echo "Skipping spec asset validation because tools/validate_assets.py was not found under $resolved_spec_root."
    fi
else
    echo "Skipping spec asset validation because --spec-root / ZMUX_SPEC_ROOT was not provided."
fi

if [ "$run_interop" -eq 1 ]; then
    if [ -z "$go_root" ]; then
        echo "--run-interop requires --go-root or ZMUX_GO_ROOT" >&2
        exit 2
    fi
    resolved_go_root=$(resolve_dir "$go_root")
    step "Java core <-> Go core smoke"
    ZMUX_INTEROP=1 ZMUX_GO_ROOT="$resolved_go_root" \
        mvn -B -pl zmux clean test -Dtest=GoInteropSmokeTest
fi

if [ "$run_benchmarks" -eq 1 ]; then
    step "JMH quick benchmarks"
    sh "$repo_root/tools/run-benchmarks.sh" --quick
fi

if [ "$skip_release_profile" -eq 0 ]; then
    step "Release profile verify"
    mvn -B -Prelease -Dgpg.skip=true -DskipTests verify
fi
