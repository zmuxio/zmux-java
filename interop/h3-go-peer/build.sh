#!/bin/sh
# Build against the supplied zmux-go checkout and its zmuxh3 dependency lock.
set -eu
if [ "$#" -ne 2 ]; then
    echo "usage: $0 ZMUX_GO_CHECKOUT OUTPUT" >&2
    exit 2
fi
checkout=$(cd "$1" && pwd)
output=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
source=$(cd "$(dirname "$0")" && pwd)
build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT
cp "$source/main.go" "$build"
cp "$checkout/zmuxh3/go.mod" "$checkout/zmuxh3/go.sum" "$build"
cd "$build"
export GOWORK=off
go mod edit -module=github.com/zmuxio/zmux-java/interop/h3-go-peer \
    -require=github.com/zmuxio/zmux-go/zmuxh3@v0.0.0 \
    -replace="github.com/zmuxio/zmux-go=$checkout" \
    -replace="github.com/zmuxio/zmux-go/zmuxh3=$checkout/zmuxh3"
go build -mod=readonly -o "$output" .
