#!/usr/bin/env bash
# Sets the version properties of the parent pom to the newest stable release on Maven Central (pre-releases such as
# alpha, beta, milestone and release candidates are skipped), for the weekly build against the newest dependencies.
# The working tree's pom.xml is changed; nothing is committed.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
central="https://repo1.maven.org/maven2"

# property group/artifact
set_latest() {
    local property="$1" path="$2" latest
    latest="$(curl -fsS --max-time 60 "$central/$path/maven-metadata.xml" |
        sed -n 's#.*<version>\(.*\)</version>.*#\1#p' |
        grep -Eiv '(alpha|beta|[-.]m[0-9]+|rc[0-9]*|-ea|snapshot)' | sort -V | tail -n 1)"
    [[ -n "$latest" ]] || { echo "no stable release for $path" >&2; exit 1; }
    sed -i.bak -E "s#(<$property>)[^<]*(</$property>)#\1$latest\2#" "$root/pom.xml" && rm -f "$root/pom.xml.bak"
    echo "$property = $latest"
}

set_latest netty.version io/netty/netty-buffer
set_latest junit.version org/junit/jupiter/junit-jupiter
set_latest maven.surefire.version org/apache/maven/plugins/maven-surefire-plugin
