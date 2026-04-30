# Releasing

This document is for maintainers. User installation instructions belong in
`README.md`.

Published coordinates:

- `io.github.zmuxio:zmux-parent`
- `io.github.zmuxio:zmux`
- `io.github.zmuxio:zmux-netty-quic`

The parent POM is publication metadata. Users should depend on `zmux` and,
when needed, `zmux-netty-quic`.

## Before Release

Start from an up-to-date `main`:

```bash
git status --short --branch
git fetch origin
git rev-list --left-right --count main...origin/main
```

The ahead/behind count should be `0 0`.

Central publishing requires:

- Central Portal namespace access for `io.github.zmuxio`.
- Central Portal token in Maven `settings.xml` under server id `central`.
- A GPG key available to `maven-gpg-plugin`.
- No `SNAPSHOT` dependencies or plugins.

## Update External Dependencies

Before setting the release version, check external dependencies and plugins:

```bash
mvn -B versions:display-property-updates
mvn -B versions:display-dependency-updates
mvn -B versions:display-plugin-updates
```

Review updates deliberately:

- Root `pom.xml` owns shared Maven plugins, JUnit, GPG, Central publishing,
  benchmark, compatibility, and test dependency properties.
- `zmux-netty-quic/pom.xml` owns the Netty QUIC version.
- Keep Netty QUIC compile and native test dependencies aligned through the same
  property.
- Do not take pre-release dependency versions unless the release intentionally
  depends on them.
- Do not change Java compatibility unless the public compatibility policy is
  changing.
- If dependency changes affect user installation, update `README.md` with
  placeholders such as `VERSION`, `NETTY_VERSION`, and `OS_CLASSIFIER`.

After dependency edits, run the release checks before changing the project
version.

## Verify Candidate

For every release candidate:

```bash
mvn -B -Pjava8-compat test
mvn -B -Prelease -Dgpg.skip=true -DskipTests verify
```

PowerShell needs the dotted property quoted:

```powershell
mvn -B -Prelease '-Dgpg.skip=true' -DskipTests verify
```

When Go/spec repositories are available, prefer the helper:

```powershell
.\tools\verify-release.ps1 -GoRoot PATH_TO_ZMUX_GO -SpecRoot PATH_TO_ZMUX_SPEC -RunInterop
```

POSIX:

```bash
ZMUX_GO_ROOT=/path/to/zmux-go ZMUX_SPEC_ROOT=/path/to/zmux-spec tools/verify-release.sh --run-interop
```

Run dependency updates and these checks again if any release fix changes code,
public API, dependencies, or publishing metadata.

## Set Version

Use one Maven version and one Git tag for all Java artifacts:

- Maven version: `X.Y.Z`
- Git tag: `vX.Y.Z`

Update the reactor version:

```bash
mvn -B versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false
```

Confirm all modules agree:

```bash
mvn -B help:evaluate -Dexpression=project.version -q -DforceStdout
mvn -B -pl zmux help:evaluate -Dexpression=project.version -q -DforceStdout
mvn -B -pl zmux-netty-quic help:evaluate -Dexpression=project.version -q -DforceStdout
```

Run the release checks again after the version change:

```bash
git diff --check
mvn -B -Pjava8-compat test
mvn -B -Prelease -Dgpg.skip=true -DskipTests verify
```

PowerShell:

```powershell
mvn -B -Prelease '-Dgpg.skip=true' -DskipTests verify
```

Optionally install locally and try a clean sample project using the README
coordinates:

```bash
mvn -B -Pjava8-compat -DskipTests install
```

## Commit And Tag

Commit the release version and intentional dependency/doc updates:

```bash
git status --short --branch
git add pom.xml zmux/pom.xml zmux-netty-quic/pom.xml README.md RELEASING.md
git commit -m "release: prepare vX.Y.Z"
git tag -a vX.Y.Z -m "vX.Y.Z"
```

Do not publish a dirty worktree.

## Upload To Central Portal

Deploy with signing enabled:

```bash
mvn -B -Prelease clean deploy
```

Do not pass `-Dgpg.skip=true`. The release profile attaches source jars,
javadoc jars, GPG signatures, and uploads with `autoPublish=false`, so the
Central Portal deployment must still be reviewed manually.

In Central Portal, inspect:

- `zmux-parent`, `zmux`, and `zmux-netty-quic` artifacts.
- Generated POM metadata.
- Main jars, source jars, javadoc jars, and signatures.

Drop the deployment if anything is wrong. Fix, commit, retag if needed, and
upload again. Do not publish an unreviewed deployment.

## Publish And Verify

After publishing in Central Portal, verify both user-facing artifacts from a
clean Maven cache or clean sample project:

```bash
mvn -U dependency:get -Dartifact=io.github.zmuxio:zmux:X.Y.Z
mvn -U dependency:get -Dartifact=io.github.zmuxio:zmux-netty-quic:X.Y.Z
```

Smoke-test a minimal Maven or Gradle app using the README snippets.

Push only after Central publication is accepted and verified:

```bash
git push origin main
git push origin vX.Y.Z
```

Maven Central versions are immutable. If a published version is wrong, release a
new version.
