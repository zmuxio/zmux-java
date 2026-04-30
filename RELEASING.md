# Releasing

This document is for maintainers publishing the Java artifacts. User-facing
installation instructions belong in `README.md`.

The Maven reactor publishes these coordinates:

- parent POM: `io.github.zmuxio:zmux-parent`
- core library: `io.github.zmuxio:zmux`
- optional Netty QUIC adapter: `io.github.zmuxio:zmux-netty-quic`

The parent POM is publication metadata. The user-facing artifacts to verify
after publication are `zmux` and `zmux-netty-quic`.

## Version Tags

Use one reactor version and one Git tag for all Java artifacts:

- Maven version: `X.Y.Z`
- Git tag: `vX.Y.Z`

Do not publish `zmux` and `zmux-netty-quic` with different versions. The adapter
depends on the core module at `${project.version}`.

## Prerequisites

Start from `main` with a clean local view of the remote:

```bash
git status --short --branch
git fetch origin
git rev-list --left-right --count main...origin/main
```

The ahead/behind count should be `0 0` before release work starts.

Maven Central publishing also requires:

- Maven Central credentials in Maven `settings.xml` under server id `central`.
- A GPG key available to `maven-gpg-plugin`.
- No `SNAPSHOT` dependencies or plugins in the release build.

The repository `release` profile attaches source jars, javadoc jars, GPG
signatures, and configures `central-publishing-maven-plugin` with
`autoPublish=false`. A deploy creates a Central Portal deployment for manual
review; it does not publish artifacts automatically.

## Dependency Update Pass

Before changing the release version, check whether dependency and plugin
properties should be updated:

```bash
mvn -B versions:display-property-updates
mvn -B versions:display-dependency-updates
mvn -B versions:display-plugin-updates
```

Apply updates deliberately:

- Root `pom.xml` owns common plugin, JUnit, GPG, Central publishing, and
  compatibility properties.
- `zmux-netty-quic/pom.xml` owns the Netty QUIC version used by the adapter.
- If Netty QUIC is updated, keep the compile-time QUIC classes dependency and
  test native QUIC dependency aligned through the same property.
- Keep Central publishing and signing plugin updates separate from behavior
  changes when possible.
- Do not change the Java compatibility target unless the public compatibility
  policy is intentionally changing.
- If dependency updates affect user installation guidance, update `README.md`
  using placeholders such as `VERSION` and `NETTY_VERSION`, not concrete
  release numbers.

Run the verification matrix after dependency edits, before preparing the release
version.

## Version Update

Update the root version and child parent versions to `X.Y.Z`:

```bash
mvn -B versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false
```

Confirm the reactor agrees on one version:

```bash
mvn -B help:evaluate -Dexpression=project.version -q -DforceStdout
mvn -B -pl zmux help:evaluate -Dexpression=project.version -q -DforceStdout
mvn -B -pl zmux-netty-quic help:evaluate -Dexpression=project.version -q -DforceStdout
```

## Verification Matrix

Use `java8-compat` for normal repository checks because published artifacts are
Java 8-compatible. Use `release` only for release packaging checks because it
also attaches source jars, javadoc jars, signatures, and Central publishing
configuration.

For a small core-only edit during iteration:

```bash
mvn -B -Pjava8-compat -pl zmux -DskipTests compile
```

For any core protocol, runtime, lifecycle, flow-control, queueing, memory, API,
or conformance change:

```bash
mvn -B -Pjava8-compat -pl zmux test
```

For any QUIC adapter change, public connection-interface change, dependency
change, or release candidate:

```bash
mvn -B -Pjava8-compat -pl zmux-netty-quic -am test
```

Before publishing every release candidate, run the full reactor and the release
packaging dry run:

```bash
mvn -B -Pjava8-compat test
mvn -B -Prelease -Dgpg.skip=true -DskipTests verify
```

Focused test groups are useful while iterating, but they do not replace the
release-candidate checks above:

```bash
mvn -B -Pjava8-compat -pl zmux -Dtest=SessionWriterTransportTest,WriterQueuePolicyTest,WriteBufferOwnershipTest test
mvn -B -Pjava8-compat -pl zmux -Dtest=SessionTelemetryStateTest,PingPongReaderRuntimeTest,KeepaliveTest,PingSemanticsTest test
mvn -B -Pjava8-compat -pl zmux -Dtest=SessionCloseCleanupRuntimeTest,SessionTerminationTest,GracefulCloseTest test
mvn -B -Pjava8-compat -pl zmux-netty-quic -am -Dtest=NettyQuicSessionContractTest,NettyQuicSupportTest,NettyQuicConformanceTest test
```

Use the repository helper when validating a release candidate or when Go/spec
repositories are available locally.

PowerShell:

```powershell
.\tools\verify-release.ps1 -GoRoot PATH_TO_ZMUX_GO -SpecRoot PATH_TO_ZMUX_SPEC -RunInterop
```

POSIX shell:

```bash
ZMUX_GO_ROOT=/path/to/zmux-go ZMUX_SPEC_ROOT=/path/to/zmux-spec tools/verify-release.sh --run-interop
```

The helper runs Java compile/tests, optional Go tests, optional spec asset
validation, optional interop smoke, and release-profile verification. Use
`-RunBenchmarks` / `--run-benchmarks` only when performance-sensitive changes
need a quick JMH pass.

Standalone quick benchmarks:

```bash
tools/run-benchmarks.sh --quick
```

PowerShell:

```powershell
.\tools\run-benchmarks.ps1 -Quick
```

Finish every release commit with:

```bash
git status --short --branch
git diff --check
mvn -B -Pjava8-compat test
mvn -B -Prelease -Dgpg.skip=true -DskipTests verify
```

## Local Consumption Test

For a no-network smoke test of user consumption, install release-compatible
artifacts locally and try a clean sample project using the README coordinates:

```bash
mvn -B -Pjava8-compat -DskipTests install
```

This verifies local Maven metadata and dependency resolution before any Central
Portal upload.

## Central Portal Test Upload

Deploy with signing enabled:

```bash
mvn -B -Prelease clean deploy
```

Do not pass `-Dgpg.skip=true` for this deploy. Because `autoPublish=false`, the
upload should appear in the Central Portal as an unpublished deployment. Inspect
the deployment, generated POMs, source jars, javadoc jars, signatures, and
artifact list before publishing.

If anything is wrong, drop the Central Portal deployment and fix the release
commit before retrying. Do not publish a deployment that has not been reviewed.

## Publish And Verify

After publishing the reviewed Central Portal deployment, verify both
user-facing artifacts from a clean Maven cache or clean sample project:

```bash
mvn -U dependency:get -Dartifact=io.github.zmuxio:zmux:X.Y.Z
mvn -U dependency:get -Dartifact=io.github.zmuxio:zmux-netty-quic:X.Y.Z
```

Then smoke-test a minimal Gradle or Maven application using the installation
snippets in `README.md`.

## Commit, Tag, Push

Commit the release version and any dependency/doc changes before deploying:

```bash
git add pom.xml zmux/pom.xml zmux-netty-quic/pom.xml README.md RELEASING.md
git commit -m "release: prepare vX.Y.Z"
git tag -a vX.Y.Z -m "vX.Y.Z"
```

Push only after the Central deployment has been accepted and verified:

```bash
git push origin main
git push origin vX.Y.Z
```

## Template

Release `vX.Y.Z`:

```bash
git status --short --branch
git fetch origin
git rev-list --left-right --count main...origin/main

mvn -B versions:display-property-updates
mvn -B versions:display-dependency-updates
mvn -B versions:display-plugin-updates

# Apply intentional dependency/plugin/doc updates, then verify.
mvn -B -Pjava8-compat test
mvn -B -Prelease -Dgpg.skip=true -DskipTests verify

mvn -B versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false
mvn -B help:evaluate -Dexpression=project.version -q -DforceStdout
mvn -B -pl zmux help:evaluate -Dexpression=project.version -q -DforceStdout
mvn -B -pl zmux-netty-quic help:evaluate -Dexpression=project.version -q -DforceStdout

mvn -B -Pjava8-compat test
mvn -B -Prelease -Dgpg.skip=true -DskipTests verify
mvn -B -Pjava8-compat -DskipTests install
git diff --check

git add pom.xml zmux/pom.xml zmux-netty-quic/pom.xml README.md RELEASING.md
git commit -m "release: prepare vX.Y.Z"
git tag -a vX.Y.Z -m "vX.Y.Z"

mvn -B -Prelease clean deploy

# Review and publish the Central Portal deployment, then verify:
mvn -U dependency:get -Dartifact=io.github.zmuxio:zmux:X.Y.Z
mvn -U dependency:get -Dartifact=io.github.zmuxio:zmux-netty-quic:X.Y.Z

git push origin main
git push origin vX.Y.Z
```
