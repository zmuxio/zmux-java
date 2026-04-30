# Releasing

This document is for maintainers. User installation instructions belong in
`README.md`.

Published coordinates:

- `io.github.zmuxio:zmux-parent`
- `io.github.zmuxio:zmux`
- `io.github.zmuxio:zmux-netty-quic`

## Release Model

Releases are automated from Git tags. After a release commit reaches `main`,
push tag `vX.Y.Z`; `.github/workflows/release.yml` validates the tag, stages
Maven artifacts, signs them with JReleaser, and publishes to Maven Central.
Dependency updates, code changes, and tests are done locally before the tag is
pushed.

The tag must match the Maven reactor version exactly:

- Maven version: `X.Y.Z`
- Git tag: `vX.Y.Z`

## Required GitHub Secrets

- `JRELEASER_MAVENCENTRAL_USERNAME`
- `JRELEASER_MAVENCENTRAL_TOKEN`
- `JRELEASER_GPG_PUBLIC_KEY`
- `JRELEASER_GPG_SECRET_KEY`
- `JRELEASER_GPG_PASSPHRASE`

The Central Portal namespace must allow publishing under `io.github.zmuxio`.
`JRELEASER_GITHUB_TOKEN` is provided by GitHub Actions through
`${{ secrets.GITHUB_TOKEN }}` and does not need to be created manually.

Central Portal token values map to the old Maven `settings.xml` shape like
this:

```xml
<server>
  <id>central</id>
  <username>...</username>
  <password>...</password>
</server>
```

Use the `<username>` value as `JRELEASER_MAVENCENTRAL_USERNAME` and the
`<password>` value as `JRELEASER_MAVENCENTRAL_TOKEN`. The server id is not used
by the workflow because Maven only stages artifacts locally; JReleaser uploads
to Maven Central.

Export the signing key material as armored text and store it as GitHub Secrets:

```bash
gpg --armor --export KEY_ID
gpg --armor --export-secret-key KEY_ID
```

Use the public key output for `JRELEASER_GPG_PUBLIC_KEY`, the secret key output
for `JRELEASER_GPG_SECRET_KEY`, and the key passphrase for
`JRELEASER_GPG_PASSPHRASE`.

## Before Release

Start from an up-to-date clean `main`:

```bash
git status --short --branch
git fetch origin
git rev-list --left-right --count main...origin/main
```

The ahead/behind count should be `0 0`.

Check external dependencies and plugins before setting the release version:

```bash
mvn -B versions:display-property-updates
mvn -B versions:display-dependency-updates
mvn -B versions:display-plugin-updates
```

Review updates deliberately. Keep Netty QUIC compile and native test
dependencies aligned through the same property, avoid pre-release versions
unless intentional, and do not change Java compatibility unless the public
policy changes.

## Verify Candidate

```bash
mvn -B -Pjava8-compat test
mvn -B -Prelease -DskipTests deploy
```

The release profile stages artifacts under `target/staging-deploy`; it does
not upload to Central. JReleaser handles signing and publication in GitHub
Actions.

When Go/spec repositories are available, prefer the helper:

```powershell
.\tools\verify-release.ps1 -GoRoot PATH_TO_ZMUX_GO -SpecRoot PATH_TO_ZMUX_SPEC -RunInterop
```

POSIX:

```bash
ZMUX_GO_ROOT=/path/to/zmux-go ZMUX_SPEC_ROOT=/path/to/zmux-spec tools/verify-release.sh --run-interop
```

## Set Version

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

Run verification again after version or dependency changes.

## Commit And Publish

Commit the release version and intentional dependency/doc updates:

```bash
git status --short --branch
git add pom.xml zmux/pom.xml zmux-netty-quic/pom.xml README.md RELEASING.md jreleaser.yml .github/workflows/release.yml
git commit -m "release: prepare vX.Y.Z"
git push origin main
```

Create and push the tag:

```bash
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

That tag push starts the release workflow. If the workflow fails, fix the
problem, delete the failed remote tag if nothing was published, retag the fixed
commit, and push again. Maven Central versions are immutable; if a version was
published incorrectly, release a new version.

## Verify Published Artifacts

After the workflow succeeds, verify both user-facing artifacts from a clean
Maven cache or sample project:

```bash
mvn -U dependency:get -Dartifact=io.github.zmuxio:zmux:X.Y.Z
mvn -U dependency:get -Dartifact=io.github.zmuxio:zmux-netty-quic:X.Y.Z
```
