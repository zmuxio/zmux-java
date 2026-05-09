# Releasing

This document is for maintainers. User installation instructions belong in
`README.md`.

Published coordinates:

- `io.github.zmuxio:zmux-parent`
- `io.github.zmuxio:zmux`
- `io.github.zmuxio:zmux-netty-quic`

## Model

Releases are triggered by Git tags. Local work ends when `vX.Y.Z` is pushed to
GitHub; the release workflow validates the tag, stages Maven artifacts, signs
them with JReleaser, and publishes to Maven Central.

The tag must match the Maven reactor version exactly:

- Maven version: `X.Y.Z`
- Git tag: `vX.Y.Z`

## Steps

Start from a clean, up-to-date `main`:

```bash
git status --short --branch
git fetch origin
git rev-list --left-right --count main...origin/main
```

Check external dependencies before setting the release version:

```bash
mvn -B versions:display-property-updates
mvn -B versions:display-dependency-updates
mvn -B versions:display-plugin-updates
```

Review updates deliberately. Keep Java compatibility unchanged unless that is
an intentional release decision.

Set the release version:

```bash
mvn -B versions:set -DnewVersion=X.Y.Z -DgenerateBackupPoms=false
```

Verify the candidate:

```bash
mvn -B test
mvn -B -Prelease -DskipTests deploy
```

Commit and push the release version plus any intentional dependency or document
updates:

```bash
git status --short --branch
git add pom.xml zmux/pom.xml zmux-netty-quic/pom.xml RELEASING.md README.md
git commit -m "release: prepare vX.Y.Z"
git push origin main
```

Create and push the tag:

```bash
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

After the tag is pushed to GitHub, the maintainer release process is complete.
If automation fails later, fix the cause and release a new tag or version as
appropriate.
