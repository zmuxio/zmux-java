# Vendored zmux-spec fixture bundle

This directory is a byte-for-byte copy of `zmux-spec/fixtures/*`. It is test input, not a second source of protocol
truth: `zmux-spec` stays authoritative for the fixture format and the expected behavior.

## Files and the tests that run them

- `wire_valid.ndjson`: `io.zmux.protocol.SpecWireFixtureTest#wireValidFixturesDecodeAndRoundTrip`
  - decodes every preface and frame with each Java reader and checks the `expect` fields and the decoded payloads;
  - re-encodes it and compares the bytes, except a padded preface, which re-encodes without its padding TLV.
- `wire_invalid.ndjson`:
  - `SpecWireFixtureTest#wireInvalidFixturesFailWithTheFixtureCode` checks the codec error code from every frame reader
    (or the varint decoders for `bytes_invalid`).
  - `io.zmux.SpecInvalidCaseFixtureTest#wireInvalidFramesCloseTheSessionWithTheFixtureCode` checks that a live session
    sends `CLOSE` with the same code.
- `invalid_cases.ndjson`: `io.zmux.SpecInvalidCaseFixtureTest#invalidCaseFixturesBehaveAsSpecified`
  - every id needs an explicit runner, so an unmapped id fails;
  - the expected code or action always comes from the fixture, with no local overrides;
  - session-scope cases are checked on the wire, through a raw peer (`io.zmux.SpecFixturePeer`).
- `state_cases.ndjson`: `io.zmux.SpecStateCaseFixtureTest` runs the `portable_state` case set
  (`examples/fixture_mapping.md` section 2.1). The other state cases are reference-harness labels and are only
  inventoried.
- `case_sets.json` and `index.json`: `SpecWireFixtureTest` checks the following.
  - The record counts match `index.json`.
  - Fixture ids are globally unique.
  - Every case-set id resolves to a fixture.
  - `codec_valid` and `codec_invalid` equal the wire bundles.

## Refreshing

Copy `zmux-spec/fixtures/*` over the bundle files here, then run `mvn -B -q test`. Run it with
`ZMUX_SPEC_ROOT=/path/to/zmux-spec` as well; `SpecWireFixtureTest#vendoredBundleMatchesSpecCheckoutWhenConfigured`
then fails if any vendored file differs from that checkout.

A new `invalid_cases` id needs a runner in `SpecInvalidCaseFixtureTest`. A new portable event or result needs
handling in `SpecStateCaseFixtureTest`.
