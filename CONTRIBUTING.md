# Contributing

Before you start on a change, open an issue to discuss it, so nobody does
the same work twice or work that won't be merged. Say on the issue when you
start, and open the pull request early.

## Development

Each Go module (`sdk-go`, `sdk-go/bloblang`, `conformance`,
`conformance/testconn`, `spec`) is tested from its own directory:

```bash
cd sdk-go && go test ./...
```

`go.work` ties the modules together, so a change in `sdk-go` is picked up by
`sdk-go/bloblang` and the conformance fixture without a release. The SDK's
tests read `spec/*.schema.json` through relative paths (`../../spec/`), so
keep the layout.

CI runs gofmt, `go vet` and the tests for each module. The `sdk-go`,
`sdk-go/bloblang` and `conformance` tests also run under `-race`.

A change to a contract surface changes `spec/` in the same pull request, and
the conformance suite when the change is something every SDK must do.

Every source file starts with the Apache 2.0 license header, followed by a
blank line. Copy it from any existing file, with the year the file is
created.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/):
`type(scope): subject`.

- The scope is one of `sdk-go`, `spec`, `conformance`, `ci` or `repo`. A new
  SDK directory adds its scope to `.commitlintrc.mjs` and to the scope
  list in `.githooks/commit-msg`.
- The subject is lowercase.
- Every line is at most 100 characters.
- Keep each commit to one change, and split unrelated changes into their own
  pull request.

commitlint checks every pull request. To check locally before you push:
`git config core.hooksPath .githooks`.

## Releasing

Each Go module is versioned on its own, with a tag prefixed by its directory:
`sdk-go/vX.Y.Z`, `sdk-go/bloblang/vX.Y.Z`, `conformance/vX.Y.Z`.

`sdk-go/bloblang` requires `sdk-go` by version. Its `replace` directive only
applies inside this repository; anyone who imports `sdk-go/bloblang` gets at
least the `sdk-go` version its `go.mod` requires. Inside the repository the
`replace` and `go.work` hide that version, so a build here can pass while the
release breaks for its users. When a bloblang release needs a newer `sdk-go`:

1. Tag `sdk-go` first and push the tag.
2. Raise the `sdk-go` requirement in `sdk-go/bloblang/go.mod` to that tag.
3. Check that `sdk-go/bloblang` builds against the released `sdk-go`:

   ```bash
   cd sdk-go/bloblang
   go mod edit -dropreplace=github.com/synadia-io/connectors-sdk/sdk-go
   GOWORK=off GOFLAGS=-mod=mod go test ./...
   git checkout go.mod go.sum
   ```

4. Tag `sdk-go/bloblang`.

When both are released from one commit that already requires the new
version, as v0.1.0 is, both tags go on that commit.

Never move or delete a tag once it has been pushed: the Go module proxy and
checksum database keep the first content they see for a version. A bad
release is fixed with a newer version, plus a `retract` directive in the
module's `go.mod` for the bad one. Publish a GitHub release for each tag,
with its notes.
