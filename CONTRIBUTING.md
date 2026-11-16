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

Each SDK is versioned on its own, with tags prefixed by its directory.
`conformance` isn't released yet; when it is, its tags are
`conformance/vX.Y.Z`.

### The Go SDK

The Go SDK is two modules, `sdk-go` and `sdk-go/bloblang`. They are released
together, under one version, from one commit: `sdk-go/vX.Y.Z` and
`sdk-go/bloblang/vX.Y.Z`.

`sdk-go/bloblang` requires `sdk-go` by version. Its `replace` directive only
applies inside this repository: anyone who imports `sdk-go/bloblang` gets at
least the `sdk-go` version its `go.mod` requires. Releasing both from one
commit, with that requirement set to the version being released, keeps the
two in step.

1. Open a release pull request. It sets the `sdk-go` requirement in
   `sdk-go/bloblang/go.mod` to `vX.Y.Z`, the version being released, and its
   description carries the release notes for both modules. Merge it.
2. Tag the merge commit with both tags, signed, and push them in one push:

   ```bash
   git tag -s sdk-go/vX.Y.Z -m "sdk-go vX.Y.Z"
   git tag -s sdk-go/bloblang/vX.Y.Z -m "sdk-go/bloblang vX.Y.Z"
   git push origin sdk-go/vX.Y.Z sdk-go/bloblang/vX.Y.Z
   ```

3. Check the release the way a user gets it, outside this repository:

   ```bash
   cd "$(mktemp -d)" && go mod init example.com/check
   go get github.com/synadia-io/connectors-sdk/sdk-go/bloblang@vX.Y.Z
   go build github.com/synadia-io/connectors-sdk/sdk-go/...
   go build github.com/synadia-io/connectors-sdk/sdk-go/bloblang
   ```

4. Ask the Go module proxy for both versions, so pkg.go.dev lists them:

   ```bash
   GOPROXY=https://proxy.golang.org go list -m github.com/synadia-io/connectors-sdk/sdk-go@vX.Y.Z
   GOPROXY=https://proxy.golang.org go list -m github.com/synadia-io/connectors-sdk/sdk-go/bloblang@vX.Y.Z
   ```

5. Publish a GitHub release for each tag, with its notes.

Never move or delete a tag once it has been pushed: the Go module proxy and
checksum database keep the first content they see for a version. A bad
release is fixed with a newer version, plus a `retract` directive in the
module's `go.mod` for the bad one.
