# connectors-sdk

[![License][License-Image]][License-Url]
[![sdk-go][sdk-go-ci-image]][sdk-go-ci-url]
[![conformance][conformance-ci-image]][conformance-ci-url]
[![spec][spec-ci-image]][spec-ci-url]

SDKs for building Synadia native connectors, and the language-neutral
contract they implement.

A connector moves data between NATS and an external system. The contract in
[`spec/`](spec/README.md) is what makes a program a connector: the `spec` and
`check` entrypoints, the config document, the status document, the admin
service, park and resume, the checkpoint store and the credential claims. An
SDK implements the mechanics once, so a connector only writes the part that
talks to its external system.

These modules are pre-1.0: the API can change in any minor version until
v1.0.0.

## Modules

| Module | Description | Docs |
|---|---|---|
| [`sdk-go`](sdk-go) | Go SDK: the connector harness and the contract packages | [![Go Reference][sdk-go-image]][sdk-go-url] |
| [`sdk-go/bloblang`](sdk-go/bloblang) | Bloblang `mapping` transform backend, in its own module so the benthos dependency graph stays out of `sdk-go` | [![Go Reference][bloblang-image]][bloblang-url] |
| [`conformance`](conformance) | Black-box contract suite that every SDK's fixture connector must pass, with importable helpers for connector suites | [![Go Reference][conformance-image]][conformance-url] |
| [`spec`](spec) | The connector contract: normative docs and JSON Schemas | [README](spec/README.md) |

## Installation

```bash
go get github.com/synadia-io/connectors-sdk/sdk-go
```

Only if the connector uses the Bloblang transform:

```bash
go get github.com/synadia-io/connectors-sdk/sdk-go/bloblang
```

## Go SDK packages

| Package | What it does |
|---|---|
| `runtime` | The harness: mode selection, config loading, NATS connection, status, admin service, park and resume, SIGTERM drain |
| `spec` | Builds the spec document, with the config JSON Schema reflected from struct tags |
| `config` | Loads the config document, applies defaults, refuses unresolved `secret://` values |
| `check` | The `check` entrypoint: one JSON result, exit code to match |
| `status` | The status document and the publisher a connector reports progress through |
| `admin` | The `connector-admin` micro service: `status`, `resume`, `skip` |
| `identity` | Runtime identity from the launch environment, and the names derived from it |
| `checkpoint` | Revision-checked checkpoint store on JetStream KV |
| `jsresource` | Creates or validates the stream a connector publishes into and the consumer it reads from |
| `subject` | Subject, token and stream/consumer name rules |
| `natsmsg` | Message size against `max_payload`, and header-name rules |
| `transform` | The per-message transform interface that backends implement |

## A minimal connector

```go
package main

import (
	"context"
	"flag"
	"os"

	"github.com/nats-io/nats.go"

	"github.com/synadia-io/connectors-sdk/sdk-go/runtime"
)

// version is set at build time (-ldflags "-X main.version=1.2.3"). Left
// empty, the harness takes it from the build info.
var version string

type Config struct {
	Subject string `json:"subject" schema:"required" display:"label=Subject"`
}

func main() {
	cfg := &Config{}
	flag.StringVar(&cfg.Subject, "subject", "", "NATS subject to subscribe on")
	flag.Parse()

	os.Exit(runtime.Main("example", version, cfg, []string{"status", "park"}, runtime.Hooks{
		Run: func(ctx context.Context, rt *runtime.Runtime) error {
			sub, err := rt.Conn.Subscribe(cfg.Subject, func(m *nats.Msg) {
				rt.Status.Add("received", 1)
			})
			if err != nil {
				return runtime.Parkf("subscribe", err)
			}
			defer sub.Unsubscribe()
			<-ctx.Done()
			return nil
		},
	}))
}
```

The harness serves `example spec` and run mode: it connects to NATS,
publishes status, serves the admin service, and drains on SIGTERM.
Without a `Probe` hook, `example check` reports a failed result and exits
1; add one, and `check` to the capabilities, to implement it. For a
connector that also implements `check`, `resume` and `skip`, see the
conformance fixture, [`conformance/testconn`](conformance/testconn/main.go).

## Layout

```
spec/              connector contract: docs and JSON Schemas (Go module: schema tests)
sdk-go/            Go SDK
sdk-go/bloblang/   Bloblang transform backend (own module)
conformance/       contract suite, its helpers, and the sdk-go fixture (testconn/)
docs/              where design decisions go (docs/README.md)
go.work            workspace over the Go modules
```

SDKs for other languages go in sibling directories (`sdk-<lang>/`).

## Versioning

Each Go module is tagged with its directory as the prefix:
`sdk-go/vX.Y.Z`, `sdk-go/bloblang/vX.Y.Z`, `conformance/vX.Y.Z`. The
contract has its own identifier, `synadia.connector/v1`
([spec/contract.md](spec/contract.md)), which does not change with SDK
releases.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0. See [LICENSE](LICENSE).

[License-Url]: https://www.apache.org/licenses/LICENSE-2.0
[License-Image]: https://img.shields.io/badge/License-Apache2-blue.svg
[sdk-go-ci-url]: https://github.com/synadia-io/connectors-sdk/actions/workflows/sdk-go.yaml
[sdk-go-ci-image]: https://github.com/synadia-io/connectors-sdk/actions/workflows/sdk-go.yaml/badge.svg?branch=main
[conformance-ci-url]: https://github.com/synadia-io/connectors-sdk/actions/workflows/conformance.yaml
[conformance-ci-image]: https://github.com/synadia-io/connectors-sdk/actions/workflows/conformance.yaml/badge.svg?branch=main
[spec-ci-url]: https://github.com/synadia-io/connectors-sdk/actions/workflows/spec.yaml
[spec-ci-image]: https://github.com/synadia-io/connectors-sdk/actions/workflows/spec.yaml/badge.svg?branch=main
[sdk-go-url]: https://pkg.go.dev/github.com/synadia-io/connectors-sdk/sdk-go
[sdk-go-image]: https://pkg.go.dev/badge/github.com/synadia-io/connectors-sdk/sdk-go.svg
[bloblang-url]: https://pkg.go.dev/github.com/synadia-io/connectors-sdk/sdk-go/bloblang
[bloblang-image]: https://pkg.go.dev/badge/github.com/synadia-io/connectors-sdk/sdk-go/bloblang.svg
[conformance-url]: https://pkg.go.dev/github.com/synadia-io/connectors-sdk/conformance
[conformance-image]: https://pkg.go.dev/badge/github.com/synadia-io/connectors-sdk/conformance.svg
