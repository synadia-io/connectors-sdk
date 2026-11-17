module github.com/synadia-io/connectors-sdk/conformance/testconn

go 1.26.4

require (
	github.com/nats-io/nats.go v1.53.1
	github.com/synadia-io/connectors-sdk/sdk-go v0.0.0-00010101000000-000000000000
)

require (
	github.com/klauspost/compress v1.20.4 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
)

replace github.com/synadia-io/connectors-sdk/sdk-go => ../../sdk-go
