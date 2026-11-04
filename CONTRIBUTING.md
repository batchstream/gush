# Contributing to Gush

Focused bug fixes, documentation improvements, tests and feature proposals are
welcome. Open an [issue](https://github.com/batchstream/gush/issues/new/choose)
first for a change to the public API.

## Report a problem

Use the issue forms and include the Gush version, the Temporal server version,
the expected result and a minimal reproduction. Remove credentials and private
data from logs and configuration.

For vulnerabilities, follow [SECURITY.md](SECURITY.md).

## Work locally

Install the Go version declared in [go.mod](go.mod).

```sh
git clone https://github.com/batchstream/gush.git
cd gush
make test
make lint
```

Default tests need no server. Integration tests start their own Temporal dev
server, using a `temporal` binary on `PATH` or downloading one:

```sh
make test-integration
```

## Prepare a pull request

- Keep the change focused and explain the observed problem and resulting behavior.
- Add a regression test when fixing behavior. Behavior that only a Temporal server
  can show belongs in [integration_test.go](integration_test.go).
- Keep the wire format stable: the payload encoding, the arguments passed to flows
  and steps, resume tokens and the JSON form of `FlowConfig`. Workers on different
  versions share these while a deployment rolls out.
- Update the README and [examples](examples) when the API changes.
