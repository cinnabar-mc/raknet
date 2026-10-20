# go-raknet parity helper

Go side of the parity harness, pinned to go-raknet `lunar` at `6124f52` through
the `replace` in `go.mod`.

- `cmd/peer`: one go-raknet listener or dialer driven over stdin/stdout by
  `raknet-tokio/tests/go_interop.rs`. Those tests use `RAKNET_GO_PEER`, or build
  the peer when `go` is on PATH.
- `cmd/inpkg`: copies the pinned module into a scratch directory, adds
  `inpkg/*_test.go` to its package and runs `go test` there, so tests reach
  go-raknet's unexported codec and connection internals.
  - `go run ./cmd/inpkg -run TestParityGolden -update` rewrites
    `testdata/golden` and `testdata/bench`; CI runs it without `-update`.
  - `go run ./cmd/inpkg -run '^$' -bench . -benchmem` runs the codec, split
    and scheduler benchmarks.
- `go test -bench . .` runs the loopback benchmarks.

Benchmark names match the criterion ids in `raknet/benches` and
`raknet-tokio/benches`; `go tool bench-compare` joins both into one table.
