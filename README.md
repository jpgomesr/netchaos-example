# netchaos-example

A small inventory HTTP API and client, used as the reference example for
[`netchaos`](https://github.com/jpgomesr/netchaos) `v0.3.0` — a Go library
that gives you a simulated, in-process `net.Conn`/`net.Listener` with
deterministic, seeded fault injection (latency, packet loss, bandwidth
throttling, packet duplication, data corruption, mid-stream reset,
partition), so retry/timeout/backoff logic can be tested without a real
network, a proxy, or a daemon.

```
go test ./...
```

CI (`.github/workflows/ci.yml`) runs on every push/PR: build + vet + gofmt
+ full test suite on Linux and Windows, a separate `-race` run, ten repeats
of the determinism test to prove the seeded-fault guarantee holds outside
a single local run, and a smoke test that boots `cmd/api` for real and
hits it with `curl`.

## Layout

```
cmd/api/                  the real service — real TCP, no netchaos involved
internal/inventory/       chi router + in-memory store; plain httptest, no chaos
internal/client/          retrying HTTP client + the netchaos test suite
internal/chaostest/       the reusable netchaos harness both client tests share
```

`internal/inventory` is deliberately boring and chaos-free: handler logic
gets plain `httptest` tests. Everything that actually crosses the wire —
the client's retry/backoff behavior — is tested through
`internal/chaostest`, against `internal/inventory`'s real handler running
behind a simulated `netchaos.Network`.

## The pattern: `internal/chaostest.Start`

```go
func Start(t *testing.T, opts ...netchaos.Option) *Harness
```

builds a `netchaos.Network`, starts the inventory server listening on it,
and returns a `Client` already wired to dial through that same network.
Every test in `internal/client/client_test.go` looks like:

```go
func TestRetries_ThroughPacketLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := chaostest.Start(t,
			netchaos.WithPacketLoss(0.3),
			netchaos.WithLatency(50*time.Millisecond, 150*time.Millisecond),
			netchaos.WithSeed(42),
		)
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

		res, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		// assert on client-level behavior: eventual success, attempts > 1 —
		// never on an error return from the drop itself (there isn't one).
	})
}
```

Beyond the retry/timeout/partition basics above, `internal/client/client_test.go`
also covers the fault kinds `v0.2.0`/`v0.3.0` added:

- **`Network.Reset`** — an abrupt `ECONNRESET` on an already-established
  connection, and the client recovering from it (`TestReset_ClientRecoversAfterAbruptFailure`).
- **`WithCorruption`** — a flipped bit in a response body the client never
  even notices at the application level, diagnosed instead via
  `Network.Trace()`'s `Size`/`CorruptedByte`/`CorruptedBit` fields
  (`TestCorruption_TraceDiagnosesTheFlippedBit`).
- **`WithDuplication`** — a duplicated request Write causing this example's
  non-idempotent reserve handler to double-decrement stock
  (`TestDuplication_ReplaysTheRequestOnTheWire`).
- **`SetPacketLoss`** — mutating fault policy on an already-established
  connection, "healthy → degraded → healthy", without rebuilding the
  `Network` (`TestSetPacketLoss_DegradesThenRecoversOnLiveConnection`).
- **`DialerFor` + `WithDialTimeout`** — bounding a wait against a
  partitioned peer from a plain `net.Dial`-shaped call site
  (`TestDialerFor_BoundedWaitAgainstPartitionedPeer`).
- **`WithBandwidth`** — the serialization-clock delay a throttled link adds
  on top of latency (`TestBandwidth_ThrottlesDelivery`).

A few things this example gets right on purpose, because they're the easy
ways to get netchaos wrong:

- **Per-attempt deadlines are mandatory, not a nicety.** A dropped write
  reports `n=len(p), nil` — it looks sent, but the peer never receives it
  and never replies. Without a bounded per-attempt context, the client
  just hangs. `internal/client.Client` always wraps each attempt in
  `context.WithTimeout`.
- **Peer naming must match exactly.** `Partition`/`Heal` only affect a
  dialer that named itself via `netchaos.WithPeerName` — a name that
  doesn't match the one used at dial time silently never binds, no error.
  `chaostest.ServerPeer` is one constant used for the `Listen` address,
  the client's base URL, and every `Partition`/`Heal` call, so there's
  only one place this could ever drift.
- **`synctest` teardown has to close the server before the test returns.**
  `http.Server.Serve` leaves an `Accept`-blocked goroutine running;
  `synctest.Test` panics if a bubble goroutine is still alive when the
  test body returns. `chaostest.Start` registers `t.Cleanup` to close the
  server and listener (so `Accept` unblocks with `net.ErrClosed`) before
  the bubble exits.
- **Keep-alives stay on.** Disabling them so every attempt gets a fresh
  dial sounds tidy, but it means the server closes the connection right
  after writing a response — and that close can race a latency-delayed
  write, tearing the connection down before the delayed bytes are
  delivered. That surfaces as a bare `EOF` that has nothing to do with any
  configured fault. Leaving keep-alives on and letting the transport evict
  broken connections on its own avoids the false failure.
- **Latency is per-`Write`, not per round trip.** A single request/response
  is at least two writes, so fixed 200ms latency yields ≳400ms elapsed —
  assert a lower bound, not a window.
- **Don't hard-code attempt counts for a seed.** What seed 42 at 30% loss
  actually produces isn't predictable from outside the library. Assert
  relations (`attempts > 1`, two runs of the same seed produce the same
  tally) instead of literals, then record the observed count in a comment
  once you've seen it.

## Reproducing a failure

Every test seeds its `Network` explicitly (`netchaos.WithSeed(n)`). If a
chaos test fails, the seed and call order in the test source are all you
need to reproduce it — re-run the same test, or drop the seed into a
scratch test, and you'll get the identical fault sequence.

## Running the real service

```
go run ./cmd/api            # listens on :8080
ADDR=:18080 go run ./cmd/api
```

```
curl localhost:8080/health
curl -X POST localhost:8080/items -d '{"id":"sku-2","name":"Gadget","stock":5}'
curl localhost:8080/items/sku-2
curl -X POST localhost:8080/items/sku-2/reserve -d '{"qty":2}'
```

This runs `internal/inventory.NewServer` over a real TCP listener — the
exact same handler the netchaos tests exercise over a simulated one.
