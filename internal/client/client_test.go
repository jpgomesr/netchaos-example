package client_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jpgomesr/netchaos"

	"github.com/jpgomesr/netchaos-example/internal/chaostest"
	"github.com/jpgomesr/netchaos-example/internal/inventory"
)

func TestBaseline_CleanNetwork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := chaostest.Start(t)
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

		res, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("got status %d, want %d", res.StatusCode, http.StatusOK)
		}
		if res.Attempts != 1 {
			t.Fatalf("got %d attempts on a clean network, want 1", res.Attempts)
		}
	})
}

func TestRetries_ThroughPacketLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := chaostest.Start(t,
			netchaos.WithPacketLoss(0.3),
			netchaos.WithLatency(50*time.Millisecond, 150*time.Millisecond),
			netchaos.WithSeed(42),
		)
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

		res, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)

		// A dropped write reports n=len(p), nil to the writer -- it never
		// surfaces as an error. The observable symptom is that the
		// request/response round trip silently doesn't complete on that
		// attempt, so the client retries until one gets through. We assert
		// the client-level outcome (eventual success, more than one
		// attempt), never an error return caused by the drop itself.
		//
		// Observed for this exact seed/config/call-order: 2 attempts. That
		// number can change if this test (or code before it in the same
		// scenario) changes call order -- re-run and update the comment,
		// don't hand-tune the seed to hit a specific count.
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("got status %d, want %d", res.StatusCode, http.StatusOK)
		}
		if res.Attempts <= 1 {
			t.Fatalf("got %d attempt(s) under 30%% packet loss, want retries (>1)", res.Attempts)
		}
	})
}

func TestFails_WhenPartitioned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := chaostest.Start(t, netchaos.WithSeed(7))
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

		// Establish the connection while healthy so the partition below
		// hits an already-connected client (dialing straight into a
		// partition would block instead of failing per-request).
		if _, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil); err != nil {
			t.Fatalf("warm-up request failed: %v", err)
		}

		h.PartitionClient()

		_, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		if err == nil {
			t.Fatal("expected an error while partitioned, got nil")
		}
		// The retried call's first attempt reuses the connection warmed up
		// above (keep-alives are on), hitting the already-established-
		// connection path: writes into a partitioned pair are silently
		// discarded and reads block until their deadline. If that attempt
		// fails and a later attempt has to redial, it hits the other
		// documented path instead -- Dial/DialContext blocking against a
		// partitioned peer. Both surface identically here (the per-attempt
		// context timing out), so this assertion can't and doesn't try to
		// tell which path fired; it just confirms every attempt times out
		// rather than one succeeding through a partition.
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("got error %v, want a deadline-exceeded error", err)
		}
	})
}

func TestRecovers_AfterHeal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := chaostest.Start(t, netchaos.WithSeed(7))
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})
		defer h.HealClient() // idempotent even if already healed

		if _, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil); err != nil {
			t.Fatalf("warm-up request failed: %v", err)
		}

		h.PartitionClient()
		if _, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil); err == nil {
			t.Fatal("expected a failure while partitioned")
		}

		h.HealClient()

		res, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		if err != nil {
			t.Fatalf("expected recovery after Heal, got error: %v", err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("got status %d, want %d", res.StatusCode, http.StatusOK)
		}
	})
}

func TestDial_ConnectionRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network := netchaos.NewNetwork(netchaos.WithSeed(3))

		// Nothing has Listen'd on this address.
		_, err := network.DialContext(context.Background(), "tcp", "nobody-here:1")
		if !errors.Is(err, netchaos.ErrConnectionRefused) {
			t.Fatalf("got error %v, want ErrConnectionRefused", err)
		}
	})
}

func TestUnsupportedNetwork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network := netchaos.NewNetwork(netchaos.WithSeed(3))

		_, err := network.Listen("udp", "inventory-api:8080")
		if !errors.Is(err, netchaos.ErrUnsupportedNetwork) {
			t.Fatalf("got error %v, want ErrUnsupportedNetwork", err)
		}
	})
}

func TestDeterminism_SameSeed(t *testing.T) {
	runOnce := func(t *testing.T) int {
		var attempts int
		synctest.Test(t, func(t *testing.T) {
			h := chaostest.Start(t,
				netchaos.WithPacketLoss(0.3),
				netchaos.WithLatency(50*time.Millisecond, 150*time.Millisecond),
				netchaos.WithSeed(42),
			)
			h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

			res, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			attempts = res.Attempts
		})
		return attempts
	}

	// Same seed, same call order -> identical fault sequence -> identical
	// attempt count. Compared against each other, not a hard-coded
	// literal: what seed 42 actually produces isn't something to guess at
	// from outside the library.
	runA := runOnce(t)
	runB := runOnce(t)
	if runA <= 1 {
		// If this scenario ever stops triggering a retry (e.g. the loss
		// rate above gets tuned down), runA == runB == 1 trivially and
		// this test would pass without checking anything -- the fault
		// config has to actually bite for the comparison below to mean
		// something.
		t.Fatalf("scenario produced only %d attempt; determinism comparison is vacuous without a retry", runA)
	}
	if runA != runB {
		t.Fatalf("same seed produced different attempt counts: %d vs %d", runA, runB)
	}
}

func TestReset_ClientRecoversAfterAbruptFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := chaostest.Start(t, netchaos.WithSeed(13))
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

		// Warm up so Reset below has an already-established connection to
		// act on -- Reset only touches connections that already exist, it
		// has no effect on Dial (unlike Partition).
		if _, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil); err != nil {
			t.Fatalf("warm-up request failed: %v", err)
		}

		h.Network.Reset("client", chaostest.ServerPeer)

		// The reused connection now fails with ECONNRESET on its next use.
		// Whether that surfaces as this client's own retry (Attempts > 1)
		// or is absorbed by net/http's own transparent one-shot retry on a
		// broken reused connection isn't something to assert on here --
		// either way, Reset doesn't block a fresh Dial (unlike Partition),
		// so the call still succeeds overall.
		res, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		if err != nil {
			t.Fatalf("expected the client to recover after Reset, got error: %v", err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("got status %d, want %d", res.StatusCode, http.StatusOK)
		}
	})
}

func TestCorruption_TraceDiagnosesTheFlippedBit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := chaostest.Start(t, netchaos.WithCorruption(1.0), netchaos.WithSeed(17))
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

		// A single bit flipped in a JSON response body is invisible to
		// this client -- it never unmarshals the body, so it can't detect
		// corruption at the application level. That's exactly the gap
		// FaultEvent's Size/CorruptedByte/CorruptedBit fields (v0.3.0,
		// issue #78) close: a test can pinpoint what was corrupted from
		// Network.Trace() even when nothing observable happened above it.
		if _, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var found bool
		for _, ev := range h.Network.Trace() {
			if !ev.Corrupted || ev.Size == 0 {
				continue
			}
			found = true
			if ev.CorruptedByte < 0 || ev.CorruptedByte >= ev.Size {
				t.Fatalf("corrupted byte index %d out of range for a %d-byte unit", ev.CorruptedByte, ev.Size)
			}
			if ev.CorruptedBit > 7 {
				t.Fatalf("corrupted bit index %d out of range (want 0-7)", ev.CorruptedBit)
			}
		}
		if !found {
			t.Fatal("WithCorruption(1.0) recorded no corrupted, non-empty unit in the trace")
		}
	})
}

func TestDuplication_ReplaysTheRequestOnTheWire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := chaostest.Start(t, netchaos.WithDuplication(1.0), netchaos.WithSeed(19))
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

		// WithDuplication(1.0) always admits a delivered Write a second
		// time, with the exact same bytes, right behind the first (same
		// release timing -- see duplicate.go). The client's reserve
		// request is one Write call, so the server's connection reads the
		// duplicate as a second, identical, pipelined request and
		// processes it again -- exactly the "does your code handle a
		// message arriving twice" gap docs/05 describes. This reserve
		// handler isn't idempotent, so stock is decremented twice.
		res, err := h.Client.Do(context.Background(), http.MethodPost, "/items/sku-1/reserve", []byte(`{"qty":3}`))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("got status %d, want %d", res.StatusCode, http.StatusOK)
		}

		// The client only waits for the first response; the server's
		// duplicate-processed second request runs on its own goroutine and
		// isn't otherwise synchronized with the assertion below. Wait for
		// every goroutine in the bubble to go durably idle so that second
		// reserve has actually completed before checking the store.
		synctest.Wait()

		got, err := h.Store.Get("sku-1")
		if err != nil {
			t.Fatalf("unexpected error reading back the item: %v", err)
		}
		if want := 10 - 2*3; got.Stock != want {
			t.Fatalf("got stock %d, want %d (reserve applied twice via the duplicated write)", got.Stock, want)
		}
	})
}

func TestSetPacketLoss_DegradesThenRecoversOnLiveConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := chaostest.Start(t, netchaos.WithSeed(21))
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

		// Healthy: succeeds on the first attempt.
		res, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		if err != nil {
			t.Fatalf("unexpected error on a healthy network: %v", err)
		}
		if res.Attempts != 1 {
			t.Fatalf("got %d attempts on a healthy network, want 1", res.Attempts)
		}

		// Degraded, on the connection already established above: rate 1.0
		// drops every write deterministically (no seed dependence), so
		// every attempt times out and the client exhausts its retries.
		h.Network.SetPacketLoss(1.0)
		res, err = h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		if err == nil {
			t.Fatal("expected an error under 100% packet loss, got nil")
		}
		if res.Attempts != 4 {
			t.Fatalf("got %d attempts under saturated loss, want all 4 exhausted", res.Attempts)
		}

		// Healthy again: SetPacketLoss(0.0) is an explicit "never" policy
		// (still draws, per the draw discipline), so the very next attempt
		// succeeds deterministically -- no rebuilding the Network, no
		// re-dial, same live connection throughout.
		h.Network.SetPacketLoss(0.0)
		res, err = h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		if err != nil {
			t.Fatalf("expected recovery after SetPacketLoss(0.0), got error: %v", err)
		}
		if res.Attempts != 1 {
			t.Fatalf("got %d attempts after recovery, want 1", res.Attempts)
		}
	})
}

func TestDialerFor_BoundedWaitAgainstPartitionedPeer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		network := netchaos.NewNetwork(netchaos.WithSeed(23))

		ln, err := network.Listen("tcp", chaostest.ServerPeer)
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		t.Cleanup(func() { ln.Close() })

		// DialerFor is the fix for a client constructor that only accepts
		// a plain net.Dial-shaped function (no context parameter) but
		// still needs to be both partition-targetable and boundable --
		// unlike plain Dial, which hangs forever against a partitioned
		// peer with no way to time out.
		dial := network.DialerFor("client", netchaos.WithDialTimeout(5*time.Second))

		network.Partition("client", chaostest.ServerPeer)

		start := time.Now()
		_, err = dial("tcp", chaostest.ServerPeer)
		elapsed := time.Since(start)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got error %v, want context.DeadlineExceeded", err)
		}
		if elapsed < 5*time.Second {
			t.Fatalf("returned after %v, want to have waited out the full 5s WithDialTimeout", elapsed)
		}
	})
}

func TestBandwidth_ThrottlesDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Sized to serialize in well under the client's default 500ms
		// per-attempt timeout (chaostest.Start doesn't expose overriding
		// it), while still producing a delay clearly attributable to
		// throttling rather than scheduling noise.
		const rate = 2000 // bytes/sec
		h := chaostest.Start(t, netchaos.WithBandwidth(rate), netchaos.WithSeed(29))

		body := make([]byte, 400)
		for i := range body {
			body[i] = 'a'
		}
		item := inventory.Item{ID: "sku-1", Name: string(body), Stock: 10}
		h.Store.Create(item)

		start := time.Now()
		res, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("got status %d, want %d", res.StatusCode, http.StatusOK)
		}

		// The ~400-byte JSON response alone takes ~200ms to serialize at
		// 2000 B/s; assert a conservative lower bound rather than an exact
		// figure, since the response also carries JSON structure/field
		// overhead beyond the 400-byte name.
		if elapsed < 100*time.Millisecond {
			t.Fatalf("elapsed %v, want at least 100ms of serialization delay at %d B/s for a ~400B response", elapsed, rate)
		}
	})
}

func TestLatencyBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const fixed = 200 * time.Millisecond
		h := chaostest.Start(t, netchaos.WithLatency(fixed, fixed), netchaos.WithSeed(9))
		h.Store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 10})

		start := time.Now()
		res, err := h.Client.Do(context.Background(), http.MethodGet, "/items/sku-1", nil)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("got status %d, want %d", res.StatusCode, http.StatusOK)
		}

		// Fault unit = one Write call, not a simulated packet, and not a
		// round trip -- a single GET is at least two writes (request out,
		// response back), so fixed latency accrues at least twice. Assert
		// only a lower bound; asserting an upper bound assumes a
		// one-write round trip that doesn't hold for real HTTP.
		if elapsed < 2*fixed {
			t.Fatalf("elapsed %v, want at least %v (>= 2 writes at fixed latency)", elapsed, 2*fixed)
		}
	})
}
