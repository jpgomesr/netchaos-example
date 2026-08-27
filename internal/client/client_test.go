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
