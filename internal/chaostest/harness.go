// Package chaostest is the reusable netchaos test harness for this
// example: it wires up a netchaos.Network, an inventory server listening
// on it, and a client dialing through it under a named peer identity.
// Copy this pattern into your own project's tests.
package chaostest

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/jpgomesr/netchaos"

	"github.com/jpgomesr/netchaos-example/internal/client"
	"github.com/jpgomesr/netchaos-example/internal/inventory"
)

// ServerPeer is used as the Listen address, the client's base URL host,
// and every Partition/Heal argument that targets the server side. It is
// intentionally a single constant: a peer name that doesn't byte-for-byte
// match the address a Listen/Dial used never errors, it just silently
// never matches (see the netchaos skill's api.md, "common mistakes").
const ServerPeer = "inventory-api:8080"

// clientPeerName is the identity the client's dialer registers via
// WithPeerName, so Partition("client", ServerPeer) can target it. An
// unnamed dialer gets an un-targetable "ephemeral:N" identity instead.
const clientPeerName = "client"

// Harness bundles everything a chaos test needs: the Network (to call
// Partition/Heal on) and a Client already wired to dial through it.
type Harness struct {
	Network *netchaos.Network
	Client  *client.Client
	Store   *inventory.Store
}

// Start builds a Network with opts (a seed is injected automatically if
// the caller didn't supply one), starts the inventory server listening on
// ServerPeer inside that Network, and returns a Harness whose Client
// dials through the same Network under the "client" peer identity.
//
// Start must be called from inside synctest.Test, and its t.Cleanup runs
// there too: the harness closes the server and listener before the test
// body returns, so the Accept goroutine started by http.Server.Serve
// unblocks with net.ErrClosed instead of leaving synctest.Test waiting on
// a goroutine that never exits.
func Start(t *testing.T, opts ...netchaos.Option) *Harness {
	t.Helper()

	// Put a default seed first so every scenario is reproducible even if
	// the caller forgets one; a caller-supplied WithSeed later in opts
	// still wins, since later options are applied after (and so override)
	// earlier ones for the same setting.
	allOpts := append([]netchaos.Option{netchaos.WithSeed(1)}, opts...)
	network := netchaos.NewNetwork(allOpts...)

	ln, err := network.Listen("tcp", ServerPeer)
	if err != nil {
		t.Fatalf("chaostest: listen on %q: %v", ServerPeer, err)
	}

	store := inventory.NewStore()
	srv := &http.Server{Handler: inventory.NewServer(store)}
	go srv.Serve(ln)

	c := client.New("http://"+ServerPeer, client.WithDialContext(
		func(ctx context.Context, netw, addr string) (net.Conn, error) {
			return network.DialContext(netchaos.WithPeerName(ctx, clientPeerName), netw, addr)
		},
	))

	t.Cleanup(func() {
		srv.Close()
		ln.Close()
		c.CloseIdleConnections()
	})

	return &Harness{Network: network, Client: c, Store: store}
}

// PartitionClient partitions the client from the server. Traffic already
// in flight is silently discarded (not queued); Dial/DialContext against
// the server blocks until Heal or ctx cancellation.
func (h *Harness) PartitionClient() {
	h.Network.Partition(clientPeerName, ServerPeer)
}

// HealClient restores traffic between the client and the server. Safe to
// call even if no partition is in effect (no-op).
func (h *Harness) HealClient() {
	h.Network.Heal(clientPeerName, ServerPeer)
}
