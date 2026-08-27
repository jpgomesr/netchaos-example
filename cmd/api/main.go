// Command api runs the inventory service over a real TCP listener. It
// mounts the exact same handler (internal/inventory.NewServer) that the
// netchaos-backed tests exercise in internal/client -- proof that the
// simulated-network tests are exercising real production code, not a
// test-only stand-in.
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/jpgomesr/netchaos-example/internal/inventory"
)

func main() {
	store := inventory.NewStore()
	store.Create(inventory.Item{ID: "sku-1", Name: "Widget", Stock: 100})

	addr := ":8080"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: inventory.NewServer(store),
	}

	log.Printf("inventory API listening on %s", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}
