package inventory

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// These tests exercise handler logic only, via httptest — no network, no
// netchaos. Anything that actually crosses the wire belongs in
// internal/client, tested through the netchaos harness instead.

func TestHealth(t *testing.T) {
	srv := NewServer(NewStore())
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestCreateAndGetItem(t *testing.T) {
	srv := NewServer(NewStore())

	body, _ := json.Marshal(Item{ID: "sku-1", Name: "Widget", Stock: 10})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/items", bytes.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: got status %d, want %d", rec.Code, http.StatusCreated)
	}

	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/items/sku-1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("get: got status %d, want %d", rec.Code, http.StatusOK)
	}

	var got Item
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Stock != 10 {
		t.Fatalf("got stock %d, want 10", got.Stock)
	}
}

func TestGetItem_NotFound(t *testing.T) {
	srv := NewServer(NewStore())
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/items/missing", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("got status %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestReserve_InsufficientStock(t *testing.T) {
	store := NewStore()
	store.Create(Item{ID: "sku-1", Name: "Widget", Stock: 1})
	srv := NewServer(store)

	body, _ := json.Marshal(map[string]int{"qty": 5})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/items/sku-1/reserve", bytes.NewReader(body)))

	if rec.Code != http.StatusConflict {
		t.Fatalf("got status %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestReserve_Success(t *testing.T) {
	store := NewStore()
	store.Create(Item{ID: "sku-1", Name: "Widget", Stock: 5})
	srv := NewServer(store)

	body, _ := json.Marshal(map[string]int{"qty": 3})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/items/sku-1/reserve", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d", rec.Code, http.StatusOK)
	}

	var got Item
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Stock != 2 {
		t.Fatalf("got stock %d, want 2", got.Stock)
	}
}
