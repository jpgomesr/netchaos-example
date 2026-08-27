package inventory

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NewServer builds the chi router for the inventory API. Both cmd/api
// (real TCP) and the netchaos-backed tests mount this exact handler, so
// there is only one code path to trust.
func NewServer(store *Store) http.Handler {
	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r.Get("/items", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, store.List())
	})

	r.Get("/items/{id}", func(w http.ResponseWriter, r *http.Request) {
		it, err := store.Get(chi.URLParam(r, "id"))
		if errors.Is(err, ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, it)
	})

	r.Post("/items", func(w http.ResponseWriter, r *http.Request) {
		var it Item
		if err := json.NewDecoder(r.Body).Decode(&it); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusCreated, store.Create(it))
	})

	r.Post("/items/{id}/reserve", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Qty int `json:"qty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}

		it, err := store.Reserve(chi.URLParam(r, "id"), body.Qty)
		switch {
		case errors.Is(err, ErrNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case errors.Is(err, ErrInsufficientStock):
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, it)
	})

	return r
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
