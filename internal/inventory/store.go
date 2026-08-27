// Package inventory implements a tiny in-memory inventory service: an
// HTTP API in front of a mutex-guarded map. It has nothing to do with
// netchaos — it's the plain, boring service that the client package talks
// to over a (possibly chaotic) network.
package inventory

import (
	"errors"
	"sync"
)

// ErrNotFound is returned when an item id doesn't exist in the store.
var ErrNotFound = errors.New("inventory: item not found")

// ErrInsufficientStock is returned when a reservation exceeds available stock.
var ErrInsufficientStock = errors.New("inventory: insufficient stock")

// Item is a single inventory record.
type Item struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Stock int    `json:"stock"`
}

// Store is an in-memory, concurrency-safe item store.
type Store struct {
	mu    sync.RWMutex
	items map[string]Item
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{items: make(map[string]Item)}
}

// List returns all items, in no particular order.
func (s *Store) List() []Item {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Item, 0, len(s.items))
	for _, it := range s.items {
		out = append(out, it)
	}
	return out
}

// Get returns the item with the given id, or ErrNotFound.
func (s *Store) Get(id string) (Item, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	it, ok := s.items[id]
	if !ok {
		return Item{}, ErrNotFound
	}
	return it, nil
}

// Create inserts or replaces an item.
func (s *Store) Create(it Item) Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[it.ID] = it
	return it
}

// Reserve decrements stock for id by qty. It returns ErrNotFound if the
// item doesn't exist, or ErrInsufficientStock if qty exceeds current stock.
func (s *Store) Reserve(id string, qty int) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	it, ok := s.items[id]
	if !ok {
		return Item{}, ErrNotFound
	}
	if qty > it.Stock {
		return Item{}, ErrInsufficientStock
	}
	it.Stock -= qty
	s.items[id] = it
	return it, nil
}
