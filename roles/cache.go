package roles

import (
	"sync"
	"time"
)

// lifetime is the longest that an answer serves reads, whatever validity it states (ADR 0031).
const lifetime = 60 * time.Second

// cache keeps the answers that reads may use, each until its validity ends, and never longer
// than lifetime after it was asked. It evicts no answer on an event (ADR 0056). A store removes
// the answers that have ended, at most once per lifetime, so that the cache holds only answers
// asked during the last two lifetimes.
type cache[K comparable, V any] struct {
	mu      sync.Mutex
	entries map[K]entry[V]
	swept   time.Time
}

// entry is one answer, and until when reads may use it.
type entry[V any] struct {
	value V
	until time.Time
}

// get returns the answer kept for key, if reads may still use it at now.
func (c *cache[K, V]) get(key K, now time.Time) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || !now.Before(e.until) {
		var none V
		return none, false
	}
	return e.value, true
}

// put keeps value, asked at asked, for key until validUntil, and never longer than lifetime
// after asked.
func (c *cache[K, V]) put(key K, value V, asked, validUntil, now time.Time) {
	until := asked.Add(lifetime)
	if validUntil.Before(until) {
		until = validUntil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[K]entry[V]{}
	}
	if now.Sub(c.swept) >= lifetime {
		for k, e := range c.entries {
			if !now.Before(e.until) {
				delete(c.entries, k)
			}
		}
		c.swept = now
	}
	c.entries[key] = entry[V]{value: value, until: until}
}
