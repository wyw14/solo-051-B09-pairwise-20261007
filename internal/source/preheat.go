package source

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Heater struct {
	mu          sync.Mutex
	StableAfter int
	Attempts    int
}

func (h *Heater) Preheat(ctx context.Context) error {
	h.mu.Lock()
	h.Attempts++
	attempts := h.Attempts
	stableAfter := h.StableAfter
	h.mu.Unlock()
	select {
	case <-time.After(time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if stableAfter > 0 && attempts <= stableAfter {
		return fmt.Errorf("source temperature unstable")
	}
	return nil
}

func (h *Heater) ConfigureFailures(n int) {
	h.mu.Lock()
	h.StableAfter = n
	h.Attempts = 0
	h.mu.Unlock()
}
