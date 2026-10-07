package source

import (
	"context"
	"sync"
	"testing"
)

func TestLeaseOwnershipAndStopIdempotency(t *testing.T) {
	c := NewController(100)
	ctx := context.Background()

	// Idempotent Stop: only the first call releases.
	if err := c.Start(ctx, 10); err != nil {
		t.Fatal(err)
	}
	c.Stop()
	c.Stop()
	c.Stop()
	if got := c.AdmissionUsed(); got != 0 {
		t.Fatalf("after repeated Stop: used=%d, want 0", got)
	}

	// Re-entrant Start must not leak the previous lease.
	if err := c.Start(ctx, 30); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(ctx, 40); err != nil {
		t.Fatal(err)
	}
	if got := c.AdmissionUsed(); got != 40 {
		t.Fatalf("after re-entrant Start: used=%d, want 40", got)
	}
	c.Stop()
	if got := c.AdmissionUsed(); got != 0 {
		t.Fatalf("after final Stop: used=%d, want 0", got)
	}

	// Concurrent Start/Stop churn: quota always returns to zero.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(power float64) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if err := c.Start(ctx, power); err != nil {
					t.Error(err)
					return
				}
				c.Stop()
			}
		}(float64(i + 1))
	}
	wg.Wait()
	if got := c.AdmissionUsed(); got != 0 {
		t.Fatalf("after concurrent churn: used=%d, want 0", got)
	}
	if got := c.Power(); got != 0 {
		t.Fatalf("power=%v, want 0", got)
	}
}
