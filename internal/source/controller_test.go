package source

import (
	"context"
	"sync"
	"testing"
)

func TestStopReleasesAcquiredWeightOnce(t *testing.T) {
	controller := NewController(10)
	if err := controller.Start(context.Background(), 4); err != nil {
		t.Fatalf("start: %v", err)
	}
	controller.Stop()
	controller.Stop()
	if used := controller.AdmissionUsed(); used != 0 {
		t.Fatalf("admission used = %d, want 0 after idempotent stop", used)
	}
}

func TestReentrantStartDoesNotLeakLease(t *testing.T) {
	controller := NewController(10)
	if err := controller.Start(context.Background(), 3); err != nil {
		t.Fatalf("first start: %v", err)
	}
	if err := controller.Start(context.Background(), 5); err != nil {
		t.Fatalf("re-entrant start: %v", err)
	}
	controller.Stop()
	if used := controller.AdmissionUsed(); used != 0 {
		t.Fatalf("admission used = %d, want 0 after re-entrant start and stop", used)
	}
}

func TestConcurrentStartStopKeepsAdmissionBalanced(t *testing.T) {
	controller := NewController(64)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(power float64) {
			defer wg.Done()
			if err := controller.Start(context.Background(), power); err != nil {
				t.Errorf("start: %v", err)
				return
			}
			controller.Stop()
		}(float64(i%3 + 1))
	}
	wg.Wait()
	if used := controller.AdmissionUsed(); used != 0 {
		t.Fatalf("admission used = %d, want 0 after concurrent start/stop", used)
	}
	if power := controller.Power(); power != 0 {
		t.Fatalf("power = %v, want 0 after all stops", power)
	}
}

func TestConcurrentStopsReleaseExactlyOnce(t *testing.T) {
	controller := NewController(10)
	if err := controller.Start(context.Background(), 6); err != nil {
		t.Fatalf("start: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			controller.Stop()
		}()
	}
	wg.Wait()
	if used := controller.AdmissionUsed(); used != 0 {
		t.Fatalf("admission used = %d, want 0 after concurrent stops", used)
	}
	// A subsequent acquire of the full capacity must succeed: no leaked weight.
	if err := controller.admission.Acquire(context.Background(), 10); err != nil {
		t.Fatalf("acquire full capacity after stops: %v", err)
	}
}
