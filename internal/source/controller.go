package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/wyw14/cry-161/internal/model"
)

type Controller struct {
	admission *Admission
	heater    *Heater
	adapter   *Adapter
	mu        sync.Mutex
	power     float64
	leased    int // admission weight currently owned; 0 means no lease held
}

func NewController(capacity int) *Controller {
	return &Controller{admission: NewAdmission(capacity), heater: &Heater{}, adapter: NewAdapter()}
}

// Start transitions the controller to the given power, atomically replacing
// any lease it already owns: the stale lease is released before the new
// weight is acquired, so a re-entrant layer can neither leak quota nor
// deadlock against its own previous lease.
func (c *Controller) Start(ctx context.Context, power float64) error {
	weight := int(power)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseLeaseLocked()
	if err := c.admission.Acquire(ctx, weight); err != nil {
		return err
	}
	if err := c.adapter.Open(); err != nil {
		c.admission.Release(weight)
		return err
	}
	if err := c.heater.Preheat(ctx); err != nil {
		c.admission.Release(weight)
		return err
	}
	c.power = power
	c.leased = weight
	return nil
}

// Stop releases the owned lease. It is idempotent: the lease weight is
// swapped to zero under the mutex, so concurrent or repeated calls release
// exactly once, and always the exact weight that was acquired.
func (c *Controller) Stop() {
	c.mu.Lock()
	weight := c.leased
	c.leased = 0
	c.power = 0
	c.mu.Unlock()
	if weight > 0 {
		c.admission.Release(weight)
	}
}

// releaseLeaseLocked drops any owned lease; caller must hold c.mu.
func (c *Controller) releaseLeaseLocked() {
	weight := c.leased
	c.leased = 0
	c.power = 0
	if weight > 0 {
		c.admission.Release(weight)
	}
}

func (c *Controller) Power() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.power
}

func (c *Controller) AdmissionUsed() int             { return c.admission.Used() }
func (c *Controller) ConfigureBusy(value bool)       { c.adapter.SetBusy(value) }
func (c *Controller) ConfigurePreheatFailures(n int) { c.heater.Reset(n) }
func (c *Controller) Layer(ctx context.Context, layer model.Layer) error {
	if err := c.Start(ctx, layer.RateNanometers); err != nil {
		return fmt.Errorf("layer %d: %w", layer.Number, err)
	}
	return nil
}
