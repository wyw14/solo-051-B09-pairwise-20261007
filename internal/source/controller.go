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

	mu     sync.Mutex
	power  float64
	leased int // admission weight held by the active Start; 0 when stopped
}

func NewController(capacity int) *Controller {
	return &Controller{admission: NewAdmission(capacity), heater: &Heater{}, adapter: NewAdapter()}
}
func (c *Controller) Start(ctx context.Context, power float64) error {
	weight := int(power)
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
	c.mu.Lock()
	superseded := c.leased
	c.power = power
	c.leased = weight
	c.mu.Unlock()
	if superseded > 0 {
		// A previous Start was never Stopped (re-entrant layer); release its
		// lease so admission accounting stays balanced.
		c.admission.Release(superseded)
	}
	return nil
}

// Stop releases the active admission lease exactly once. It is idempotent and
// safe to call concurrently: only the first call observes the lease.
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

func (c *Controller) Power() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.power
}
func (c *Controller) AdmissionUsed() int             { return c.admission.Used() }
func (c *Controller) ConfigureBusy(value bool)       { c.adapter.SetBusy(value) }
func (c *Controller) ConfigurePreheatFailures(n int) { c.heater.ConfigureFailures(n) }
func (c *Controller) Layer(ctx context.Context, layer model.Layer) error {
	if err := c.Start(ctx, layer.RateNanometers); err != nil {
		return fmt.Errorf("layer %d: %w", layer.Number, err)
	}
	return nil
}
