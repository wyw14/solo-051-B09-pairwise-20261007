package source

import (
	"errors"
	"fmt"
	"sync"
)

var ErrSourceBusy = errors.New("source busy")

type Adapter struct {
	mu   sync.Mutex
	busy bool
}

func NewAdapter() *Adapter { return &Adapter{} }
func (a *Adapter) SetBusy(value bool) {
	a.mu.Lock()
	a.busy = value
	a.mu.Unlock()
}
func (a *Adapter) Open() error {
	a.mu.Lock()
	busy := a.busy
	a.mu.Unlock()
	if busy {
		return fmt.Errorf("open source: %w", ErrSourceBusy)
	}
	return nil
}
