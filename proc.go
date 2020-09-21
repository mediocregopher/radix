package radix

import (
	"context"
	"errors"
	"sync"
)

var errPreviouslyClosed = errors.New("previously closed")

// proc implements a lightweight pattern for setting up and tearing down
// go-routines cleanly and consistently.
type proc struct {
	ctx         context.Context
	ctxCancelFn context.CancelFunc
	ctxDoneCh   <-chan struct{}

	closeOnce sync.Once
	closeErr  error
	closed    bool
	wg        sync.WaitGroup

	lock sync.RWMutex
}

func newProc() proc {
	ctx, cancel := context.WithCancel(context.Background())
	return proc{
		ctx:         ctx,
		ctxCancelFn: cancel,
		ctxDoneCh:   ctx.Done(),
	}
}

func (p *proc) run(fn func(ctx context.Context)) {
	p.wg.Add(1)
	go func() {
		fn(p.ctx)
		p.wg.Done()
	}()
}

func (p *proc) close(fn func() error) error {
	return p.prefixedClose(func() error { return nil }, fn)
}

func (p *proc) prefixedClose(prefixFn, fn func() error) error {
	err := errPreviouslyClosed
	p.closeOnce.Do(func() {
		err = prefixFn()
		p.ctxCancelFn()
		p.wg.Wait()
		p.lock.Lock()
		defer p.lock.Unlock()
		p.closed = true
		if fn != nil {
			if fnErr := fn(); err == nil {
				err = fnErr
			}
		}
	})
	return err
}

func (p *proc) closedCh() <-chan struct{} {
	return p.ctxDoneCh
}

func (p *proc) isClosed() bool {
	select {
	case <-p.ctxDoneCh:
		return true
	default:
		return false
	}
}

func (p *proc) withRLock(fn func() error) error {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.closed {
		return errPreviouslyClosed
	}
	return fn()
}

func (p *proc) withLock(fn func() error) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return errPreviouslyClosed
	}
	return fn()
}
