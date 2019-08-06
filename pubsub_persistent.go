package radix

import (
	"sync"
	"time"
)

type persistentPubSubOpts struct {
	cf         ConnFunc
	abortAfter int
}

// PersistentPubSubOpt is an optional behavior which can be applied to the PersistentPubSub function
// to effect a PersistentPubSub's behavior
type PersistentPubSubOpt func(*persistentPubSubOpts)

// PersistentPubSubConnFunc tells the PersistentPubSub to use the given ConnFunc when creating new
// Conns to its redis instance. The ConnFunc can be used to set timeouts,
// perform AUTH, or even use custom Conn implementations.
func PersistentPubSubConnFunc(cf ConnFunc) PersistentPubSubOpt {
	return func(opts *persistentPubSubOpts) {
		opts.cf = cf
	}
}

// PersistentPubSubAbortAfter tells the PersistentPubSub to give up
// and return an error after this many attempts to establish a connection.
// The error will be returned to whichever method happens to be being called at that moment.
//
// A value of 0 indiciates no limit on retry attempts.
func PersistentPubSubAbortAfter(attempts int) PersistentPubSubOpt {
	return func(opts *persistentPubSubOpts) {
		opts.abortAfter = attempts
	}
}

type persistentPubSub struct {
	dial func() (Conn, error)

	l           sync.Mutex
	curr        PubSubConn
	opts        persistentPubSubOpts
	subs, psubs chanSet
	closeCh     chan struct{}
}

// PersistentPubSub is like PubSub, but instead of taking in an existing Conn to
// wrap it will create one on the fly. If the connection is ever terminated then
// a new one will be created using the connFn (which defaults to DefaultConnFunc
// if nil) and will be reset to the previous connection's state.
//
// This is effectively a way to have a permanent PubSubConn established which
// supports subscribing/unsubscribing but without the hassle of implementing
// reconnect/re-subscribe logic.
//
// None of the methods on the returned PubSubConn will ever return an error,
// they will instead block until a connection can be successfully reinstated.
func PersistentPubSub(network, addr string, opts ...PersistentPubSubOpt) (PubSubConn, error) {
	p := &persistentPubSub{
		subs:    chanSet{},
		psubs:   chanSet{},
		closeCh: make(chan struct{}),
	}

	defaultOpts := []PersistentPubSubOpt{
		PersistentPubSubConnFunc(DefaultConnFunc),
		// When the abortAfter option not set
		// there are no limit for connection attempts
		PersistentPubSubAbortAfter(0),
	}

	for _, opt := range append(defaultOpts, opts...) {
		if opt != nil {
			opt(&p.opts)
		}
	}

	p.dial = func() (Conn, error) {
		return p.opts.cf(network, addr)
	}

	if err := p.refresh(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *persistentPubSub) refresh() error {
	if p.curr != nil {
		p.curr.Close()
	}

	attempt := func() (PubSubConn, error) {
		c, err := p.dial()
		if err != nil {
			return nil, err
		}
		errCh := make(chan error, 1)
		pc := newPubSub(c, errCh)

		for msgCh, channels := range p.subs.inverse() {
			if err := pc.Subscribe(msgCh, channels...); err != nil {
				pc.Close()
				return nil, err
			}
		}

		for msgCh, patterns := range p.psubs.inverse() {
			if err := pc.PSubscribe(msgCh, patterns...); err != nil {
				pc.Close()
				return nil, err
			}
		}

		go func() {
			select {
			case <-errCh:
				p.l.Lock()
				// It's possible that one of the methods (e.g. Subscribe)
				// already had the lock, saw the error, and called refresh. This
				// check prevents a double-refresh in that case.
				if p.curr == pc {
					p.refresh()
				}
				p.l.Unlock()
			case <-p.closeCh:
			}
		}()

		return pc, nil
	}

	var err error
	var attempts int
	for {
		if p.opts.abortAfter > 0 && attempts >= p.opts.abortAfter {
			return err
		}

		p.curr, err = attempt()
		if p.curr != nil {
			return nil
		}

		attempts++
		select {
		case <-time.After(200 * time.Millisecond):
		case <-p.closeCh:
			return err
		}
	}
}

func (p *persistentPubSub) Subscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	p.l.Lock()
	defer p.l.Unlock()

	// add first, so if the actual call fails then refresh will catch it
	for _, channel := range channels {
		p.subs.add(channel, msgCh)
	}

	if err := p.curr.Subscribe(msgCh, channels...); err == nil {
		return nil
	}
	return p.refresh()
}

func (p *persistentPubSub) Unsubscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	p.l.Lock()
	defer p.l.Unlock()

	// remove first, so if the actual call fails then refresh will catch it
	for _, channel := range channels {
		p.subs.del(channel, msgCh)
	}

	if err := p.curr.Unsubscribe(msgCh, channels...); err == nil {
		return nil
	}
	return p.refresh()
}

func (p *persistentPubSub) PSubscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	p.l.Lock()
	defer p.l.Unlock()

	// add first, so if the actual call fails then refresh will catch it
	for _, channel := range channels {
		p.psubs.add(channel, msgCh)
	}

	if err := p.curr.PSubscribe(msgCh, channels...); err == nil {
		return nil
	}
	return p.refresh()
}

func (p *persistentPubSub) PUnsubscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	p.l.Lock()
	defer p.l.Unlock()

	// remove first, so if the actual call fails then refresh will catch it
	for _, channel := range channels {
		p.psubs.del(channel, msgCh)
	}

	if err := p.curr.PUnsubscribe(msgCh, channels...); err == nil {
		return nil
	}
	return p.refresh()
}

func (p *persistentPubSub) Ping() error {
	p.l.Lock()
	defer p.l.Unlock()

	for {
		if err := p.curr.Ping(); err == nil {
			break
		}
		p.refresh()
	}
	return nil
}

func (p *persistentPubSub) Close() error {
	p.l.Lock()
	defer p.l.Unlock()
	close(p.closeCh)
	if p.curr == nil {
		return nil
	}
	return p.curr.Close()
}
