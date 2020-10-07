package radix

import (
	"fmt"
	"sync"
	"time"
)

type persistentPubSubOpts struct {
	connFn     ConnFunc
	abortAfter int
	errCh      chan<- error
}

// PersistentPubSubOpt is an optional parameter which can be passed into
// PersistentPubSub in order to affect its behavior.
type PersistentPubSubOpt func(*persistentPubSubOpts)

// PersistentPubSubConnFunc causes PersistentPubSub to use the given ConnFunc
// when connecting to its destination.
func PersistentPubSubConnFunc(connFn ConnFunc) PersistentPubSubOpt {
	return func(opts *persistentPubSubOpts) {
		opts.connFn = connFn
	}
}

// PersistentPubSubAbortAfter changes PersistentPubSub's reconnect behavior.
// Usually PersistentPubSub will try to reconnect forever upon a disconnect,
// blocking any methods which have been called until reconnect is successful.
//
// When PersistentPubSubAbortAfter is used, it will give up after that many
// attempts and return the error to the method which has been blocked the
// longest. Another method will need to be called in order for PersistentPubSub
// to resume trying to reconnect.
func PersistentPubSubAbortAfter(attempts int) PersistentPubSubOpt {
	return func(opts *persistentPubSubOpts) {
		opts.abortAfter = attempts
	}
}

// PersistentPubSubErrCh takes a channel which asynchronous errors
// encountered by the PersistentPubSub can be read off of. If the channel blocks
// the error will be dropped. The channel will be closed when PersistentPubSub
// is closed.
func PersistentPubSubErrCh(errCh chan<- error) PersistentPubSubOpt {
	return func(opts *persistentPubSubOpts) {
		opts.errCh = errCh
	}
}

type pubSubCmd struct {
	// msgCh can be set along with one of subscribe/unsubscribe/etc...
	msgCh                                            chan<- PubSubMessage
	subscribe, unsubscribe, psubscribe, punsubscribe []string

	// ... or one of ping or close can be set
	ping, close bool

	// resCh is always set
	resCh chan error
}

type persistentPubSub struct {
	dial func() (Conn, error)
	opts persistentPubSubOpts

	subs, psubs chanSet

	curr      PubSubConn
	currErrCh chan error

	cmdCh chan pubSubCmd

	closeErr  error
	closeCh   chan struct{}
	closeOnce sync.Once
}

// PersistentPubSubWithOpts is like PubSub, but instead of taking in an existing
// Conn to wrap it will create one on the fly. If the connection is ever
// terminated then a new one will be created and will be reset to the previous
// connection's state.
//
// This is effectively a way to have a permanent PubSubConn established which
// supports subscribing/unsubscribing but without the hassle of implementing
// reconnect/re-subscribe logic.
//
// With default options, neither this function nor any of the methods on the
// returned PubSubConn will ever return an error, they will instead block until
// a connection can be successfully reinstated.
//
// PersistentPubSubWithOpts takes in a number of options which can overwrite its
// default behavior. The default options PersistentPubSubWithOpts uses are:
//
//	PersistentPubSubConnFunc(DefaultConnFunc)
//
func PersistentPubSubWithOpts(
	network, addr string, options ...PersistentPubSubOpt,
) (
	PubSubConn, error,
) {
	opts := persistentPubSubOpts{
		connFn: DefaultConnFunc,
	}
	for _, opt := range options {
		opt(&opts)
	}

	p := &persistentPubSub{
		dial:    func() (Conn, error) { return opts.connFn(network, addr) },
		opts:    opts,
		subs:    chanSet{},
		psubs:   chanSet{},
		cmdCh:   make(chan pubSubCmd),
		closeCh: make(chan struct{}),
	}
	if err := p.refresh(); err != nil {
		return nil, err
	}
	go p.spin()
	return p, nil
}

// PersistentPubSub is deprecated in favor of PersistentPubSubWithOpts instead.
func PersistentPubSub(network, addr string, connFn ConnFunc) PubSubConn {
	var opts []PersistentPubSubOpt
	if connFn != nil {
		opts = append(opts, PersistentPubSubConnFunc(connFn))
	}
	// since PersistentPubSubAbortAfter isn't used, this will never return an
	// error, panic if it does
	p, err := PersistentPubSubWithOpts(network, addr, opts...)
	if err != nil {
		panic(fmt.Sprintf("PersistentPubSubWithOpts impossibly returned an error: %v", err))
	}
	return p
}

// refresh only returns an error if the connection could not be made
func (p *persistentPubSub) refresh() error {
	if p.curr != nil {
		p.curr.Close()
		<-p.currErrCh
		p.curr = nil
		p.currErrCh = nil
	}

	attempt := func() (PubSubConn, chan error, error) {
		c, err := p.dial()
		if err != nil {
			return nil, nil, err
		}
		errCh := make(chan error, 1)
		pc := newPubSub(c, errCh)

		for msgCh, channels := range p.subs.inverse() {
			if err := pc.Subscribe(msgCh, channels...); err != nil {
				pc.Close()
				return nil, nil, err
			}
		}

		for msgCh, patterns := range p.psubs.inverse() {
			if err := pc.PSubscribe(msgCh, patterns...); err != nil {
				pc.Close()
				return nil, nil, err
			}
		}
		return pc, errCh, nil
	}

	var attempts int
	for {
		var err error
		if p.curr, p.currErrCh, err = attempt(); err == nil {
			return nil
		}
		attempts++
		if p.opts.abortAfter > 0 && attempts >= p.opts.abortAfter {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (p *persistentPubSub) execCmd(cmd pubSubCmd) error {
	if p.curr == nil {
		if err := p.refresh(); err != nil {
			return err
		}
	}

	// For all subscribe/unsubscribe/etc... commands the modifications to
	// p.subs/p.psubs are made first, so that if the actual call to curr fails
	// then refresh will still instate the new desired subscription.
	var err error
	switch {
	case len(cmd.subscribe) > 0:
		for _, channel := range cmd.subscribe {
			p.subs.add(channel, cmd.msgCh)
		}
		err = p.curr.Subscribe(cmd.msgCh, cmd.subscribe...)

	case len(cmd.unsubscribe) > 0:
		for _, channel := range cmd.unsubscribe {
			p.subs.del(channel, cmd.msgCh)
		}
		err = p.curr.Unsubscribe(cmd.msgCh, cmd.unsubscribe...)

	case len(cmd.psubscribe) > 0:
		for _, channel := range cmd.psubscribe {
			p.psubs.add(channel, cmd.msgCh)
		}
		err = p.curr.PSubscribe(cmd.msgCh, cmd.psubscribe...)

	case len(cmd.punsubscribe) > 0:
		for _, channel := range cmd.punsubscribe {
			p.psubs.del(channel, cmd.msgCh)
		}
		err = p.curr.PUnsubscribe(cmd.msgCh, cmd.punsubscribe...)

	case cmd.ping:
		err = p.curr.Ping()

	case cmd.close:
		if p.curr != nil {
			err = p.curr.Close()
			<-p.currErrCh
		}

	default:
		// don't do anything I guess
	}

	if err != nil {
		return p.refresh()
	}
	return nil
}

func (p *persistentPubSub) err(err error) {
	select {
	case p.opts.errCh <- err:
	default:
	}
}

func (p *persistentPubSub) spin() {
	for {
		select {
		case err := <-p.currErrCh:
			p.err(err)
			if err := p.refresh(); err != nil {
				p.err(err)
			}
		case cmd := <-p.cmdCh:
			cmd.resCh <- p.execCmd(cmd)
			if cmd.close {
				return
			}
		}
	}
}

func (p *persistentPubSub) cmd(cmd pubSubCmd) error {
	cmd.resCh = make(chan error, 1)
	select {
	case p.cmdCh <- cmd:
		return <-cmd.resCh
	case <-p.closeCh:
		return fmt.Errorf("closed")
	}
}

func (p *persistentPubSub) Subscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	return p.cmd(pubSubCmd{
		msgCh:     msgCh,
		subscribe: channels,
	})
}

func (p *persistentPubSub) Unsubscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	return p.cmd(pubSubCmd{
		msgCh:       msgCh,
		unsubscribe: channels,
	})
}

func (p *persistentPubSub) PSubscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	return p.cmd(pubSubCmd{
		msgCh:      msgCh,
		psubscribe: channels,
	})
}

func (p *persistentPubSub) PUnsubscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	return p.cmd(pubSubCmd{
		msgCh:        msgCh,
		punsubscribe: channels,
	})
}

func (p *persistentPubSub) Ping() error {
	return p.cmd(pubSubCmd{ping: true})
}

func (p *persistentPubSub) Close() error {
	p.closeOnce.Do(func() {
		p.closeErr = p.cmd(pubSubCmd{close: true})
		close(p.closeCh)
		if p.opts.errCh != nil {
			close(p.opts.errCh)
		}
	})
	return p.closeErr
}
