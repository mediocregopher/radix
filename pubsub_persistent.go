package radix

import (
	"context"
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
	ctx context.Context

	// msgCh can be set along with one of subscribe/unsubscribe/etc...
	msgCh                                            chan<- PubSubMessage
	subscribe, unsubscribe, psubscribe, punsubscribe []string

	// ... or ping can be set
	ping bool

	// resCh is always set
	resCh chan error
}

type persistentPubSub struct {
	proc proc
	dial func() (Conn, error)
	opts persistentPubSubOpts

	subs, psubs chanSet

	curr      PubSubConn
	currErrCh chan error

	cmdCh chan pubSubCmd
}

// PersistentPubSub is like PubSub, but instead of taking in an existing Conn to
// wrap it will create one on the fly. If the connection is ever terminated then
// a new one will be created and will be reset to the previous connection's
// state.
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
func PersistentPubSub(
	ctx context.Context,
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
		proc:  newProc(),
		dial:  func() (Conn, error) { return opts.connFn(network, addr) },
		opts:  opts,
		subs:  chanSet{},
		psubs: chanSet{},
		cmdCh: make(chan pubSubCmd),
	}
	if err := p.refresh(ctx); err != nil {
		return nil, err
	}
	p.proc.run(p.spin)
	return p, nil
}

// refresh only returns an error if the connection could not be made
func (p *persistentPubSub) refresh(ctx context.Context) error {
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
			if err := pc.Subscribe(ctx, msgCh, channels...); err != nil {
				pc.Close()
				return nil, nil, err
			}
		}

		for msgCh, patterns := range p.psubs.inverse() {
			if err := pc.PSubscribe(ctx, msgCh, patterns...); err != nil {
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
		} else if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
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
		if err := p.refresh(cmd.ctx); err != nil {
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
		err = p.curr.Subscribe(cmd.ctx, cmd.msgCh, cmd.subscribe...)

	case len(cmd.unsubscribe) > 0:
		for _, channel := range cmd.unsubscribe {
			p.subs.del(channel, cmd.msgCh)
		}
		err = p.curr.Unsubscribe(cmd.ctx, cmd.msgCh, cmd.unsubscribe...)

	case len(cmd.psubscribe) > 0:
		for _, channel := range cmd.psubscribe {
			p.psubs.add(channel, cmd.msgCh)
		}
		err = p.curr.PSubscribe(cmd.ctx, cmd.msgCh, cmd.psubscribe...)

	case len(cmd.punsubscribe) > 0:
		for _, channel := range cmd.punsubscribe {
			p.psubs.del(channel, cmd.msgCh)
		}
		err = p.curr.PUnsubscribe(cmd.ctx, cmd.msgCh, cmd.punsubscribe...)

	case cmd.ping:
		err = p.curr.Ping(cmd.ctx)

	default:
		// don't do anything I guess
	}

	if err != nil {
		return p.refresh(cmd.ctx)
	}
	return nil
}

func (p *persistentPubSub) err(err error) {
	select {
	case p.opts.errCh <- err:
	default:
	}
}

func (p *persistentPubSub) spin(ctx context.Context) {
	for {
		select {
		case err := <-p.currErrCh:
			p.err(err)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = p.refresh(ctx)
			cancel()
			if err != nil {
				p.err(err)
			}
		case cmd := <-p.cmdCh:
			cmd.resCh <- p.execCmd(cmd)
		case <-ctx.Done():
			return
		}
	}
}

func (p *persistentPubSub) cmd(cmd pubSubCmd) error {
	return p.proc.withLock(func() error {
		cmd.resCh = make(chan error, 1)
		p.cmdCh <- cmd
		return <-cmd.resCh
	})
}

func (p *persistentPubSub) Subscribe(ctx context.Context, msgCh chan<- PubSubMessage, channels ...string) error {
	return p.cmd(pubSubCmd{
		ctx:       ctx,
		msgCh:     msgCh,
		subscribe: channels,
	})
}

func (p *persistentPubSub) Unsubscribe(ctx context.Context, msgCh chan<- PubSubMessage, channels ...string) error {
	return p.cmd(pubSubCmd{
		ctx:         ctx,
		msgCh:       msgCh,
		unsubscribe: channels,
	})
}

func (p *persistentPubSub) PSubscribe(ctx context.Context, msgCh chan<- PubSubMessage, channels ...string) error {
	return p.cmd(pubSubCmd{
		ctx:        ctx,
		msgCh:      msgCh,
		psubscribe: channels,
	})
}

func (p *persistentPubSub) PUnsubscribe(ctx context.Context, msgCh chan<- PubSubMessage, channels ...string) error {
	return p.cmd(pubSubCmd{
		ctx:          ctx,
		msgCh:        msgCh,
		punsubscribe: channels,
	})
}

func (p *persistentPubSub) Ping(ctx context.Context) error {
	return p.cmd(pubSubCmd{
		ctx:  ctx,
		ping: true,
	})
}

func (p *persistentPubSub) Close() error {
	return p.proc.close(func() error {
		var closeErr error
		if p.curr != nil {
			closeErr = p.curr.Close()
			<-p.currErrCh
		}
		if p.opts.errCh != nil {
			close(p.opts.errCh)
		}
		return closeErr
	})
}
