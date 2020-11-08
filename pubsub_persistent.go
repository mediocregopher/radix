package radix

import (
	"context"
	"time"

	"github.com/mediocregopher/radix/v4/internal/proc"
)

// PersistentPubSubConfig is used to create a persistent PubSubConn with
// particular settings. All fields are optional, all methods are thread-safe.
type PersistentPubSubConfig struct {
	// Dialer is used to create new Conns.
	Dialer Dialer

	// AbortAfter changes the reconnect behavior of the persistent PubSubConn.
	// Usually a persistent PubSubConn will try to reconnect forever upon a
	// disconnect, blocking any methods which have been called until reconnect
	// is successful.
	//
	// When AbortAfter is used, it will give up after that many attempts and
	// return the error to the method being called. In this case another method
	// must be called in order for reconnection to be tried again.
	AbortAfter int

	// ErrCh is a channel which asynchronous errors encountered by the
	// persistent PubSubConn will be written to. If the channel blocks the error
	// will be dropped. The channel will be closed when the PubSubConn is
	// closed.
	ErrCh chan<- error
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
	proc *proc.Proc
	cfg  PersistentPubSubConfig
	dial func(context.Context) (Conn, error)

	subs, psubs chanSet

	curr      PubSubConn
	currErrCh chan error

	cmdCh chan pubSubCmd
}

// New is like NewPubSubConn, but instead of taking in an existing Conn to wrap
// it will create its own using the network/address returned from the given
// callback.
//
// If the Conn is ever severed then the callback will be re-called, a new Conn
// will be created, and that Conn will be reset to the previous Conn's state.
//
// This is effectively a way to have a permanent PubSubConn established which
// supports subscribing/unsubscribing but without the hassle of implementing
// reconnect/re-subscribe logic.
func (cfg PersistentPubSubConfig) New(ctx context.Context, cb func() (network, addr string)) (PubSubConn, error) {
	p := &persistentPubSub{
		proc: proc.New(),
		dial: func(ctx context.Context) (Conn, error) {
			var network, addr string
			if cb != nil {
				network, addr = cb()
			}
			return cfg.Dialer.Dial(ctx, network, addr)
		},
		cfg:   cfg,
		subs:  chanSet{},
		psubs: chanSet{},
		cmdCh: make(chan pubSubCmd),
	}
	if err := p.refresh(ctx); err != nil {
		return nil, err
	}
	p.proc.Run(p.spin)
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
		c, err := p.dial(ctx)
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
		if p.cfg.AbortAfter > 0 && attempts >= p.cfg.AbortAfter {
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
	case p.cfg.ErrCh <- err:
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
	return p.proc.WithLock(func() error {
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
	return p.proc.Close(func() error {
		var closeErr error
		if p.curr != nil {
			closeErr = p.curr.Close()
			<-p.currErrCh
		}
		if p.cfg.ErrCh != nil {
			close(p.cfg.ErrCh)
		}
		return closeErr
	})
}
