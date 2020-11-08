package radix

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/mediocregopher/radix/v4/internal/proc"
	"github.com/mediocregopher/radix/v4/resp"
	"github.com/mediocregopher/radix/v4/trace"
)

var errPoolFull = errors.New("connection pool is full")

// ioErrConn is a Conn which tracks the last net.Error which was seen either
// during an Encode call or a Decode call
type ioErrConn struct {
	Conn

	// The most recent network error which occurred when either reading
	// or writing. A critical network error is basically any non-application
	// level error, e.g. a timeout, disconnect, etc... Close is automatically
	// called on the client when it encounters a critical network error
	lastIOErr error
}

func newIOErrConn(c Conn) *ioErrConn {
	return &ioErrConn{Conn: c}
}

func (ioc *ioErrConn) EncodeDecode(ctx context.Context, m, u interface{}) error {
	if ioc.lastIOErr != nil {
		return ioc.lastIOErr
	}
	err := ioc.Conn.EncodeDecode(ctx, m, u)
	if err != nil && !errors.As(err, new(resp.ErrConnUsable)) {
		ioc.lastIOErr = err
	}
	return err
}

func (ioc *ioErrConn) Do(ctx context.Context, a Action) error {
	return a.Perform(ctx, ioc)
}

func (ioc *ioErrConn) Close() error {
	ioc.lastIOErr = io.EOF
	return ioc.Conn.Close()
}

////////////////////////////////////////////////////////////////////////////////

// PoolConfig is used to create Pool instances with particular settings. All
// fields are optional, all methods are thread-safe.
type PoolConfig struct {
	// CustomPool indicates that this callback should be used in place of
	// NewClient when NewClient is called. All behavior of NewClient is
	// superceded when this is set.
	CustomPool func(ctx context.Context, network, addr string) (Client, error)

	// Dialer is used by Pool to create new Conns to the Pool's redis instance.
	Dialer Dialer

	// Size indicates the minimum number of Conns the Pool will attempt to
	// maintain.
	//
	// If -1 then the Pool will not maintain any open Conns and all Actions will
	// result in the creation and closing of a fresh Conn (except for where the
	// overflow buffer is used, see OverflowBufferSize).
	//
	// Defaults to 4.
	Size int

	// PingInterval specifies the interval at which a ping event happens. On
	// each ping event the Pool calls the PING redis command one one of its
	// available connections.
	//
	// Since connections are used in LIFO order, the ping interval * pool size
	// is the duration of time it takes to ping every connection once when the
	// pool is idle.
	//
	// If not given then the default value is calculated to be roughly 5 seconds
	// to check every connection in the Pool.
	PingInterval time.Duration

	// RefillInterval specifies the interval at which a refill event happens. On
	// each refill event the Pool checks to see if it is full, and if it's not a
	// single connection is created and added to it.
	//
	// Defaults to 1 second.
	RefillInterval time.Duration

	// OnEmptyCreateImmediately effects the Pool's behavior when there are no
	// available connections in the Pool. The effect is to cause a new Conn to
	// be created immediately and used.
	//
	// OnEmptyCreateImmediately is mutually exclusive with OnEmptyCreateAfter.
	OnEmptyCreateImmediately bool

	// OnEmptyCreateAfter effects the Pool's behavior when there are no
	// available connections in the Pool. The effect is to cause actions to
	// block until a Conn becomes available or until the duration has passed. If
	// the duration is passed a new connection is created and used.
	//
	// If -1 then the Pool will block indefinitely (or until the passed in
	// Context is cancelled).
	//
	// Defaults to 1 second.
	OnEmptyCreateAfter time.Duration

	// OverflowBufferSize indicates how big the overflow buffer should be. The
	// overflow buffer is used when a Conn is being put back into a full Pool;
	// the Conn will instead be put into the buffer, making it available for
	// use. If the overflow buffer is full then the Conn is closed.
	//
	// See OverflowBufferDrainInterval for details on how the overflow is
	// drained.
	//
	// If OnEmptyCreateAfter is -1 this won't have any effect, because there
	// won't be any occasion where more connections than the pool size will be
	// created.
	//
	// If -1 then no overflow buffer will be used.
	//
	// If not given the default value is calculated to be roughly 1/3 of the
	// size of the Pool.
	OverflowBufferSize int

	// OverflowBufferDrainInterval indicates the interval at which a drain event
	// happens. On each drain event a Conn will be removed from the overflow
	// buffer (if any are present in it), closed, and discarded.
	//
	// Defaults to 1 second.
	OverflowBufferDrainInterval time.Duration

	// Trace contains callbacks that a Pool can use to trace itself.
	//
	// All callbacks are blocking.
	Trace trace.PoolTrace

	// ErrCh is a channel which asynchronous errors encountered by the Pool will
	// be written to. If the channel blocks the error will be dropped. The
	// channel will be closed when the Pool is closed.
	ErrCh chan<- error
}

func (cfg PoolConfig) withDefaults() PoolConfig {
	if cfg.Size == -1 {
		cfg.Size = 0
	} else if cfg.Size == 0 {
		cfg.Size = 4
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 5 * time.Second / time.Duration(cfg.Size+1)
	}
	if cfg.RefillInterval == 0 {
		cfg.RefillInterval = 1 * time.Second
	}
	if cfg.OnEmptyCreateAfter == -1 {
		cfg.OnEmptyCreateAfter = 0
	} else if cfg.OnEmptyCreateAfter == 0 {
		cfg.OnEmptyCreateAfter = 1 * time.Second
	}
	if cfg.OverflowBufferSize == -1 {
		cfg.OverflowBufferSize = 0
	} else if cfg.OverflowBufferSize == 0 {
		cfg.OverflowBufferSize = (cfg.Size / 3) + 1
	}
	if cfg.OverflowBufferDrainInterval == 0 {
		cfg.OverflowBufferDrainInterval = 1 * time.Second
	}
	return cfg
}

// TODO allow for setting a hard upper limit on number of connections while also
// allowing for overflow buffer. See
// https://github.com/mediocregopher/radix/issues/219.

// Pool is a dynamic connection pool which implements the Client interface. It
// takes in a number of options which can effect its specific behavior; see the
// NewPool method.
//
// Pool is dynamic in that it can create more connections on-the-fly to handle
// increased load. The maximum number of extra connections (if any) can be
// configured, along with how long they are kept after load has returned to
// normal.
//
type Pool struct {
	// Atomic fields must be at the beginning of the struct since they must be
	// correctly aligned or else access may cause panics on 32-bit architectures
	// See https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	totalConns int64 // atomic, must only be access using functions from sync/atomic

	proc          *proc.Proc
	cfg           PoolConfig
	network, addr string
	pool          chan *ioErrConn
}

var _ Client = new(Pool)

// NewClient calls New and returns the Pool as a Client. If CustomPool is set in
// the PoolConfig then that is called instead.
func (cfg PoolConfig) NewClient(ctx context.Context, network, addr string) (Client, error) {
	if cfg.CustomPool != nil {
		return cfg.CustomPool(ctx, network, addr)
	}
	p, err := cfg.New(ctx, network, addr)
	return p, err
}

// New initializes and returns a Pool instance using the PoolConfig. This will
// panic if CustomPool is set in the PoolConfig.
func (cfg PoolConfig) New(ctx context.Context, network, addr string) (*Pool, error) {
	if cfg.CustomPool != nil {
		panic("Cannot use PoolConfig.New with CustomPool")
	}

	p := &Pool{
		proc:    proc.New(),
		cfg:     cfg.withDefaults(),
		network: network,
		addr:    addr,
	}

	totalSize := p.cfg.Size + p.cfg.OverflowBufferSize
	p.pool = make(chan *ioErrConn, totalSize)

	// make one Conn synchronously to ensure there's actually a redis instance
	// present. The rest will be created asynchronously.
	ioc, err := p.newConn(ctx, trace.PoolConnCreatedReasonInitialization)
	if err != nil {
		return nil, err
	}
	p.put(ioc)

	p.proc.Run(func(ctx context.Context) {
		startTime := time.Now()
		for i := 0; i < p.cfg.Size-1; i++ {
			ioc, err := p.newConn(ctx, trace.PoolConnCreatedReasonInitialization)
			if err != nil {
				p.err(err)
				// if there was an error connecting to the instance than it
				// might need a little breathing room, redis can sometimes get
				// sad if too many connections are created simultaneously.
				time.Sleep(100 * time.Millisecond)
				continue
			} else if !p.put(ioc) {
				// if the connection wasn't put in it could be for two reasons:
				// - the Pool has already started being used and is full.
				// - Close was called.
				// in any case, bail
				break
			}
		}
		p.traceInitCompleted(time.Since(startTime))

	})

	if p.cfg.PingInterval > 0 && p.cfg.Size > 0 {
		p.atIntervalDo(p.cfg.PingInterval, func(ctx context.Context) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			p.Do(ctx, Cmd(nil, "PING"))
		})
	}
	if p.cfg.RefillInterval > 0 && p.cfg.Size > 0 {
		p.atIntervalDo(p.cfg.RefillInterval, p.doRefill)
	}
	if p.cfg.OverflowBufferSize > 0 && p.cfg.OverflowBufferDrainInterval > 0 {
		p.atIntervalDo(p.cfg.OverflowBufferDrainInterval, p.doOverflowDrain)
	}
	return p, nil
}

func (p *Pool) traceInitCompleted(elapsedTime time.Duration) {
	if p.cfg.Trace.InitCompleted != nil {
		p.cfg.Trace.InitCompleted(trace.PoolInitCompleted{
			PoolCommon:  p.traceCommon(),
			ElapsedTime: elapsedTime,
		})
	}
}

func (p *Pool) err(err error) {
	select {
	case p.cfg.ErrCh <- err:
	default:
	}
}

func (p *Pool) traceCommon() trace.PoolCommon {
	return trace.PoolCommon{
		Network: p.network, Addr: p.addr,
		PoolSize: p.cfg.Size, OverflowBufferSize: p.cfg.OverflowBufferSize,
		AvailCount: len(p.pool),
	}
}

func (p *Pool) traceConnCreated(
	ctx context.Context,
	connectTime time.Duration,
	reason trace.PoolConnCreatedReason,
	err error,
) {
	if p.cfg.Trace.ConnCreated != nil {
		p.cfg.Trace.ConnCreated(trace.PoolConnCreated{
			PoolCommon:  p.traceCommon(),
			Context:     ctx,
			Reason:      reason,
			ConnectTime: connectTime,
			Err:         err,
		})
	}
}

func (p *Pool) traceConnClosed(reason trace.PoolConnClosedReason) {
	if p.cfg.Trace.ConnClosed != nil {
		p.cfg.Trace.ConnClosed(trace.PoolConnClosed{
			PoolCommon: p.traceCommon(),
			Reason:     reason,
		})
	}
}

func (p *Pool) newConn(ctx context.Context, reason trace.PoolConnCreatedReason) (*ioErrConn, error) {
	start := time.Now()
	c, err := p.cfg.Dialer.Dial(ctx, p.network, p.addr)
	elapsed := time.Since(start)
	p.traceConnCreated(ctx, elapsed, reason, err)
	if err != nil {
		return nil, err
	}
	ioc := newIOErrConn(c)
	atomic.AddInt64(&p.totalConns, 1)
	return ioc, nil
}

func (p *Pool) atIntervalDo(d time.Duration, do func(context.Context)) {
	p.proc.Run(func(ctx context.Context) {
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				do(ctx)
			case <-ctx.Done():
				return
			}
		}

	})
}

func (p *Pool) doRefill(ctx context.Context) {
	if atomic.LoadInt64(&p.totalConns) >= int64(p.cfg.Size) {
		return
	}
	ioc, err := p.newConn(ctx, trace.PoolConnCreatedReasonRefill)
	if err == nil {
		p.put(ioc)
	} else if err != errPoolFull {
		p.err(err)
	}
}

func (p *Pool) doOverflowDrain(context.Context) {
	if len(p.pool) <= p.cfg.Size {
		return
	}

	// pop a connection off and close it, if there's any to pop off
	var ioc *ioErrConn
	select {
	case ioc = <-p.pool:
	default:
		// pool is empty, nothing to drain
		return
	}

	ioc.Close()
	p.traceConnClosed(trace.PoolConnClosedReasonBufferDrain)
	atomic.AddInt64(&p.totalConns, -1)
}

func (p *Pool) getExisting(ctx context.Context) (*ioErrConn, error) {
	// Fast-path if the pool is not empty. Return error if pool has been closed.
	select {
	case ioc, ok := <-p.pool:
		if !ok {
			return nil, proc.ErrClosed
		}
		return ioc, nil
	default:
	}

	if p.cfg.OnEmptyCreateImmediately {
		return nil, nil
	}

	// only set when we have a timeout, since a nil channel always blocks which
	// is what we want
	var tc <-chan time.Time
	if p.cfg.OnEmptyCreateAfter > 0 {
		tc = time.After(p.cfg.OnEmptyCreateAfter)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ioc, ok := <-p.pool:
		if !ok {
			return nil, proc.ErrClosed
		}
		return ioc, nil
	case <-tc:
		return nil, nil
	}
}

func (p *Pool) get(ctx context.Context) (*ioErrConn, error) {
	ioc, err := p.getExisting(ctx)
	if err != nil {
		return nil, err
	} else if ioc != nil {
		return ioc, nil
	}
	return p.newConn(ctx, trace.PoolConnCreatedReasonPoolEmpty)
}

// returns true if the connection was put back, false if it was closed and
// discarded.
func (p *Pool) put(ioc *ioErrConn) bool {
	if ioc.lastIOErr == nil {
		var ok bool
		_ = p.proc.WithRLock(func() error {
			select {
			case p.pool <- ioc:
				ok = true
			default:
			}
			return nil
		})
		if ok {
			return true
		}
	}

	// the pool might close here, but that's fine, because all that's happening
	// at this point is that the connection is being closed
	ioc.Close()
	p.traceConnClosed(trace.PoolConnClosedReasonPoolFull)
	atomic.AddInt64(&p.totalConns, -1)
	return false
}

// Do implements the Do method of the Client interface by retrieving a Conn out
// of the pool, calling Perform on the given Action with it, and returning the
// Conn to the pool.
func (p *Pool) Do(ctx context.Context, a Action) error {
	c, err := p.get(ctx)
	if err != nil {
		return err
	}

	err = c.Do(ctx, a)
	p.put(c)

	return err
}

// NumAvailConns returns the number of connections currently available in the
// pool, as well as in the overflow buffer if that option is enabled.
func (p *Pool) NumAvailConns() int {
	return len(p.pool)
}

func (p *Pool) drain() {
	for {
		select {
		case ioc := <-p.pool:
			ioc.Close()
			atomic.AddInt64(&p.totalConns, -1)
			p.traceConnClosed(trace.PoolConnClosedReasonPoolClosed)
		default:
			return
		}
	}
}

// Addr implements the method for the Client interface.
func (p *Pool) Addr() net.Addr {
	return rawAddr{network: p.network, addr: p.addr}
}

// Close implements the method for the Client interface.
func (p *Pool) Close() error {
	return p.proc.Close(func() error {
		p.drain()
		close(p.pool)
		if p.cfg.ErrCh != nil {
			close(p.cfg.ErrCh)
		}
		return nil
	})
}
