package radix

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/mediocregopher/radix/v3/trace"
)

// ErrPoolEmpty is used by Pools created using the PoolOnEmptyErrAfter option
var ErrPoolEmpty = errors.New("connection pool is empty")

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

func (ioc *ioErrConn) EncodeDecode(ctx context.Context, m resp.Marshaler, u resp.Unmarshaler) error {
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

type poolOpts struct {
	cf                    ConnFunc
	pingInterval          time.Duration
	refillInterval        time.Duration
	overflowDrainInterval time.Duration
	overflowSize          int
	onEmptyWait           time.Duration
	errOnEmpty            error
	pt                    trace.PoolTrace
	errCh                 chan<- error
}

// PoolOpt is an optional behavior which can be applied to the NewPool function
// to effect a Pool's behavior
type PoolOpt func(*poolOpts)

// PoolConnFunc tells the Pool to use the given ConnFunc when creating new
// Conns to its redis instance. The ConnFunc can be used to set timeouts,
// perform AUTH, or even use custom Conn implementations.
func PoolConnFunc(cf ConnFunc) PoolOpt {
	return func(po *poolOpts) {
		po.cf = cf
	}
}

// PoolPingInterval specifies the interval at which a ping event happens. On
// each ping event the Pool calls the PING redis command over one of it's
// available connections.
//
// Since connections are used in LIFO order, the ping interval * pool size is
// the duration of time it takes to ping every connection once when the pool is
// idle.
//
// A shorter interval means connections are pinged more frequently, but also
// means more traffic with the server.
func PoolPingInterval(d time.Duration) PoolOpt {
	return func(po *poolOpts) {
		po.pingInterval = d
	}
}

// PoolRefillInterval specifies the interval at which a refill event happens. On
// each refill event the Pool checks to see if it is full, and if it's not a
// single connection is created and added to it.
func PoolRefillInterval(d time.Duration) PoolOpt {
	return func(po *poolOpts) {
		po.refillInterval = d
	}
}

// PoolOnEmptyWait effects the Pool's behavior when there are no available
// connections in the Pool. The effect is to cause actions to block as long as
// it takes until a connection becomes available.
func PoolOnEmptyWait() PoolOpt {
	return func(po *poolOpts) {
		po.onEmptyWait = -1
	}
}

// PoolOnEmptyCreateAfter effects the Pool's behavior when there are no
// available connections in the Pool. The effect is to cause actions to block
// until a connection becomes available or until the duration has passed. If the
// duration is passed a new connection is created and used.
//
// If wait is 0 then a new connection is created immediately upon an empty Pool.
func PoolOnEmptyCreateAfter(wait time.Duration) PoolOpt {
	return func(po *poolOpts) {
		po.onEmptyWait = wait
		po.errOnEmpty = nil
	}
}

// PoolOnEmptyErrAfter effects the Pool's behavior when there are no
// available connections in the Pool. The effect is to cause actions to block
// until a connection becomes available or until the duration has passed. If the
// duration is passed then ErrEmptyPool is returned.
//
// If wait is 0 then ErrEmptyPool is returned immediately upon an empty Pool.
func PoolOnEmptyErrAfter(wait time.Duration) PoolOpt {
	return func(po *poolOpts) {
		po.onEmptyWait = wait
		po.errOnEmpty = ErrPoolEmpty
	}
}

// PoolOnFullClose effects the Pool's behavior when it is full. The effect is to
// cause any connection which is being put back into a full pool to be closed
// and discarded.
func PoolOnFullClose() PoolOpt {
	return func(po *poolOpts) {
		po.overflowSize = 0
		po.overflowDrainInterval = 0
	}
}

// PoolOnFullBuffer effects the Pool's behavior when it is full. The effect is
// to give the pool an additional buffer for connections, called the overflow.
// If a connection is being put back into a full pool it will be put into the
// overflow. If the overflow is also full then the connection will be closed and
// discarded.
//
// drainInterval specifies the interval at which a drain event happens. On each
// drain event a connection will be removed from the overflow buffer (if any are
// present in it), closed, and discarded.
//
// If drainInterval is zero then drain events will never occur.
//
// NOTE that if used with PoolOnEmptyWait or PoolOnEmptyErrAfter this won't have
// any effect, because there won't be any occasion where more connections than
// the pool size will be created.
func PoolOnFullBuffer(size int, drainInterval time.Duration) PoolOpt {
	return func(po *poolOpts) {
		po.overflowSize = size
		po.overflowDrainInterval = drainInterval
	}
}

// PoolWithTrace tells the Pool to trace itself with the given PoolTrace
// Note that PoolTrace will block every point that you set to trace.
func PoolWithTrace(pt trace.PoolTrace) PoolOpt {
	return func(po *poolOpts) {
		po.pt = pt
	}
}

// PoolErrCh takes a channel which asynchronous errors encountered by the Pool
// can be read off of. If the channel blocks the error will be dropped.  The
// channel will be closed when the Pool is closed.
func PoolErrCh(errCh chan<- error) PoolOpt {
	return func(po *poolOpts) {
		po.errCh = errCh
	}
}

////////////////////////////////////////////////////////////////////////////////

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

	proc          proc
	opts          poolOpts
	network, addr string
	size          int
	pool          chan *ioErrConn

	// TODO replace with tracing
	initDone chan struct{} // used for tests
}

// NewPool creates a *Pool which will keep open at least the given number of
// connections to the redis instance at the given address.
//
// NewPool takes in a number of options which can overwrite its default
// behavior. The default options NewPool uses are:
//
//	PoolConnFunc(DefaultConnFunc)
//	PoolOnEmptyCreateAfter(1 * time.Second)
//	PoolRefillInterval(1 * time.Second)
//	PoolOnFullBuffer((size / 3)+1, 1 * time.Second)
//	PoolPingInterval(5 * time.Second / (size+1))
//	PoolPipelineConcurrency(size)
//
// The recommended size of the pool depends on many factors, such as the number
// of concurrent goroutines that will use the pool.
//
func NewPool(network, addr string, size int, opts ...PoolOpt) (*Pool, error) {
	p := &Pool{
		proc:     newProc(),
		network:  network,
		addr:     addr,
		size:     size,
		initDone: make(chan struct{}),
	}

	defaultPoolOpts := []PoolOpt{
		PoolConnFunc(DefaultConnFunc),
		PoolOnEmptyCreateAfter(1 * time.Second),
		PoolRefillInterval(1 * time.Second),
		PoolOnFullBuffer((size/3)+1, 1*time.Second),
		PoolPingInterval(5 * time.Second / time.Duration(size+1)),
	}

	for _, opt := range append(defaultPoolOpts, opts...) {
		// the other args to NewPool used to be a ConnFunc, which someone might
		// have left as nil, in which case this now gives a weird panic. Just
		// handle it
		if opt != nil {
			opt(&(p.opts))
		}
	}

	totalSize := size + p.opts.overflowSize
	p.pool = make(chan *ioErrConn, totalSize)

	// make one Conn synchronously to ensure there's actually a redis instance
	// present. The rest will be created asynchronously.
	ioc, err := p.newConn(trace.PoolConnCreatedReasonInitialization)
	if err != nil {
		return nil, err
	}
	p.put(ioc)

	p.proc.run(func(ctx context.Context) {
		startTime := time.Now()
		for i := 0; i < size-1; i++ {
			ioc, err := p.newConn(trace.PoolConnCreatedReasonInitialization)
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
		close(p.initDone)
		p.traceInitCompleted(time.Since(startTime))

	})

	if p.opts.pingInterval > 0 && size > 0 {
		p.atIntervalDo(p.opts.pingInterval, func(ctx context.Context) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			p.Do(ctx, Cmd(nil, "PING"))
		})
	}
	if p.opts.refillInterval > 0 && size > 0 {
		p.atIntervalDo(p.opts.refillInterval, p.doRefill)
	}
	if p.opts.overflowSize > 0 && p.opts.overflowDrainInterval > 0 {
		p.atIntervalDo(p.opts.overflowDrainInterval, p.doOverflowDrain)
	}
	return p, nil
}

func (p *Pool) traceInitCompleted(elapsedTime time.Duration) {
	if p.opts.pt.InitCompleted != nil {
		p.opts.pt.InitCompleted(trace.PoolInitCompleted{
			PoolCommon:  p.traceCommon(),
			AvailCount:  len(p.pool),
			ElapsedTime: elapsedTime,
		})
	}
}

func (p *Pool) err(err error) {
	select {
	case p.opts.errCh <- err:
	default:
	}
}

func (p *Pool) traceCommon() trace.PoolCommon {
	return trace.PoolCommon{
		Network: p.network, Addr: p.addr,
		PoolSize: p.size, BufferSize: p.opts.overflowSize,
	}
}

func (p *Pool) traceConnCreated(connectTime time.Duration, reason trace.PoolConnCreatedReason, err error) {
	if p.opts.pt.ConnCreated != nil {
		p.opts.pt.ConnCreated(trace.PoolConnCreated{
			PoolCommon:  p.traceCommon(),
			Reason:      reason,
			ConnectTime: connectTime,
			Err:         err,
		})
	}
}

func (p *Pool) traceConnClosed(reason trace.PoolConnClosedReason) {
	if p.opts.pt.ConnClosed != nil {
		p.opts.pt.ConnClosed(trace.PoolConnClosed{
			PoolCommon: p.traceCommon(),
			AvailCount: len(p.pool),
			Reason:     reason,
		})
	}
}

func (p *Pool) newConn(reason trace.PoolConnCreatedReason) (*ioErrConn, error) {
	start := time.Now()
	c, err := p.opts.cf(p.network, p.addr)
	elapsed := time.Since(start)
	p.traceConnCreated(elapsed, reason, err)
	if err != nil {
		return nil, err
	}
	ioc := newIOErrConn(c)
	atomic.AddInt64(&p.totalConns, 1)
	return ioc, nil
}

func (p *Pool) atIntervalDo(d time.Duration, do func(context.Context)) {
	p.proc.run(func(ctx context.Context) {
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

func (p *Pool) doRefill(context.Context) {
	if atomic.LoadInt64(&p.totalConns) >= int64(p.size) {
		return
	}
	ioc, err := p.newConn(trace.PoolConnCreatedReasonRefill)
	if err == nil {
		p.put(ioc)
	} else if err != errPoolFull {
		p.err(err)
	}
}

func (p *Pool) doOverflowDrain(context.Context) {
	if len(p.pool) <= p.size {
		return
	}

	// pop a connection off and close it, if there's any to pop off
	var ioc *ioErrConn
	select {
	case ioc = <-p.pool:
	default:
		// pool is empty, nothing to drain
	}

	ioc.Close()
	p.traceConnClosed(trace.PoolConnClosedReasonBufferDrain)
	atomic.AddInt64(&p.totalConns, -1)
}

func (p *Pool) getExisting() (*ioErrConn, error) {
	// Fast-path if the pool is not empty. Return error if pool has been closed.
	select {
	case <-p.proc.closedCh():
		return nil, errPreviouslyClosed
	case ioc := <-p.pool:
		return ioc, nil
	default:
	}

	if p.opts.onEmptyWait == 0 {
		// If we should not wait we return without allocating a timer.
		return nil, p.opts.errOnEmpty
	}

	// only set when we have a timeout, since a nil channel always blocks which
	// is what we want
	var tc <-chan time.Time
	if p.opts.onEmptyWait > 0 {
		t := getTimer(p.opts.onEmptyWait)
		defer putTimer(t)

		tc = t.C
	}

	select {
	case <-p.proc.closedCh():
		return nil, errPreviouslyClosed
	case ioc := <-p.pool:
		return ioc, nil
	case <-tc:
		return nil, p.opts.errOnEmpty
	}
}

func (p *Pool) get() (*ioErrConn, error) {
	ioc, err := p.getExisting()
	if err != nil {
		return nil, err
	} else if ioc != nil {
		return ioc, nil
	}
	return p.newConn(trace.PoolConnCreatedReasonPoolEmpty)
}

// returns true if the connection was put back, false if it was closed and
// discarded.
func (p *Pool) put(ioc *ioErrConn) bool {
	if ioc.lastIOErr == nil {
		var ok bool
		_ = p.proc.withRLock(func() error {
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
	startTime := time.Now()
	c, err := p.get() // TODO make get take the context?
	if err != nil {
		return err
	}

	err = c.Do(ctx, a)
	p.put(c)
	p.traceDoCompleted(time.Since(startTime), err)

	return err
}

func (p *Pool) traceDoCompleted(elapsedTime time.Duration, err error) {
	if p.opts.pt.DoCompleted != nil {
		p.opts.pt.DoCompleted(trace.PoolDoCompleted{
			PoolCommon:  p.traceCommon(),
			AvailCount:  len(p.pool),
			ElapsedTime: elapsedTime,
			Err:         err,
		})
	}
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

// Close implements the Close method of the Client
func (p *Pool) Close() error {
	return p.proc.close(func() error {
		p.drain()
		close(p.pool)
		if p.opts.errCh != nil {
			close(p.opts.errCh)
		}
		return nil
	})
}
