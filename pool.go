package radix

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix/v3/resp"
)

// ErrPoolEmpty is used by Pools created using the PoolOnEmptyErrAfter option
var ErrPoolEmpty = errors.New("connection pool is empty")

var errPoolFull = errors.New("connection pool is full")

// TODO do something with errors which happen asynchronously

type staticPoolConn struct {
	Conn
	sp *Pool

	// The most recent network error which occurred when either reading
	// or writing. A critical network error is basically any non-application
	// level error, e.g. a timeout, disconnect, etc... Close is automatically
	// called on the client when it encounters a critical network error
	lastIOErr error
}

func (spc *staticPoolConn) Encode(m resp.Marshaler) error {
	err := spc.Conn.Encode(m)
	if nerr, _ := err.(net.Error); nerr != nil {
		spc.lastIOErr = err
	}
	return err
}

func (spc *staticPoolConn) Decode(m resp.Unmarshaler) error {
	err := spc.Conn.Decode(m)
	if nerr, _ := err.(net.Error); nerr != nil {
		spc.lastIOErr = err
	}
	return err
}

func (spc *staticPoolConn) Do(a Action) error {
	return a.Run(spc)
}

func (spc *staticPoolConn) Close() error {
	spc.lastIOErr = io.EOF
	return spc.Conn.Close()
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
func PoolOnFullBuffer(size int, drainInterval time.Duration) PoolOpt {
	return func(po *poolOpts) {
		po.overflowSize = size
		po.overflowDrainInterval = drainInterval
	}
}

////////////////////////////////////////////////////////////////////////////////

// Pool is a semi-dynamic pool which holds a fixed number of connections open
// and which implements the Client interface. It takes in a number of options
// which can effect its specific behavior, see the NewPool method.
type Pool struct {
	po            poolOpts
	network, addr string
	size          int

	l sync.RWMutex
	// totalConns is only really needed by the refill part of the code to ensure
	// it's not overly refilling the pool. It is protected by l.
	totalConns int
	// pool is read-protected by l, and should not be written to or read from
	// when closed is true (closed is also protected by l)
	pool   chan *staticPoolConn
	closed bool

	wg       sync.WaitGroup
	closeCh  chan bool
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
//	PoolPingInterval(10 * time.Second / (size+1))
//
func NewPool(network, addr string, size int, opts ...PoolOpt) (*Pool, error) {
	sp := &Pool{
		network:  network,
		addr:     addr,
		size:     size,
		closeCh:  make(chan bool),
		initDone: make(chan struct{}),
	}

	defaultPoolOpts := []PoolOpt{
		PoolConnFunc(DefaultConnFunc),
		PoolOnEmptyCreateAfter(1 * time.Second),
		PoolRefillInterval(1 * time.Second),
		PoolOnFullBuffer((size/3)+1, 1*time.Second),
		PoolPingInterval(10 * time.Second / time.Duration(size+1)),
	}

	for _, opt := range append(defaultPoolOpts, opts...) {
		// the other args to NewPool used to be a ConnFunc, which someone might
		// have left as nil, in which case this now gives a weird panic. Just
		// handle it
		if opt != nil {
			opt(&(sp.po))
		}
	}

	totalSize := size + sp.po.overflowSize
	sp.pool = make(chan *staticPoolConn, totalSize)

	// make one Conn synchronously to ensure there's actually a redis instance
	// present. The rest will be created asynchronously.
	spc, err := sp.newConn(false) // false in case size is zero
	if err != nil {
		return nil, err
	}
	sp.put(spc)

	sp.wg.Add(1)
	go func() {
		defer sp.wg.Done()
		for i := 0; i < size-1; i++ {
			spc, err := sp.newConn(true)
			if err == nil {
				sp.put(spc)
			}
			// TODO do something with that error?
		}
		close(sp.initDone)
	}()

	if sp.po.pingInterval > 0 && size > 0 {
		sp.atIntervalDo(sp.po.pingInterval, func() { sp.Do(Cmd(nil, "PING")) })
	}
	if sp.po.refillInterval > 0 && size > 0 {
		sp.atIntervalDo(sp.po.refillInterval, sp.doRefill)
	}
	if sp.po.overflowSize > 0 && sp.po.overflowDrainInterval > 0 {
		sp.atIntervalDo(sp.po.overflowDrainInterval, sp.doOverflowDrain)
	}
	return sp, nil
}

// this must always be called with sp.l unlocked
func (sp *Pool) newConn(errIfFull bool) (*staticPoolConn, error) {
	c, err := sp.po.cf(sp.network, sp.addr)
	if err != nil {
		return nil, err
	}
	spc := &staticPoolConn{
		Conn: c,
		sp:   sp,
	}

	// We don't want to wrap the entire function in a lock because dialing might
	// take a while, but we also don't want to be making any new connections if
	// the pool is closed
	sp.l.Lock()
	defer sp.l.Unlock()
	if sp.closed {
		spc.Close()
		return nil, errClientClosed
	} else if errIfFull && sp.totalConns >= sp.size {
		spc.Close()
		return nil, errPoolFull
	}
	sp.totalConns++

	return spc, nil
}

func (sp *Pool) atIntervalDo(d time.Duration, do func()) {
	sp.wg.Add(1)
	go func() {
		defer sp.wg.Done()
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				do()
			case <-sp.closeCh:
				return
			}
		}
	}()
}

func (sp *Pool) doRefill() {
	// this is a preliminary check to see if more conns are needed. Technically
	// it's not needed, as newConn will do the same one, but it will also incur
	// creating a connection and fully locking the mutex. We can handle the
	// majority of cases here with a much less expensive read-lock.
	sp.l.RLock()
	if sp.totalConns >= sp.size {
		sp.l.RUnlock()
		return
	}
	sp.l.RUnlock()

	spc, err := sp.newConn(true)
	// TODO do something with this error? not if it's errPoolFull
	if err == nil {
		sp.put(spc)
	}
}

func (sp *Pool) doOverflowDrain() {
	// the other do* processes inherently handle this case, this one needs to do
	// it manually
	sp.l.RLock()

	if sp.closed || len(sp.pool) <= sp.size {
		sp.l.RUnlock()
		return
	}

	// pop a connection off and close it, if there's any to pop off
	var spc *staticPoolConn
	select {
	case spc = <-sp.pool:
	default:
		// pool is empty, nothing to drain
	}
	sp.l.RUnlock()

	if spc == nil {
		return
	}

	spc.Close()
	sp.l.Lock()
	sp.totalConns--
	sp.l.Unlock()
}

func (sp *Pool) getExisting() (*staticPoolConn, error) {
	sp.l.RLock()
	defer sp.l.RUnlock()

	if sp.closed {
		return nil, errClientClosed
	}

	// Fast-path if the pool is not empty.
	select {
	case spc := <-sp.pool:
		return spc, nil
	default:
	}

	if sp.po.onEmptyWait == 0 {
		// If we should not wait we return without allocating a timer.
		return nil, sp.po.errOnEmpty
	}

	// only set when we have a timeout, since a nil channel always blocks which
	// is what we want
	var tc <-chan time.Time
	if sp.po.onEmptyWait > 0 {
		t := getTimer(sp.po.onEmptyWait)
		defer putTimer(t)

		tc = t.C
	}

	select {
	case spc := <-sp.pool:
		return spc, nil
	case <-tc:
		return nil, sp.po.errOnEmpty
	}
}

func (sp *Pool) get() (*staticPoolConn, error) {
	spc, err := sp.getExisting()
	if err != nil {
		return nil, err
	} else if spc != nil {
		return spc, nil
	}

	// at this point everything is unlocked and the conn needs to be created.
	// newConn will handle checking if the pool has been closed since the inner
	// was called.
	return sp.newConn(false)
}

func (sp *Pool) put(spc *staticPoolConn) {
	sp.l.RLock()
	if spc.lastIOErr == nil && !sp.closed {
		select {
		case sp.pool <- spc:
			sp.l.RUnlock()
			return
		default:
		}
	}

	sp.l.RUnlock()
	// the pool might close here, but that's fine, because all that's happening
	// at this point is that the connection is being closed
	spc.Close()
	sp.l.Lock()
	sp.totalConns--
	sp.l.Unlock()
}

// Do implements the Do method of the Client interface by retrieving a Conn out
// of the pool, calling Run on the given Action with it, and returning the Conn
// to the pool
func (sp *Pool) Do(a Action) error {
	c, err := sp.get()
	if err != nil {
		return err
	}
	defer sp.put(c)

	return c.Do(a)
}

// NumAvailConns returns the number of connections currently available in the
// pool, as well as in the overflow buffer if that option is enabled.
func (sp *Pool) NumAvailConns() int {
	return len(sp.pool)
}

// Close implements the Close method of the Client
func (sp *Pool) Close() error {
	sp.l.Lock()
	if sp.closed {
		sp.l.Unlock()
		return errClientClosed
	}
	sp.closed = true
	close(sp.closeCh)

	// at this point get and put won't work anymore, so it's safe to empty and
	// close the pool channel
emptyLoop:
	for {
		select {
		case spc := <-sp.pool:
			spc.Close()
			sp.totalConns--
		default:
			close(sp.pool)
			break emptyLoop
		}
	}
	sp.l.Unlock()

	// by now the pool's go-routines should have bailed, wait to make sure they
	// do
	sp.wg.Wait()
	return nil
}
