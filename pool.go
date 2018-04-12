package radix

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v3/resp"
)

// ErrPoolEmpty is used by Pools created using the PoolOnEmptyErrAfter option
var ErrPoolEmpty = errors.New("connection pool is empty")

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
	return spc.Conn.Close()
}

////////////////////////////////////////////////////////////////////////////////

type poolOpts struct {
	cf                    ConnFunc
	pingInterval          time.Duration
	refillInterval        time.Duration
	overflowDrainInterval time.Duration
	overflowSize          int
	onEmptyErr            bool
	onEmptyWait           time.Duration
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
		po.onEmptyErr = false
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
		po.onEmptyErr = true
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
// to cause any connection which is being put back into a full pool to be put
// instead into an overflow buffer which can hold up to the given number of
// connections. If the overflow buffer is also full then the connection is
// closed and discarded.
//
// drainInterval specifies the interval at which a drain event happens. On each
// drain event a connection is removed from the overflow buffer and put into the
// pool. If the pool is full the connection is closed and discarded.
//
// When Actions are performed with the Pool the connection used may come from
// either the main pool or the overflow buffer. Connections do _not_ have to
// wait to be drained into the main pool before they will be used.
func PoolOnFullBuffer(size int, drainInterval time.Duration) PoolOpt {
	return func(po *poolOpts) {
		po.overflowSize = size
		po.overflowDrainInterval = drainInterval
	}
}

// Pool is a semi-dynamic pool which holds a fixed number of connections open
// and which implements the Client interface. It takes in a number of options
// which can effect its specific behavior, see the NewPool method.
type Pool struct {
	po            poolOpts
	network, addr string

	l              sync.RWMutex
	pool, overflow chan *staticPoolConn
	closed         bool

	wg       sync.WaitGroup
	closeCh  chan bool
	initDone chan struct{} // used for tests
}

// NewPool creates a *Pool which will hold up to the given number of connections
// to the redis instance at the given address.
//
// NewPool takes in a number of options which can overwrite its default
// behavior. The default options NewPool uses are:
//
//	PoolConnFunc(Dial)
//	PoolOnEmptyCreateAfter(1 * time.Second)
//	PoolRefillInterval(1 * time.Second)
//	PoolOnFullClose()
//	PoolPingInterval(10 * time.Second / size)
//
func NewPool(network, addr string, size int, opts ...PoolOpt) (*Pool, error) {
	sp := &Pool{
		network:  network,
		addr:     addr,
		pool:     make(chan *staticPoolConn, size),
		closeCh:  make(chan bool),
		initDone: make(chan struct{}),
	}

	defaultPoolOpts := []PoolOpt{
		PoolConnFunc(Dial),
		PoolOnEmptyCreateAfter(1 * time.Second),
		PoolRefillInterval(1 * time.Second),
		PoolOnFullClose(),
	}
	// if pool size is 0 don't start up a pingSpin, cause there'd be no point
	if size > 0 {
		pingOpt := PoolPingInterval(10 * time.Second / time.Duration(size))
		defaultPoolOpts = append(defaultPoolOpts, pingOpt)
	}

	for _, opt := range append(defaultPoolOpts, opts...) {
		// the other args to NewPool used to be a ConnFunc, which someone might
		// have left as nil, in which case this now gives a weird panic. Just
		// handle it
		if opt != nil {
			opt(&(sp.po))
		}
	}

	// make one Conn synchronously to ensure there's actually a redis instance
	// present. The rest will be created asynchronously
	spc, err := sp.newConn()
	if err != nil {
		return nil, err
	}
	sp.put(spc)

	sp.wg.Add(1)
	go func() {
		defer sp.wg.Done()
		for i := 0; i < size-1; i++ {
			spc, err := sp.newConn()
			if err == nil {
				sp.put(spc)
			}
		}
		close(sp.initDone)
	}()

	if sp.po.pingInterval > 0 {
		sp.atIntervalDo(sp.po.pingInterval, func() { sp.Do(Cmd(nil, "PING")) })
	}
	if sp.po.refillInterval > 0 && size > 0 {
		sp.atIntervalDo(sp.po.refillInterval, sp.doRefill)
	}
	if sp.po.overflowDrainInterval > 0 && sp.po.overflowSize > 0 {
		sp.overflow = make(chan *staticPoolConn, sp.po.overflowSize)
		sp.atIntervalDo(sp.po.overflowDrainInterval, sp.doOverflowDrain)
	}
	return sp, nil
}

func (sp *Pool) newConn() (*staticPoolConn, error) {
	c, err := sp.po.cf(sp.network, sp.addr)
	if err != nil {
		return nil, err
	}

	spc := &staticPoolConn{
		Conn: c,
		sp:   sp,
	}
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
	if len(sp.pool) == cap(sp.pool) {
		return
	}
	spc, err := sp.newConn()
	if err == nil {
		sp.put(spc)
	}
}

func (sp *Pool) doOverflowDrain() {
	// If the overflow has a connection we first try to move it to the main
	// pool, but if that's full then just close the connection
	select {
	case spc := <-sp.overflow:
		select {
		case sp.pool <- spc:
		default:
			spc.Close()
		}
	default:
	}
}

func (sp *Pool) get() (*staticPoolConn, error) {
	sp.l.RLock()
	defer sp.l.RUnlock()
	if sp.closed {
		return nil, errClientClosed
	}

	// if an error is written to waitCh get returns that, otherwise if it's
	// closed get will make a new connection
	waitCh := make(chan error, 1)
	effectWaitCh := func() {
		if sp.po.onEmptyErr {
			waitCh <- ErrPoolEmpty
		} else {
			close(waitCh)
		}
	}

	if sp.po.onEmptyWait == -1 {
		// block, waitCh is never effected
	} else if sp.po.onEmptyWait == 0 {
		effectWaitCh()
	} else {
		// TODO it might be worthwhile to use a sync.Pool for timers, rather
		// than creating a new one for every single get
		t := time.AfterFunc(sp.po.onEmptyWait, effectWaitCh)
		defer t.Stop()
	}

	select {
	case spc := <-sp.pool:
		return spc, nil
	case spc := <-sp.overflow:
		return spc, nil
	case err := <-waitCh:
		if err != nil {
			return nil, err
		}
		return sp.newConn()
	}
}

func (sp *Pool) put(spc *staticPoolConn) {
	sp.l.RLock()
	defer sp.l.RUnlock()
	if spc.lastIOErr != nil || sp.closed {
		spc.Close()
		return
	}

	select {
	case sp.pool <- spc:
	default:
		select {
		case sp.overflow <- spc:
		default:
			spc.Close()
		}
	}
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
	return len(sp.pool) + len(sp.overflow)
}

// Close implements the Close method of the Client
func (sp *Pool) Close() error {
	sp.l.Lock()
	if sp.closed {
		return errClientClosed
	}
	sp.closed = true
	close(sp.closeCh)
	sp.l.Unlock()

	// at this point get and put won't work anymore, so it's safe to empty and
	// close the pool channel
emptyLoop:
	for {
		select {
		case spc := <-sp.pool:
			spc.Close()
		default:
			close(sp.pool)
			break emptyLoop
		}
	}

	// by now the pool's go-routines should have bailed, wait to make sure they
	// do
	sp.wg.Wait()
	return nil
}
