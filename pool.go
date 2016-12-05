package radix

import (
	"errors"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v2/resp"
)

// PoolConn is a Conn which came from a Pool, and which has the special
// property of being able to be returned to the Pool it came from
type PoolConn interface {
	Conn

	// Return, when called, indicates that the PoolConn will no longer be
	// used by its current borrower and should be returned to the Pool it came
	// from. This _must_ be called by all borrowers, or their PoolConns
	// will never be put back in their Pools.
	//
	// May not be called after Close is called on the PoolConn
	Return()
}

// Pool is a Client which can be used to manage a set of open Conns which can be
// used by multiple go-routines.
type Pool interface {
	Client

	// Get retrieves an available PoolConn for use by a single go-routine
	// (until a subsequent call to Return on it), or returns an error if that's
	// not possible.
	Get() (PoolConn, error)

	// Stats returns any runtime stats that the implementation of Pool wishes to
	// return, or nil if it doesn't want to return any. This method aims to help
	// support logging and debugging, not necessarily to give any actionable
	// information to the program during runtime.
	//
	// Examples of useful runtime stats might be: number of connections
	// currently available, number of connections currently lent out, number of
	// connections ever created, number of connections ever closed, average time
	// to create a new connection, and so on.
	//
	// TODO I'm not sure if I actually like this
	//
	//Stats() map[string]interface{}
}

// PoolFunc is a function which can be used to create a Pool of connections to
// the redis instance on the given network/address.
type PoolFunc func(network, addr string) (Pool, error)

// TODO expose Avail for the pool

type staticPoolConn struct {
	Conn
	sp *staticPool

	// The most recent network error which occurred when either reading
	// or writing. A critical network error is basically any non-application
	// level error, e.g. a timeout, disconnect, etc... Close is automatically
	// called on the client when it encounters a critical network error
	lastIOErr error
}

func (spc *staticPoolConn) Return() {
	if spc.sp == nil {
		panic("Return called on Closed PoolConn")
	}
	spc.sp.put(spc)
}

func (spc *staticPoolConn) Encode(m resp.Marshaler) error {
	if spc.lastIOErr != nil {
		return spc.lastIOErr
	} else if err := spc.Conn.Encode(m); err != nil {
		spc.lastIOErr = err
		return err
	}
	return nil
}

func (spc *staticPoolConn) Decode(m resp.Unmarshaler) error {
	if spc.lastIOErr != nil {
		return spc.lastIOErr
	} else if err := spc.Conn.Decode(m); err != nil {
		spc.lastIOErr = err
		return err
	}
	return nil
}

func (spc *staticPoolConn) Close() error {
	// in case there's some kind of problem with circular reference and gc, also
	// prevents Return from being called
	spc.sp = nil
	return spc.Conn.Close()
}

type staticPool struct {
	pool          chan *staticPoolConn
	df            DialFunc
	network, addr string

	closeL sync.RWMutex
	closed bool
}

// NewPool creates a new Pool whose connections are all created using the given
// DialFunc (or Dial, if the given DialFunc is nil).  The size indicates the
// maximum number of idle connections to have waiting to be used at any given
// moment. If an error is encountered an empty (but still usable) Pool is
// returned alongside that error
//
// The implementation of Pool returned here is a semi-dynamic pool. It holds a
// fixed number of connections open. If Get is called and there are no available
// Conns it will create a new one on the spot (using the DialFunc). If Put is
// called and the Pool is full that Conn will be closed and discarded. In this
// way spikes are handled rather well, but sustained over-use will cause
// connection churn and will need the size to be increased.
func NewPool(network, addr string, size int, df DialFunc) (Pool, error) {
	sp := &staticPool{
		network: network,
		addr:    addr,
		df:      df,
		pool:    make(chan *staticPoolConn, size),
	}
	if sp.df == nil {
		sp.df = Dial
	}

	// First make as many Conns as we can to initialize the pool. If we hit an
	// error bail entirely, we'll return an empty pool
	var spc *staticPoolConn
	var err error
	pool := make([]*staticPoolConn, 0, size)
	for i := 0; i < size; i++ {
		if spc, err = sp.newConn(); err != nil {
			for _, spc := range pool {
				spc.Close()
			}
			pool = pool[0:]
			break
		}
		pool = append(pool, spc)
	}

	for i := range pool {
		sp.pool <- pool[i]
	}
	return sp, err
}

func (sp *staticPool) newConn() (*staticPoolConn, error) {
	c, err := sp.df(sp.network, sp.addr)
	if err != nil {
		return nil, err
	}

	spc := &staticPoolConn{
		Conn: c,
		sp:   sp,
	}
	return spc, nil
}

func (sp *staticPool) isClosed() bool {
	sp.closeL.RLock()
	defer sp.closeL.RUnlock()
	return sp.closed
}

func (sp *staticPool) setClosed(to bool) {
	sp.closeL.Lock()
	defer sp.closeL.Unlock()
	sp.closed = to
}

func (sp *staticPool) Get() (PoolConn, error) {
	if sp.isClosed() {
		return nil, errors.New("pool is closed")
	}

	select {
	case spc := <-sp.pool:
		return spc, nil
	default:
		return sp.newConn()
	}
}

func (sp *staticPool) put(spc *staticPoolConn) {
	if spc.lastIOErr != nil || sp.isClosed() {
		spc.Close()
		return
	}

	select {
	case sp.pool <- spc:
	default:
		spc.Close()
	}
}

func (sp *staticPool) Close() error {
	sp.setClosed(true)
	for {
		select {
		case spc := <-sp.pool:
			spc.Close()
		default:
			close(sp.pool)
			return nil
		}
	}
}

func (sp *staticPool) Do(a Action) error {
	c, err := sp.Get()
	if err != nil {
		return err
	}
	defer c.Return()

	return a.Run(c)
}

type poolPinger struct {
	Pool
	closeCh chan struct{}
	doneCh  chan struct{}
}

// NewPoolPinger will periodically call Get on the given Pool, do a PING
// command, then return the connection to the Pool. This effectively tests the
// connections and cleans our the dead ones.
func NewPoolPinger(p Pool, period time.Duration) Pool {
	closeCh := make(chan struct{})
	doneCh := make(chan struct{})
	pingCmd := CmdNoKey("PING")
	go func() {
		t := time.NewTicker(period)
		for {
			select {
			case <-t.C:
				p.Do(pingCmd)
			case <-closeCh:
				t.Stop()
				close(doneCh)
				return
			}
		}
	}()

	return poolPinger{Pool: p, closeCh: closeCh, doneCh: doneCh}
}

func (pp poolPinger) Close() error {
	close(pp.closeCh)
	<-pp.doneCh
	return pp.Pool.Close()
}
