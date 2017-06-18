package radix

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v2/resp"
)

var (
	errPoolClosed = errors.New("pool is closed")
)

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

// Pool a semi-dynamic pool which holds a fixed number of connections open. If a
// connection needs to be retrieved from the pool but the pool is empty a new
// connection will be created on-the-fly using ConnFunc. If connection is being
// put back in the pool but the pool is full then the connection will be closed
// and discarded.  In this way spikes are handled rather well, but sustained
// over-use will cause connection churn and will need the pool size to be
// increased.
type Pool struct {
	df            ConnFunc
	network, addr string

	l      sync.RWMutex
	pool   chan *staticPoolConn
	closed bool

	closeCh  chan bool
	initDone chan struct{} // used for tests
}

// NewPool creates a *Pool whose connections are all created using the ConnFunc
// (or Dial, if the ConnFunc is nil). The size indicates the maximum number of
// idle connections to have waiting to be used at any given moment.
func NewPool(network, addr string, size int, df ConnFunc) (*Pool, error) {
	sp := &Pool{
		network:  network,
		addr:     addr,
		df:       df,
		pool:     make(chan *staticPoolConn, size),
		closeCh:  make(chan bool),
		initDone: make(chan struct{}),
	}
	if sp.df == nil {
		sp.df = Dial
	}

	// make one Conn synchronously to ensure there's actually a redis instance
	// present. The rest will be created asynchronously
	spc, err := sp.newConn()
	if err != nil {
		return nil, err
	}
	sp.put(spc)

	go func() {
		for i := 0; i < size-1; i++ {
			spc, err := sp.newConn()
			if err == nil {
				sp.put(spc)
			}
		}
		close(sp.initDone)
	}()

	go sp.pingSpin()
	return sp, nil
}

func (sp *Pool) pingSpin() {
	pingCmd := Cmd(nil, "PING")
	// we want each conn to be pinged every 10 seconds, so divide that by number
	// of conns to know how often to call PING
	t := time.NewTicker(10 * time.Second / time.Duration(cap(sp.pool)))
	defer t.Stop()
	for {
		select {
		case <-t.C:
			sp.Do(pingCmd)
		case <-sp.closeCh:
			return
		}
	}
}

func (sp *Pool) newConn() (*staticPoolConn, error) {
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

func (sp *Pool) get() (*staticPoolConn, error) {
	sp.l.RLock()
	defer sp.l.RUnlock()
	if sp.closed {
		return nil, errPoolClosed
	}

	select {
	case spc := <-sp.pool:
		return spc, nil
	default:
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
		spc.Close()
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
// pool
func (sp *Pool) NumAvailConns() int {
	return len(sp.pool)
}

// Close implements the Close method of the Client
func (sp *Pool) Close() error {
	sp.l.Lock()
	defer sp.l.Unlock()
	if sp.closed {
		return errPoolClosed
	}
	sp.closed = true
	sp.closeCh <- true

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
