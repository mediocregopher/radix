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

// TODO not super happy with the naming here

// PoolFunc is a function which can be used to create a Pool of connections to a
// single redis instance on the given network/address.
type PoolFunc func(network, addr string) (Client, error)

// NewPoolFunc returns a PoolFunc which will use this package's NewPool function
// to create a Pool of the given size and using the given DialFunc.
//
// If the DialFunc is nil then this package's Dial function is used.
func NewPoolFunc(size int, df DialFunc) PoolFunc {
	return func(network, addr string) (Client, error) {
		// dunno why I have to do this....
		cl, err := Pool(network, addr, size, df)
		return cl, err
	}
}

type staticPoolConn struct {
	Conn
	sp *staticPool

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

func (spc *staticPoolConn) Close() error {
	return spc.Conn.Close()
}

type staticPool struct {
	df            DialFunc
	network, addr string

	l      sync.RWMutex
	pool   chan *staticPoolConn
	closed bool

	closeCh  chan bool
	initDone chan struct{} // used for tests
}

// Pool creates a Client whose connections are all created using the DialFunc
// (or Dial, if the DialFunc is nil). The size indicates the maximum number of
// idle connections to have waiting to be used at any given moment.
//
// The implementation of Pool returned here is a semi-dynamic pool. It holds a
// fixed number of connections open. If Get is called and there are no available
// Conns it will create a new one on the spot (using the DialFunc). If Put is
// called and the Pool is full that Conn will be closed and discarded. In this
// way spikes are handled rather well, but sustained over-use will cause
// connection churn and will need the size to be increased.
func Pool(network, addr string, size int, df DialFunc) (Client, error) {
	sp := &staticPool{
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

func (sp *staticPool) pingSpin() {
	pingCmd := CmdNoKey(nil, "PING")
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

func (sp *staticPool) get() (*staticPoolConn, error) {
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

func (sp *staticPool) put(spc *staticPoolConn) {
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

func (sp *staticPool) Do(a Action) error {
	c, err := sp.get()
	if err != nil {
		return err
	}
	defer sp.put(c)

	return a.Run(c)
}

func (sp *staticPool) Close() error {
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
