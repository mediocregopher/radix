package radix

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v2/resp"
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
	pool          chan *staticPoolConn
	df            DialFunc
	network, addr string

	closeL sync.RWMutex
	closed bool
}

// Pool creates a Client whose connections are all created using the DialFunc
// (or Dial, if the DialFunc is nil). The size indicates the maximum number of
// idle connections to have waiting to be used at any given moment. If an error
// is encountered an empty (but still usable) Pool is returned alongside that
// error
//
// TODO make initialization mostly asynchronous and not bother with this
// "returning empty but usable" nonsense
//
// The implementation of Pool returned here is a semi-dynamic pool. It holds a
// fixed number of connections open. If Get is called and there are no available
// Conns it will create a new one on the spot (using the DialFunc). If Put is
// called and the Pool is full that Conn will be closed and discarded. In this
// way spikes are handled rather well, but sustained over-use will cause
// connection churn and will need the size to be increased.
func Pool(network, addr string, size int, df DialFunc) (Client, error) {
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

func (sp *staticPool) get() (*staticPoolConn, error) {
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

func (sp *staticPool) Do(a Action) error {
	c, err := sp.get()
	if err != nil {
		return err
	}
	defer sp.put(c)

	return a.Run(c)
}

func (sp *staticPool) Close() error {
	sp.setClosed(true)
	for {
		select {
		case spc := <-sp.pool:
			spc.Close()
		default:
			close(sp.pool)
			// TODO race condition here, if a connection were to get returned
			// here it would be no bueno. setClosed/isClosed can't be counted on
			// to protect here
			return nil
		}
	}
}

type clientPinger struct {
	Client
	closeCh chan struct{}
}

// TODO do this in Pool automatically?

// Pinger will periodically call Ping on a Conn held by the Client. This
// effectively tests the Client's connections and cleans our the dead ones.
func Pinger(cl Client, period time.Duration) Client {
	closeCh := make(chan struct{})
	go func() {
		pingCmd := CmdNoKey("PING")
		t := time.NewTicker(period)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				cl.Do(pingCmd)
			case <-closeCh:
				return
			}
		}
	}()

	return clientPinger{Client: cl, closeCh: closeCh}
}

func (clP clientPinger) Close() error {
	clP.closeCh <- struct{}{}
	close(clP.closeCh)
	return clP.Client.Close()
}
