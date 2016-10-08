package radix

import "time"

// PoolCmder is a ConnCmder which came from a Pool, and which has the
// special property of being able to be returned to the Pool it came from
type PoolCmder interface {
	Cmder

	// Done, when called, indicates that the PoolCmder will no longer be
	// used by its current borrower and should be returned to the Pool it came
	// from. This _must_ be called by all borrowers, or their PoolCmders
	// will never be put back in their Pools.
	//
	// May not be called after Close is called on the PoolCmder
	Done()
}

// Pool is an entity which can be used to manage a set of open ConnCmders which
// can be used by multiple go-routines.
type Pool interface {
	// Get retrieves an available PoolCmder for use by a single go-routine
	// (until a subsequent call to Done on it), or returns an error if that's
	// not possible.
	//
	// The key passed in should be one of the keys involved in the
	// command/commands being performed. This is helps to support cluster, for
	// normal pool implementations this key may be ignored.
	Get(forKey string) (PoolCmder, error)

	// Put takes in a ConnCmder previously returned by Get and returns it to the
	// Pool. A defunct ConnCmder (i.e. one which has been closed or encountered
	// an IOErr) may be passed in, so Put needs to handle that case (probably by
	// discarding it).
	//
	// Only ConnCmders obtained through Get should be passed into the same
	// Pool's Put.
	//Put(ConnCmder)

	// Close closes all PoolCmders in the Pool and cleans up the Pool's
	// resources. Once Close is called no other methods should be called on the
	// Pool, though Done may still be called on PoolCmders which haven't
	// been returned yet (these will be closed at that point as well).
	Close()

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
	ConnCmder
	sp *staticPool

	// The most recent network error which occurred when either reading
	// or writing. A critical network error is basically any non-application
	// level error, e.g. a timeout, disconnect, etc... Close is automatically
	// called on the client when it encounters a critical network error
	lastIOErr error
}

func (spc *staticPoolConn) Done() {
	if spc.sp == nil {
		panic("Done called on Closed PoolCmder")
	}
	spc.sp.put(spc)
}

func (spc *staticPoolConn) Write(m interface{}) error {
	if spc.lastIOErr != nil {
		return spc.lastIOErr
	}

	err := spc.ConnCmder.Write(m)
	if err != nil {
		spc.lastIOErr = err
	}
	return err
}

func (spc *staticPoolConn) Read() Resp {
	if spc.lastIOErr != nil {
		return ioErrResp(spc.lastIOErr)
	}

	r := spc.ConnCmder.Read()
	if _, ok := r.Err.(IOErr); ok {
		spc.lastIOErr = r.Err
	}
	return r
}

func (spc *staticPoolConn) Close() error {
	// in case there's some kind of problem with circular reference and gc, also
	// prevents Done from being called
	spc.sp = nil
	return spc.ConnCmder.Close()
}

type staticPool struct {
	pool          chan *staticPoolConn
	df            DialFunc
	network, addr string
	closed        chan struct{}
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
// TODO make this return a PoolCmder
func NewPool(network, addr string, size int, df DialFunc) (Pool, error) {
	sp := &staticPool{
		network: network,
		addr:    addr,
		df:      df,
		pool:    make(chan *staticPoolConn, size),
		closed:  make(chan struct{}),
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

	return &staticPoolConn{
		ConnCmder: NewConnCmder(c),
		sp:        sp,
	}, nil
}

func (sp *staticPool) Get(forkey string) (PoolCmder, error) {
	select {
	case spc := <-sp.pool:
		return spc, nil
	default:
		return sp.newConn()
	}
}

func (sp *staticPool) put(spc *staticPoolConn) {
	if spc.lastIOErr != nil {
		spc.Close()
		return
	}

	select {
	case <-sp.closed:
		spc.Close()
		return
	default:
		select {
		case sp.pool <- spc:
		default:
			spc.Close()
		}
	}
}

func (sp *staticPool) Close() {
	close(sp.closed)
	for {
		select {
		case spc := <-sp.pool:
			spc.Close()
		default:
			close(sp.pool)
			return
		}
	}
}

type poolCmder struct {
	p Pool
}

// NewPoolCmder takes a Pool and wraps it to support the Cmd method. When Cmd is
// called the PoolCmder will call Get, then run the command on the returned
// Conn, then call Put on the Conn.
func NewPoolCmder(p Pool) Cmder {
	return poolCmder{p: p}
}

func (pc poolCmder) Cmd(cmd string, args ...interface{}) Resp {
	k := NewCmd(cmd, args...).FirstArg()
	c, err := pc.p.Get(k)
	if err != nil {
		return ioErrResp(err)
	}
	defer c.Done()

	return c.Cmd(cmd, args...)
}

type poolPinger struct {
	Pool
	closeCh chan struct{}
	doneCh  chan struct{}
}

// NewPoolPinger will periodically call Get on the given Pool, do a PING
// command, then return the connection to the Pool. This effectively tests the
// connections and cleans our the dead ones.
//
// Close must be called on the returned Pool in order to properly clean up.
func NewPoolPinger(p Pool, period time.Duration) Pool {
	closeCh := make(chan struct{})
	doneCh := make(chan struct{})
	go func() {
		pc := NewPoolCmder(p)
		t := time.NewTicker(period)
		for {
			select {
			case <-t.C:
				pc.Cmd("PING")
			case <-closeCh:
				t.Stop()
				close(doneCh)
				return
			}
		}
	}()

	return poolPinger{Pool: p, closeCh: closeCh, doneCh: doneCh}
}

func (pp poolPinger) Close() {
	close(pp.closeCh)
	<-pp.doneCh
	pp.Pool.Close()
}
