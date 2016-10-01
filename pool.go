package radix

import "time"

// Pool is an entity which can be used to manage a set of open Conns which can
// be used by multiple go-routines.
type Pool interface {
	// Get retrieves an available ConnCmder for use by a single go-routine
	// (until a subsequent call to Put), or returns an error if that's not
	// possible.
	Get() (ConnCmder, error)

	// Put takes in a ConnCmder previously returned by Put and returns to the
	// Pool. A defunct ConnCmder (i.e. one which has been closed or encountered
	// an IOErr) may be passed in, so Put needs to handle that case.
	//
	// Only ConnCmders obtained through Get should be passed into Put.
	Put(ConnCmder)

	// Close closes all ConnCmders owned by the Pool and cleans up the Pool's
	// resources. Once Close is called no other methods should be called on the
	// Pool.
	Close()
}

type lastCritConn struct {
	ConnCmder

	// The most recent network error which occurred when either reading
	// or writing. A critical network error is basically any non-application
	// level error, e.g. a timeout, disconnect, etc... Close is automatically
	// called on the client when it encounters a critical network error
	lastIOErr error
}

func (lc lastCritConn) Write(m interface{}) error {
	if lc.lastIOErr != nil {
		return lc.lastIOErr
	}

	err := lc.ConnCmder.Write(m)
	if err != nil {
		lc.lastIOErr = err
	}
	return err
}

func (lc lastCritConn) Read() Resp {
	if lc.lastIOErr != nil {
		return ioErrResp(lc.lastIOErr)
	}

	r := lc.ConnCmder.Read()
	if _, ok := r.Err.(IOErr); ok {
		lc.lastIOErr = r.Err
	}
	return r
}

// TODO expose Avail for the pool

type staticPool struct {
	pool          chan ConnCmder
	df            DialFunc
	network, addr string
}

// NewPool creates a new Pool whose connections are all created using the given
// DialFunc (or Dial, if the given DialFunc is nil).  The size indicates the
// maximum number of idle connections to have waiting to be used at any given
// moment. If an error is encountered an empty (but still usable) pool is
// returned alongside that error
//
// The implementation of Pool returned here is a semi-dynamic pool. It holds a
// fixed number of connections open. If Get is called and there are no available
// Conns it will create a new one on the spot (using the DialFunc). If Put is
// called and the Pool is full that Conn will be closed and discarded. In this
// way spikes are handled rather well, but sustained over-use will cause
// connection churn and will need the size to be increased.
func NewPool(network, addr string, size int, df DialFunc) (Pool, error) {
	if df == nil {
		df = Dial
	}

	// First make as many Conns as we can to initialize the pool. If we hit an
	// error bail entirely, we'll return an empty pool
	var c Conn
	var err error
	pool := make([]Conn, 0, size)
	for i := 0; i < size; i++ {
		if c, err = df(network, addr); err != nil {
			for _, c = range pool {
				c.Close()
			}
			pool = pool[0:]
			break
		}
		pool = append(pool, c)
	}

	sp := staticPool{
		network: network,
		addr:    addr,
		pool:    make(chan ConnCmder, size),
		df:      df,
	}
	for i := range pool {
		sp.pool <- NewConnCmder(pool[i])
	}
	return sp, err
}

func (sp staticPool) Get() (ConnCmder, error) {
	select {
	case cc := <-sp.pool:
		return cc, nil
	default:
		c, err := sp.df(sp.network, sp.addr)
		if err != nil {
			return nil, err
		}
		return lastCritConn{ConnCmder: NewConnCmder(c)}, nil
	}
}

func (sp staticPool) Put(cc ConnCmder) {
	lc, ok := cc.(lastCritConn)
	// !ok is a weird edge-case that could theoretically happen, even
	// though the docs disallow it. We have no idea if the connection is ok
	// to use or not, so just Close it and move on
	if !ok || lc.lastIOErr != nil {
		cc.Close()
		return
	}

	select {
	case sp.pool <- cc:
	default:
		cc.Close()
	}
}

func (sp staticPool) Close() {
	for {
		select {
		case cc := <-sp.pool:
			cc.Close()
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
	c, err := pc.p.Get()
	if err != nil {
		return ioErrResp(err)
	}
	defer pc.p.Put(c)

	return ConnCmder(c).Cmd(cmd, args...)
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
