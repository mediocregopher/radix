package radix

import "github.com/mediocregopher/radix.v2/redis"

// Pool is an entity which can be used to manage a set of open Conns which can
// be used by multiple go-routines.
type Pool interface {
	// Get retrieves an available Conn for use by a single go-routine (until a
	// subsequent call to Put), or returns an error if that's not possible.
	Get() (Conn, error)

	// Put takes in a Conn previously returned by Put and returns to the Pool. A
	// defunct Conn (i.e. one which has been closed or encountered an IOErr) may
	// be passed in, so Put needs to handle that case.
	//
	// Only Conns obtained through Get should be passed into Put.
	Put(Conn)

	// Close closes all Conns owned by the Pool and cleans up the Pool's
	// resources. Once Close is called no other methods should be called on the
	// Pool.
	Close()
}

type lastCritConn struct {
	Conn

	// The most recent network error which occurred when either reading
	// or writing. A critical network error is basically any non-application
	// level error, e.g. a timeout, disconnect, etc... Close is automatically
	// called on the client when it encounters a critical network error
	lastIOErr error
}

func (lc lastCritConn) Write(r redis.Resp) error {
	if lc.lastIOErr != nil {
		return lc.lastIOErr
	}

	err := lc.Conn.Write(r)
	if err != nil {
		lc.lastIOErr = err
	}
	return err
}

func (lc lastCritConn) Read() redis.Resp {
	if lc.lastIOErr != nil {
		return *redis.NewRespIOErr(lc.lastIOErr)
	}

	r := lc.Conn.Read()
	if r.IsType(redis.IOErr) {
		lc.lastIOErr = r.Err
	}
	return r
}

type staticPool struct {
	pool          chan Conn
	df            DialFunc
	network, addr string
}

// NewPool creates a new Pool whose connections are all created using the given
// DialFunc (or Dial, if the given DialFunc is nil).  The size indicates the
// maximum number of idle connections to have waiting to be used at any given
// moment. If an error is encountered an empty (but still usable) pool is
// returned alongside that error
//
// The implementation of Pool returned here is a semi-dynamic pool. It holds a fixed
// number of connections open. If Get is called and there are no available
// Conns it will create a new one on the spot (using the DialFunc). If Put is
// called and the Pool is full that Conn will be closed and discarded. In this
// way spikes are handled rather well, but sustained over-use will cause
// connection churn and will need the size to be increased.
func NewPool(network, addr string, size int, df DialFunc) (Pool, error) {
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

	// TODO if df is nil, use Dial

	sp := staticPool{
		network: network,
		addr:    addr,
		pool:    make(chan Conn, size),
		df:      df,
	}
	for i := range pool {
		sp.pool <- pool[i]
	}
	return sp, err
}

func (sp staticPool) Get() (Conn, error) {
	select {
	case c := <-sp.pool:
		return c, nil
	default:
		c, err := sp.df(sp.network, sp.addr)
		if err != nil {
			return nil, err
		}
		return lastCritConn{Conn: c}, nil
	}
}

func (sp staticPool) Put(c Conn) {
	lc, ok := c.(lastCritConn)
	// !ok is a weird edge-case that could theoretically happen, even
	// though the docs disallow it. We have no idea if the connection is ok
	// to use or not, so just Close it and move on
	if !ok || lc.lastIOErr != nil {
		c.Close()
		return
	}

	select {
	case sp.pool <- c:
	default:
		c.Close()
	}
}

func (sp staticPool) Close() {
	for {
		select {
		case c := <-sp.pool:
			c.Close()
		default:
			close(sp.pool)
			return
		}
	}
}

type poolCmder struct {
	p Pool
}

// PoolCmder takes a Pool and wraps it to support the Cmd method. When Cmd is
// called the PoolCmder will call Get, then run the command on the returned
// Conn, then call Put on the Conn.
func PoolCmder(p Pool) Cmder {
	return poolCmder{p: p}
}

func (pc poolCmder) Cmd(cmd string, args ...interface{}) redis.Resp {
	c, err := pc.p.Get()
	if err != nil {
		return *redis.NewResp(err)
	}
	defer pc.p.Put(c)

	return ConnCmder(c).Cmd(cmd, args...)
}
