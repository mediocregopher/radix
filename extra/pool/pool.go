// The pool package implements a connection pool for redis connections which is
// thread-safe
package pool

import (
	"github.com/fzzy/radix/redis"
)

// A simple connection pool. It will create a small pool of initial connections,
// and if more connections are needed they will be created on demand. If a
// connection is returned and the pool is full it will be closed.
type Pool struct {
	network string
	addr    string
	pool    chan *redis.Client
	df      DialFunc
}

// A function which can be passed into NewCustomPool
type DialFunc func(network, addr string) (*redis.Client, error)

// A Pool whose connections are all created using f(network, addr). The size
// indicates the maximum number of idle connections to have waiting to be used
// at any given moment. f will be used to create all new connections associated
// with this pool.
//
// The following is an example of using NewCustomPool to have all connections
// automatically get AUTH called on them upon creation
//
// 	df := func(network, addr string) (*redis.Client, error) {
// 		client, err := redis.Dial(network, addr)
// 		if err != nil {
// 			return nil, err
// 		}
// 		if err = client.Cmd("AUTH", "SUPERSECRET").Err; err != nil {
// 			client.Close()
// 			return nil, err
// 		}
// 		return client, nil
// 	}
// 	p, _ := pool.NewCustomPool("tcp", "127.0.0.1:6379", 10, df)
//
func NewCustomPool(network, addr string, size int, df DialFunc) (*Pool, error) {
	pool := make([]*redis.Client, 0, size)
	for i := 0; i < size; i++ {
		client, err := df(network, addr)
		if err != nil {
			for _, client = range pool {
				client.Close()
			}
			return nil, err
		}
		if client != nil {
			pool = append(pool, client)
		}
	}
	p := Pool{
		network: network,
		addr:    addr,
		pool:    make(chan *redis.Client, len(pool)),
		df:      df,
	}
	for i := range pool {
		p.pool <- pool[i]
	}
	return &p, nil
}

// Creates a new Pool whose connections are all created using
// redis.Dial(network, addr). The size indicates the maximum number of idle
// connections to have waiting to be used at any given moment
func NewPool(network, addr string, size int) (*Pool, error) {
	return NewCustomPool(network, addr, size, redis.Dial)
}

// Calls NewPool, but if there is an error it return a pool of the same size but
// without any connections pre-initialized (can be used the same way, but if
// this happens there might be something wrong with the redis instance you're
// connecting to)
func NewOrEmptyPool(network, addr string, size int) *Pool {
	pool, err := NewPool(network, addr, size)
	if err != nil {
		pool = &Pool{
			network: network,
			addr:    addr,
			pool:    make(chan *redis.Client, size),
			df:      redis.Dial,
		}
	}
	return pool
}

// Retrieves an available redis client. If there are none available it will
// create a new one on the fly
func (p *Pool) Get() (*redis.Client, error) {
	select {
	case conn := <-p.pool:
		return conn, nil
	default:
		return p.df(p.network, p.addr)
	}
}

// Returns a client back to the pool. If the pool is full the client is closed
// instead. If the client is already closed (due to connection failure or
// what-have-you) it should not be put back in the pool. The pool will create
// more connections as needed.
func (p *Pool) Put(conn *redis.Client) {
	select {
	case p.pool <- conn:
	default:
		conn.Close()
	}
}

// A useful helper method which acts as a wrapper around Put. It will only
// actually Put the conn back if potentialErr is not an error or is a
// redis.CmdError. It would be used like the following:
//
//	func doSomeThings(p *Pool) error {
//		conn, redisErr := p.Get()
//		if redisErr != nil {
//			return redisErr
//		}
//		defer p.CarefullyPut(conn, &redisErr)
//
//		var i int
//		i, redisErr = conn.Cmd("GET", "foo").Int()
//		if redisErr != nil {
//			return redisErr
//		}
//
//		redisErr = conn.Cmd("SET", "foo", i * 3).Err
//		return redisErr
//	}
//
// If we were just using the normal Put we wouldn't be able to defer it because
// we don't want to Put back a connection which is broken. This method takes
// care of doing that check so we can still use the convenient defer
func (p *Pool) CarefullyPut(conn *redis.Client, potentialErr *error) {
	if potentialErr != nil && *potentialErr != nil {
		// We don't care about command errors, they don't indicate anything
		// about the connection integrity
		if _, ok := (*potentialErr).(*redis.CmdError); !ok {
			conn.Close()
			return
		}
	}
	p.Put(conn)
}

// Removes and calls Close() on all the connections currently in the pool.
// Assuming there are no other connections waiting to be Put back this method
// effectively closes and cleans up the pool.
func (p *Pool) Empty() {
	var conn *redis.Client
	for {
		select {
		case conn = <-p.pool:
			conn.Close()
		default:
			return
		}
	}
}
