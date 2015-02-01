// Package pool implements a connection pool for redis connections which is
// thread-safe.
//
// Basic usage
//
// The basic use-case is to create a pool and then pass that pool amongst
// multiple go-routines, each of which can use it safely. To retrieve a
// connection you use Get, and to return the connection to the pool when you're
// done with it you use Put.
//
//	p, err := pool.NewPool("tcp", "localhost:6379", 10)
//	if err != nil {
//		// handle error
//	}
//
//	// In another go-routine
//
//	conn, err := p.Get()
//	if err != nil {
//		// handle error
//	}
//
//	if conn.Cmd("SOME", "CMD").Err != nil {
//		// handle error
//	}
//
//	p.Put(conn)
//
// Shortcuts
//
// If you're doing multiple operations you may find it useful to defer the Put
// right after retrieving a connection, so that you don't have to always
// remember to do so
//
//	conn, err := p.Get()
//	if err != nil {
//		// handle error
//	}
//	defer p.Put(conn)
//
//	if conn.Cmd("SOME", "CMD").Err != nil {
//		// handle error
//	}
//
//	if conn.Cmd("SOME", "OTHER", "CMD").Err != nil {
//		// handle error
//	}
//
// Additionally there is the With method, which handles Get-ing and Put-ing for
// you
//
//	err := p.With(func(conn *redis.Client) {
//		if conn.Cmd("SOME", "CMD").Err != nil {
//			// handle error
//		}
//	})
//	if err != nil {
//		// handle error
//	}
//
// Custom connections
//
// Sometimes it's necessary to run some code on each connection in a pool upon
// its creation, for example in the case of AUTH. This can be done with
// NewCustomPool, like so
//
//	df := func(network, addr string) (*redis.Client, error) {
//		client, err := redis.Dial(network, addr)
//		if err != nil {
//			return nil, err
//		}
//		if err = client.Cmd("AUTH", "SUPERSECRET").Err; err != nil {
//			client.Close()
//			return nil, err
//		}
//		return client, nil
//	}
//	p, err := pool.NewCustomPool("tcp", "127.0.0.1:6379", 10, df)
package pool
