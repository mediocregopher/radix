package radix

import (
	"sync"
)

// connPool is a stack-like structure that holds the connections of a Client.
type connPool struct {
	available int
	capacity  int
	pool      []*connection
	lock      sync.Mutex
	fullCond  *sync.Cond
	emptyCond *sync.Cond
	config    *Configuration
}

func newConnPool(config *Configuration) *connPool {
	cp := &connPool{
		available: config.PoolSize,
		capacity:  config.PoolSize,
		pool:      make([]*connection, config.PoolSize),
		config:    config,
	}
	cp.fullCond  = sync.NewCond(&cp.lock)
	cp.emptyCond = sync.NewCond(&cp.lock)

	return cp
}
func (cp *connPool) push(conn *connection) {
	if conn != nil && conn.closed {
		// Connection was closed likely due to an error.
		// Don't attempt to reuse closed connections.
		conn = nil
	}

	cp.lock.Lock()
	for cp.available == cp.capacity {
		cp.fullCond.Wait()
	}

	cp.pool[cp.available] = conn
	cp.available++

	cp.emptyCond.Signal()
	cp.lock.Unlock()
}

func (cp *connPool) pull() (*connection, *Error) {
	var err *Error

	cp.lock.Lock()
	for cp.available == 0 {
		cp.emptyCond.Wait()
	}

	conn := cp.pool[cp.available-1]
	if conn == nil {
		// Lazy init of a connection
		conn, err = newConnection(cp.config)

		if err != nil {
			cp.lock.Unlock()
			return nil, err
		}
	}

	cp.available--
	cp.fullCond.Signal()
	cp.lock.Unlock()

	return conn, nil
}

func (cp *connPool) close() {
	cp.lock.Lock()
	defer cp.lock.Unlock()

	for i, conn := range cp.pool {
		if conn != nil {
			conn.close()
			cp.pool[i] = nil
		}
	}
}
