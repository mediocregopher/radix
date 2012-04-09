package radix

import (
	"sync"
)

// connectionPool is a stack-like structure that holds the connections of a Client.
type connectionPool struct {
	size int
	capacity int
	pool []*connection
	lock *sync.Mutex
	fullCond *sync.Cond
	emptyCond *sync.Cond
	configuration *Configuration
}

func newConnectionPool(conf *Configuration) *connectionPool {
	locker := &sync.Mutex{}
	cp := &connectionPool{
		size: conf.PoolSize,
		capacity: conf.PoolSize,
		pool: make([]*connection, conf.PoolSize),
		lock: locker,
		fullCond: sync.NewCond(locker),
		emptyCond: sync.NewCond(locker),
		configuration: conf,
	}

	return cp
}
func (cp *connectionPool) push(conn *connection) {
	if conn != nil && conn.closed {
		// Connection was closed likely due to an error.
		// Don't attempt to reuse closed connections.
		conn = nil
	}

	cp.lock.Lock()
	for cp.size == cp.capacity {
		cp.fullCond.Wait()
	}

	cp.pool[cp.size] = conn
	cp.size++

	cp.emptyCond.Signal()
	cp.lock.Unlock()
}

func (cp *connectionPool) pull() (*connection, *Error) {
	var err *Error

	cp.lock.Lock()
	for cp.size == 0 {
		cp.emptyCond.Wait()
	}

	conn := cp.pool[cp.size - 1]
	if conn == nil {
		// Lazy init of a connection
		conn, err = newConnection(cp.configuration)
		
		if err != nil {
			cp.lock.Unlock()
			return nil, err
		}
	}

	cp.size--
	cp.fullCond.Signal()
	cp.lock.Unlock()

	return conn, nil
}
