package redis

import (
	"container/list"
	"sync"
)

// connPool is a stack-like structure that holds the connections of a Client.
type connPool struct {
	available int
	free      list.List
	lock      sync.Mutex
	emptyCond *sync.Cond
	config    *Configuration
}

func newConnPool(config *Configuration) *connPool {
	cp := &connPool{
		available: config.PoolCapacity,
		config:    config,
	}

	cp.emptyCond = sync.NewCond(&cp.lock)
	return cp
}

func (cp *connPool) push(conn *connection) {
	cp.lock.Lock()
	defer cp.lock.Unlock()

	if conn != nil {
		if conn.closed == 0 {
			cp.free.PushFront(conn)
		}
	}

	cp.available++
	cp.emptyCond.Signal()
}

func (cp *connPool) pull() (conn *connection, err *Error) {
	cp.lock.Lock()
	defer cp.lock.Unlock()

	for cp.available == 0 {
		cp.emptyCond.Wait()
	}

	if cp.free.Len() > 0 {
		conn, _ = cp.free.Remove(cp.free.Back()).(*connection)
	} else {
		// Lazy creation of a connection
		conn, err = newConnection(cp.config)

		if err != nil {
			return nil, err
		}
	}

	cp.available--
	return conn, nil
}

func (cp *connPool) close() {
	cp.lock.Lock()
	defer cp.lock.Unlock()
	for e := cp.free.Front(); e != nil; e = e.Next() {
		conn, _ := e.Value.(*connection)
		conn.close()
	}
}
