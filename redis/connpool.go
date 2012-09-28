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
	config    *Config
	closed    bool
}

func newConnPool(config *Config) *connPool {
	cp := new(connPool)
	cp.available = config.PoolCapacity
	cp.config = config
	cp.closed = false
	cp.emptyCond = sync.NewCond(&cp.lock)
	return cp
}

func (cp *connPool) push(c *conn) {
	cp.lock.Lock()
	defer cp.lock.Unlock()
	if cp.closed {
		return
	}

	if !c.closed() {
		cp.free.PushFront(c)
	}

	cp.available++
	cp.emptyCond.Signal()
}

func (cp *connPool) pull() (c *conn, err *Error) {
	cp.lock.Lock()
	defer cp.lock.Unlock()
	if cp.closed {
		return nil, newError("connection pool is closed", ErrorConnection)
	}

	for cp.available == 0 {
		cp.emptyCond.Wait()
	}

	if cp.free.Len() > 0 {
		c, _ = cp.free.Remove(cp.free.Back()).(*conn)
	} else {
		// Lazy creation of a connection
		c, err = newConn(cp.config)

		if err != nil {
			cp.emptyCond.Signal()
			return nil, err
		}
	}

	cp.available--
	return c, nil
}

func (cp *connPool) close() {
	cp.lock.Lock()
	defer cp.lock.Unlock()
	for e := cp.free.Front(); e != nil; e = e.Next() {
		c, _ := e.Value.(*conn)
		c.close()
	}

	cp.free.Init()
	cp.closed = true
}
