package redis

import (
	"container/list"
	"sync"
)

// connPool is a stack-like structure that holds the connections of a Client.
type connPool struct {
	sync.Mutex
	available int
	free      list.List
	emptyCond *sync.Cond
	config    *Config
	closed    bool
}

func newConnPool(config *Config) *connPool {
	cp := new(connPool)
	cp.available = config.PoolCapacity
	cp.config = config
	cp.closed = false
	cp.emptyCond = sync.NewCond(cp)
	return cp
}

func (cp *connPool) push(c *Conn) {
	cp.Lock()
	defer cp.Unlock()
	if cp.closed {
		return
	}

	if !c.Closed() {
		cp.free.PushFront(c)
	}

	cp.available++
	cp.emptyCond.Signal()
}

func (cp *connPool) pull() (c *Conn, err *Error) {
	cp.Lock()
	defer cp.Unlock()
	if cp.closed {
		return nil, newError("connection pool is closed", ErrorConnection)
	}

	for cp.available == 0 {
		cp.emptyCond.Wait()
	}

	if cp.free.Len() > 0 {
		c, _ = cp.free.Remove(cp.free.Back()).(*Conn)
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
	cp.Lock()
	defer cp.Unlock()
	for e := cp.free.Front(); e != nil; e = e.Next() {
		c, _ := e.Value.(*Conn)
		c.Close()
	}

	cp.free.Init()
	cp.closed = true
}
