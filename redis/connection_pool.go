package redis

import (
	"sync"
)

// connectionPool is a stack-like structure that holds the connections of a Client.
type connectionPool struct {
	pool []*connection
	lock *sync.Mutex
	cond *sync.Cond
}

func newConnectionPool() *connectionPool {
	locker := &sync.Mutex{}
	cp := &connectionPool{
		pool: []*connection{},
		lock: locker,
		cond: sync.NewCond(locker),
	}

	return cp
}

func (cp *connectionPool) push(conn *connection) {
	cp.lock.Lock()
	cp.pool = append(cp.pool, conn)
	cp.cond.Signal()
	cp.lock.Unlock()
}

func (cp *connectionPool) pull() (conn *connection) {
	cp.lock.Lock()
	for len(cp.pool) == 0 {
		cp.cond.Wait()
	}

	conn = cp.pool[0]
	cp.pool = cp.pool[1:]
	cp.lock.Unlock()

	return conn
}
