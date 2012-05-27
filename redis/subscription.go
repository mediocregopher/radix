package redis

//* Subscription

// Subscription is a structure for holding a Redis subscription for multiple channels.
type Subscription struct {
	conn        *connection
	pool        *connPool
}

// newSubscription returns a new Subscription or an error.
func newSubscription(pool *connPool, msgHdlr func(msg *Message)) (*Subscription, *Error) {
	var err *Error

	sub := &Subscription{
		pool: pool,
	}

	// Connection handling
	sub.conn, err = sub.pool.pull()

	if err != nil {
		return nil, err
	}

	sub.conn.setMsgHdlr(msgHdlr)
	return sub, nil
}

// Subscribe subscribes to given channels or returns an error.
func (s *Subscription) Subscribe(channels ...string) *Error {
	return s.conn.subscribe(channels...)
}

// Unsubscribe unsubscribes from given channels or returns an error.
func (s *Subscription) Unsubscribe(channels ...string) *Error {
	return s.conn.unsubscribe(channels...)
}

// Psubscribe subscribes to given patterns or returns an error.
func (s *Subscription) Psubscribe(patterns ...string) *Error {
	return s.conn.psubscribe(patterns...)
}

// Punsubscribe unsubscribes from given patterns or returns an error.
func (s *Subscription) Punsubscribe(patterns ...string) *Error {
	return s.conn.punsubscribe(patterns...)
}

// Close closes the Subscription and returns its connection to the connection pool.
func (s *Subscription) Close() {
	s.conn.setMsgHdlr(nil)
	// Try to unsubscribe from all channels to reset the connection state back to normal
	err := s.conn.unsubscribe()
	if err != nil {
		s.conn.close()
		s.conn = nil
	} else {
		s.pool.push(s.conn)
	}
}
