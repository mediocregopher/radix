package redis

import "runtime"

//* Subscription

// Subscription is a structure for holding a Redis subscription for multiple channels.
type Subscription struct {
	client      *Client
	conn        *connection
	closerChan  chan bool
	messageChan chan *Message
	msgHdlr     func(msg *Message)
}

// Create a new Subscription or return an error.
func newSubscription(client *Client, msgHdlr func(msg *Message)) (*Subscription, *Error) {
	var err *Error

	sub := &Subscription{
		client:      client,
		closerChan:  make(chan bool),
		messageChan: make(chan *Message),
		msgHdlr:     msgHdlr,
	}

	// Connection handling
	sub.conn, err = sub.client.pullConnection()

	if err != nil {
		sub.client.pushConnection(sub.conn)
		return nil, err
	}

	runtime.SetFinalizer(sub, (*Subscription).Close)
	go sub.backend()

	return sub, nil
}

// Subscribe to given channels or return an error.
func (s *Subscription) Subscribe(channels ...string) *Error {
	return s.conn.subscribe(channels...)
}

// Unsubscribe from given channels or return an error.
func (s *Subscription) Unsubscribe(channels ...string) *Error {
	return s.conn.unsubscribe(channels...)
}

// Subscribe to given patterns or return an error.
func (s *Subscription) PSubscribe(patterns ...string) *Error {
	return s.conn.psubscribe(patterns...)
}

// Unsubscribe from given patterns or return an error.
func (s *Subscription) PUnsubscribe(patterns ...string) *Error {
	return s.conn.punsubscribe(patterns...)
}

// Close the Subscription and return its connection to the connection pool.
func (s *Subscription) Close() {
	runtime.SetFinalizer(s, nil)
	s.closerChan <- true
	// Try to unsubscribe from all channels to reset the connection state back to normal
	err := s.conn.unsubscribe()
	if err != nil {
		s.conn.close()
		s.conn = nil
	}

	s.client.pushConnection(s.conn)
}

// Backend of the subscription.
func (s *Subscription) backend() {
	for {
		select {
		case <-s.closerChan:
			return
		case msg := <-s.conn.messageChan:
			s.msgHdlr(msg)
		}
	}
}
