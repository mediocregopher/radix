package redis

import "runtime"

//* Subscription

// Subscription is a structure for holding a Redis subscription for multiple channels.
type Subscription struct {
	client      *Client
	conn        *connection
	error       error
	closerChan  chan bool
	messageChan chan *Message
	MessageChan <-chan *Message
}

// Create a new Subscription or return an error.
func newSubscription(conn *connection, channels ...string) (*Subscription, error) {
	messageChan := make(chan *Message, 1)
	sub := &Subscription{
		conn:        conn,
		closerChan:  make(chan bool, 1),
		messageChan: messageChan,
		MessageChan: messageChan,
	}
	runtime.SetFinalizer(sub, (*Subscription).Close)
	go sub.backend()

	err := sub.conn.subscribe(channels...)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

// Subscribe to given channels and return an error, if any.
func (s *Subscription) Subscribe(channels ...string) error {
	return s.conn.subscribe(channels...)
}

// Unsubscribe from given channels and return an error, if any.
func (s *Subscription) Unsubscribe(channels ...string) error {
	return s.conn.unsubscribe(channels...)
}

// Close the Subscription.
func (s *Subscription) Close() {
	runtime.SetFinalizer(s, nil)
	s.closerChan <- true
	// Try to unsubscribe from all channels to reset the connection state back to normal
	err := s.conn.unsubscribe()
	if err != nil {
		s.conn.close()
		s.conn = nil
	}
}

// Backend of the subscription.
func (s *Subscription) backend() {
	for {
		select {
		case <-s.closerChan:
			// Close the backend.
			close(s.messageChan)
			return
		case s.messageChan <- <-s.conn.messageChan:
			// Message forwarding succesful
		}
	}
}
