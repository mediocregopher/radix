package redis

import "runtime"

//* Subscription

// Subscription is a structure for holding a Redis subscription for multiple channels.
type Subscription struct {
	client      *Client
	urp         *unifiedRequestProtocol
	error       error
	MessageChan chan *Message
}

// Create a new Subscription or return an error.
func newSubscription(urp *unifiedRequestProtocol, channels ...string) (*Subscription, error) {
	sub := &Subscription{
		urp:         urp,
		MessageChan: urp.pubChan,
	}

	runtime.SetFinalizer(sub, (*Subscription).Close)
	err := sub.urp.subscribe(channels...)
	return sub, err
}

// Subscribe to given channels and return an error, if any.
func (s *Subscription) Subscribe(channels ...string) error {
	return s.urp.subscribe(channels...)
}

// Unsubscribe from given channels and return an error, if any.
func (s *Subscription) Unsubscribe(channels ...string) error {
	return s.urp.unsubscribe(channels...)
}

// Close the Subscription.
func (s *Subscription) Close() {
	runtime.SetFinalizer(s, nil)
	s.urp.close()
	s.urp = nil
}
