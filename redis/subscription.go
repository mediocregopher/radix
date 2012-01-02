package redis

import "runtime"

//* Subscription value

// The subscription value is a result value
// plus channel pattern and channel.
type SubscriptionValue struct {
	Value
	ChannelPattern string
	Channel        string
}

// Create a new subscription value.
func newSubscriptionValue(data [][]byte) *SubscriptionValue {
	switch len(data) {
	case 3:
		return &SubscriptionValue{
			Value:          Value(data[2]),
			ChannelPattern: "*",
			Channel:        string(data[1]),
		}
	case 4:
		return &SubscriptionValue{
			Value:          Value(data[3]),
			ChannelPattern: string(data[1]),
			Channel:        string(data[2]),
		}
	}

	return nil
}

//* Subscription

// Subscription is a structure for holding multiple Redis subscriptions for multiple channels.
type Subscription struct {
	urp                   *unifiedRequestProtocol
	error                 error
	SubscriptionValueChan chan *SubscriptionValue
	closerChan            chan bool
}

// Create a new subscription.
func newSubscription(urp *unifiedRequestProtocol, channels ...string) *Subscription {
	sub := &Subscription{
		urp: urp,
		SubscriptionValueChan: make(chan *SubscriptionValue, 10),
		closerChan:            make(chan bool, 1)}

	runtime.SetFinalizer(sub, (*Subscription).Stop)
	// Subscribe to the channels.
	chanCount := sub.urp.subscribe(channels...)
	go sub.backend()
	return sub, chanCount
}

// Subscribe to channels and return the count of subscribed channels.
func (s *Subscription) Subscribe(channels ...string) int {
	return s.urp.subscribe(channels...)
}

// Unsubscribe from channels and return the count of remaining subscribed channels.
func (s *Subscription) Unsubscribe(channels ...string) int {
	return s.urp.unsubscribe(channels...)
}

// Close the subscription.
func (s *Subscription) Stop() {
	runtime.SetFinalizer(s, nil)
	s.urp.stop()
	s.closerChan <- true
}

// Backend of the subscription.
func (s *Subscription) backend() {
	for epd := range s.urp.publishedDataChan {
		// Received a published data, republish
		// as subscription value.
		sv := newSubscriptionValue(epd.data)

		// Send the subscription value.
		select {
		case <-s.closerChan:
			// Close the backend.
			close(s.SubscriptionValueChan)
			return
		case s.SubscriptionValueChan <- sv:
			// OK.
		default:
			// Not sent!
			return
		}
	}
}
