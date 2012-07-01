package redis

import (
	"log"
)

type subType uint8

const (
	subSubscribe subType = iota
	subUnsubscribe
	subPsubscribe
	subPunsubscribe
)

// Subscription is a structure for holding a Redis subscription for multiple channels.
type Subscription struct {
	c       *conn
	msgHdlr func(msg *Message)
}

// newSubscription returns a new Subscription or an error.
func newSubscription(config *Config, msgHdlr func(msg *Message)) (*Subscription, *Error) {
	var err *Error

	s := &Subscription{
		msgHdlr: msgHdlr,
	}

	// Connection handling
	s.c, err = newConn(config)
	if err != nil {
		return nil, err
	}

	s.c.noReadTimeout = true // disable read timeout during pubsub mode
	go s.listener()
	return s, nil
}

// Subscribe subscribes to given channels or returns an error.
func (s *Subscription) Subscribe(channels ...string) (err *Error) {
	return s.c.subscription(subSubscribe, channels)
}

// Unsubscribe unsubscribes from given channels or returns an error.
func (s *Subscription) Unsubscribe(channels ...string) (err *Error) {
	return s.c.subscription(subUnsubscribe, channels)
}

// Psubscribe subscribes to given patterns or returns an error.
func (s *Subscription) Psubscribe(patterns ...string) (err *Error) {
	return s.c.subscription(subPsubscribe, patterns)
}

// Punsubscribe unsubscribes from given patterns or returns an error.
func (s *Subscription) Punsubscribe(patterns ...string) (err *Error) {
	return s.c.subscription(subPunsubscribe, patterns)
}

// Close closes the subscription.
func (s *Subscription) Close() {
	// just sack the connection, listener will close down eventually.
	s.c.close()
}

// readMessage reads and parses pubsub message data from the connection and returns it as a message.
func (s *Subscription) readMessage() *Message {
	var err error
	var rs string
	m := new(Message)
	r := s.c.read()

	if r.Type != ReplyMulti || len(r.Elems) < 3 {
		goto Err
	}

	// first argument is the message type
	if rs, err = r.Elems[0].Str(); err != nil {
		goto Err
	}

	switch rs {
	case "subscribe":
		m.Type = MessageSubscribe
	case "unsubscribe":
		m.Type = MessageUnsubscribe
	case "psubscribe":
		m.Type = MessagePsubscribe
	case "punsubscribe":
		m.Type = MessagePunsubscribe
	case "message":
		m.Type = MessageMessage
	case "pmessage":
		m.Type = MessagePmessage
	default:
		goto Err
	}

	// second argument
	if rs, err = r.Elems[1].Str(); err != nil {
		goto Err
	}

	switch {
	case m.Type == MessageSubscribe || m.Type == MessageUnsubscribe:
		m.Channel = rs

		// number of subscriptions
		if m.Subscriptions, err = r.Elems[2].Int(); err != nil {
			goto Err
		}
	case m.Type == MessagePsubscribe || m.Type == MessagePunsubscribe:
		m.Pattern = rs

		// number of subscriptions
		if m.Subscriptions, err = r.Elems[2].Int(); err != nil {
			goto Err
		}
	case m.Type == MessageMessage:
		m.Channel = rs

		// payload
		if m.Payload, err = r.Elems[2].Str(); err != nil {
			goto Err
		}
	case m.Type == MessagePmessage:
		m.Pattern = rs

		// name of the originating channel
		if m.Channel, err = r.Elems[2].Str(); err != nil {
			goto Err
		}

		if len(r.Elems) < 4 {
			goto Err
		}

		// payload
		if m.Payload, err = r.Elems[3].Str(); err != nil {
			goto Err
		}
	default:
		goto Err
	}

	return m

Err:
	// Error/Invalid message reply
	// we shouldn't generally get these, except when closing.
	if r.Err != nil && !r.Err.Test(ErrorConnection) {
		log.Printf("received an unexpected error reply while in pubsub mode: %s.\n ignoring...",
			r.Err)
	}

	return nil
}

// listener is a goroutine for reading and handling pubsub messages.
func (s *Subscription) listener() {
	var m *Message

	// read until connection is closed
	for {
		m = s.readMessage()
		if m == nil && s.c.closed() {
			return
		}

		go s.msgHdlr(m)
	}
}
