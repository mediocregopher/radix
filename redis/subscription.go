package redis

import (
	"log"
	"sync/atomic"
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
	conn    *connection
	msgHdlr func(msg *Message)
}

// newSubscription returns a new Subscription or an error.
func newSubscription(config *Configuration, msgHdlr func(msg *Message)) (*Subscription, *Error) {
	var err *Error

	s := &Subscription{
		msgHdlr: msgHdlr,
	}

	// Connection handling
	s.conn, err = newConnection(config)
	if err != nil {
		return nil, err
	}

	s.conn.noReadTimeout = true // disable read timeout during pubsub mode
	go s.listener()
	return s, nil
}

// Subscribe subscribes to given channels or returns an error.
func (s *Subscription) Subscribe(channels ...string) (err *Error) {
	return s.conn.subscription(subSubscribe, channels)
}

// Unsubscribe unsubscribes from given channels or returns an error.
func (s *Subscription) Unsubscribe(channels ...string) (err *Error) {
	return s.conn.subscription(subUnsubscribe, channels)
}

// Psubscribe subscribes to given patterns or returns an error.
func (s *Subscription) Psubscribe(patterns ...string) (err *Error) {
	return s.conn.subscription(subPsubscribe, patterns)
}

// Punsubscribe unsubscribes from given patterns or returns an error.
func (s *Subscription) Punsubscribe(patterns ...string) (err *Error) {
	return s.conn.subscription(subPunsubscribe, patterns)
}

// Close closes the subscription.
func (s *Subscription) Close() {
	// just sack the connection, listener will close down eventually.
	s.conn.close()
}

// readMessage reads and parses pubsub message data from the connection and returns it as a message.
func (s *Subscription) readMessage() *Message {
	var err error
	var r, mr *Reply
	var rs string
	m := new(Message)
	mr = s.conn.read()

	if mr.Error != nil {
		goto Errmsg
	}

	// first argument is the message type
	if r, err = mr.At(0); err != nil {
		goto Errmsg
	}

	if rs, err = r.Str(); err != nil {
		goto Errmsg
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
		goto Errmsg
	}

	// second argument
	if r, err = mr.At(1); err != nil {
		goto Errmsg
	}
	if rs, err = r.Str(); err != nil {
		goto Errmsg
	}

	switch {
	case m.Type == MessageSubscribe || m.Type == MessageUnsubscribe:
		m.Channel = rs

		// number of subscriptions
		if r, err = mr.At(2); err != nil {
			goto Errmsg
		}
		if m.Subscriptions, err = r.Int(); err != nil {
			goto Errmsg
		}
	case m.Type == MessagePsubscribe || m.Type == MessagePunsubscribe:
		m.Pattern = rs

		// number of subscriptions
		if r, err = mr.At(2); err != nil {
			goto Errmsg
		}
		if m.Subscriptions, err = r.Int(); err != nil {
			goto Errmsg
		}
	case m.Type == MessageMessage:
		m.Channel = rs

		// payload
		if r, err = mr.At(2); err != nil {
			goto Errmsg
		}
		if m.Payload, err = r.Str(); err != nil {
			goto Errmsg
		}
	case m.Type == MessagePmessage:
		m.Pattern = rs

		// name of the originating channel
		if r, err = mr.At(2); err != nil {
			goto Errmsg
		}
		if m.Channel, err = r.Str(); err != nil {
			goto Errmsg
		}

		// payload
		if r, err = mr.At(3); err != nil {
			goto Errmsg
		}
		if m.Payload, err = r.Str(); err != nil {
			goto Errmsg
		}
	default:
		goto Errmsg
	}

	return m

Errmsg:
	// Error/Invalid message reply
	// we shouldn't generally get these, except when closing.
	if r.Error != nil && !r.Error.Test(ErrorConnection) {
		log.Printf("received an unexpected error reply while in pubsub mode: %s.\n ignoring...",
			r.Error)
	}

	return nil
}

// listener is a goroutine for reading and handling pubsub messages.
func (s *Subscription) listener() {
	var m *Message

	// read until connection is closed
	for {
		m = s.readMessage()
		if m == nil && atomic.LoadInt32(&s.conn.closed) == 1 {
			return
		}

		go s.msgHdlr(m)
	}
}
