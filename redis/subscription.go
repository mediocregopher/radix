package redis

import (
	"sync"
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
	conn        *connection
	pool        *connPool
	msgHdlr func(msg *Message)
	listening bool
	lock sync.Mutex // lock for handling listening
}

// newSubscription returns a new Subscription or an error.
func newSubscription(pool *connPool, msgHdlr func(msg *Message)) (*Subscription, *Error) {
	var err *Error

	s := &Subscription{
		pool: pool,
		msgHdlr: msgHdlr,
	}

	// Connection handling
	s.conn, err = s.pool.pull()
	if err != nil {
		return nil, err
	}

	s.conn.noRTimeout = true // disable read timeout during pubsub mode
	return s, nil
}

// listen starts the listener goroutine, if it's not running already.
func (s *Subscription) listen() {
	s.lock.Lock()
	if !s.listening {
		s.listening = true
		go s.listener()
	}
}

// Subscribe subscribes to given channels or returns an error.
func (s *Subscription) Subscribe(channels ...string) (err *Error) {
	s.listen()
	err = s.conn.subscription(subSubscribe, channels)
	s.lock.Unlock()
	return err
}

// Unsubscribe unsubscribes from given channels or returns an error.
func (s *Subscription) Unsubscribe(channels ...string) (err *Error) {
	s.listen()
	err = s.conn.subscription(subUnsubscribe, channels)
	s.lock.Unlock()
	return err
}

// Psubscribe subscribes to given patterns or returns an error.
func (s *Subscription) Psubscribe(patterns ...string) (err *Error) {
	s.listen()
	err = s.conn.subscription(subPsubscribe, patterns)
	s.lock.Unlock()
	return err
}

// Punsubscribe unsubscribes from given patterns or returns an error.
func (s *Subscription) Punsubscribe(patterns ...string) (err *Error) {
	s.listen()
	err = s.conn.subscription(subPunsubscribe, patterns)
	s.lock.Unlock()
	return err
}

// Close closes the subscription.
// Subscription's connection will be returned to the connection pool, 
// if there are no active subscriptions in it.
// Otherwise, the connection is sacked.
func (s *Subscription) Close() {
	s.lock.Lock()
	listening := s.listening
	s.lock.Unlock()

	if listening {
		s.conn.close()
		s.pool.push(nil)
	} else {
		s.conn.noRTimeout = true
		s.pool.push(s.conn)
	}
}

// parseResponse parses the given pubsub message data and returns it as a Message.
func (s *Subscription) parseResponse(rd *readData) *Message {
	r := s.conn.receiveReply(rd)

	if r.Type == ReplyError {
		// Error reply
		// NOTE: Redis SHOULD NOT send error replies while the connection is in subscribed mode.
		// These errors must always originate from radix itself.
		return &Message{Type: MessageError, Error: r.Error}
	}

	var r0, r1 *Reply
	m := &Message{}

	if r.Type != ReplyMulti || r.Len() < 3 {
		goto Invalid
	}

	r0 = r.At(0)
	if r0.Type != ReplyString {
		goto Invalid
	}

	// first argument is the message type
	switch r0.Str() {
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
		goto Invalid
	}

	// second argument
	r1 = r.At(1)
	if r1.Type != ReplyString {
		goto Invalid
	}

	switch {
	case m.Type == MessageSubscribe || m.Type == MessageUnsubscribe:
		m.Channel = r1.Str()

		// number of subscriptions
		r2 := r.At(2)
		if r2.Type != ReplyInteger {
			goto Invalid
		}

		m.Subscriptions = r2.Int()
	case m.Type == MessagePsubscribe || m.Type == MessagePunsubscribe:
		m.Pattern = r1.Str()

		// number of subscriptions
		r2 := r.At(2)
		if r2.Type != ReplyInteger {
			goto Invalid
		}

		m.Subscriptions = r2.Int()
	case m.Type == MessageMessage:
		m.Channel = r1.Str()

		// payload
		r2 := r.At(2)
		if r2.Type != ReplyString {
			goto Invalid
		}

		m.Payload = r2.Str()
	case m.Type == MessagePmessage:
		m.Pattern = r1.Str()

		// name of the originating channel
		r2 := r.At(2)
		if r2.Type != ReplyString {
			goto Invalid
		}

		m.Channel = r2.Str()

		// payload
		r3 := r.At(3)
		if r3.Type != ReplyString {
			goto Invalid
		}

		m.Payload = r3.Str()
	default:
		goto Invalid
	}

	return m

Invalid:
	// Invalid reply
	return &Message{Type: MessageError, Error: newError("received invalid pubsub reply",
			ErrorInvalidReply)}
}

// listener is a goroutine for reading and handling pubsub messages.
func (s *Subscription) listener() {
	var m *Message

	// read until connection is closed or
	// when subscription count reaches zero
	for {
		rd := s.conn.read()
		s.lock.Lock()
		if rd.error != nil && rd.error.Test(ErrorConnection) {
			// connection closed
			return
		}

		m = s.parseResponse(rd)
		if (m.Type == MessageSubscribe ||
			m.Type == MessageUnsubscribe ||
			m.Type == MessagePsubscribe ||
			m.Type == MessagePunsubscribe) &&
			m.Subscriptions == 0 {
			s.listening = false
			s.lock.Unlock()
			return
		}

		s.lock.Unlock()
		s.msgHdlr(m)
	}
}