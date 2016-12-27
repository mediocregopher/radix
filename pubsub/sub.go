// Package pubsub provides a wrapper around a normal redis client which makes
// interacting with publish/subscribe commands much easier
package pubsub

import (
	"bufio"
	"bytes"
	"errors"
	"net"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/mediocregopher/radix.v2/resp"
)

// Message describes a message being published to a subscribed channel
type Message struct {
	Pattern string // will be set if PSUBSCRIBE was used
	Channel string
	Message []byte
}

// UnmarshalRESP implements the radix.Unmarshaler interface
func (m *Message) UnmarshalRESP(p *resp.Pool, br *bufio.Reader) error {
	bb := make([][]byte, 0, 4)
	if err := (resp.Any{I: &bb}).UnmarshalRESP(p, br); err != nil {
		return err
	}

	if len(bb) < 3 {
		return errors.New("message has too few elements")
	}

	typ := bytes.ToUpper(bb[0])
	isPat := bytes.Equal(typ, []byte("PMESSAGE"))
	if isPat && len(bb) < 4 {
		return errors.New("message has too few elements")
	} else if !bytes.Equal(typ, []byte("MESSAGE")) {
		return errors.New("not MESSAGE or PMESSAGE")
	}

	pop := func() []byte {
		b := bb[0]
		bb = bb[1:]
		return b
	}

	pop() // discard (p)message
	if isPat {
		m.Pattern = string(pop())
	}
	m.Channel = string(pop())
	m.Message = pop()
	return nil
}

type maybeMessage struct {
	ok bool
	Message
}

func (mm *maybeMessage) UnmarshalRESP(p *resp.Pool, br *bufio.Reader) error {
	var rm resp.RawMessage
	if err := rm.UnmarshalRESP(p, br); err != nil {
		return err
	}

	err := rm.UnmarshalInto(p, &mm.Message)
	mm.ok = err == nil
	return nil
}

// SubConn wraps a radix.Conn in order to provide a channel to which messages
// from subscribed channels will be written.
type SubConn struct {
	c         radix.Conn
	lastErr   error
	cmdDoneCh chan chan bool
	closeCh   chan bool

	// Ch is the channel to which all publish messages for subscribed channels
	// will be written. It should be being read from at all times in a separate
	// go-routine from the one making subscribe/unsubscribe calls on the Client.
	//
	// This channel will be closed if the Close method is called or an error is
	// encountered. The Err method can be used to retrieve the last error.
	Ch <-chan Message
}

// New returns an initialized SubConn. Check the docs on the Ch field for how to
// read publishes.
func New(c radix.Conn) *SubConn {
	ch := make(chan Message)
	sc := &SubConn{
		c:         TimeoutOk(c),
		cmdDoneCh: make(chan chan bool, 1),
		closeCh:   make(chan bool),
		Ch:        ch,
	}
	go sc.readSpin(ch)
	return sc
}

func (sc *SubConn) readSpin(ch chan Message) {
	defer close(ch)
	defer close(sc.closeCh)
	defer sc.c.Close()
	for {
		select {
		case <-sc.closeCh:
			return
		default:
		}

		var mm maybeMessage
		err := sc.c.Decode(&mm)
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			continue
		} else if err != nil {
			sc.lastErr = err
			return
		} else if !mm.ok {
			(<-sc.cmdDoneCh) <- true
			continue
		}

		ch <- mm.Message
	}
}

// Err returns the error which caused the SubConn to close, if any. This should
// only be called after Ch has been closed.
func (sc *SubConn) Err() error {
	return sc.lastErr
}

// Close will clean up the resources taken by this SubConn and then call Close
// on the underlying connection
func (sc *SubConn) Close() error {
	sc.closeCh <- true
	return nil
}

// Subscribe runs a Redis "SUBSCRIBE" command with the provided channels
func (sc *SubConn) Subscribe(channels ...string) {
	sc.doCmd("SUBSCRIBE", channels...)
}

// PSubscribe runs a Redis "PSUBSCRIBE" command with the provided patterns
func (sc *SubConn) PSubscribe(patterns ...string) {
	sc.doCmd("PSUBSCRIBE", patterns...)
}

// Unsubscribe runs a Redis "UNSSUBSCRIBE" command with the provided channels
func (sc *SubConn) Unsubscribe(channels ...string) {
	sc.doCmd("UNSUBSCRIBE", channels...)
}

// PUnsubscribe runs a Redis "PUNSSUBSCRIBE" command with the provided patterns
func (sc *SubConn) PUnsubscribe(patterns ...string) {
	sc.doCmd("PUNSUBSCRIBE", patterns...)
}

// Ping writes a PING message to the connection, which can be used to unsure the
// connection is still alive.
func (sc *SubConn) Ping() {
	sc.doCmd("PING")
}

func (sc *SubConn) doCmd(cmd string, args ...string) {
	doneCh := make(chan bool)
	sc.cmdDoneCh <- doneCh
	sc.c.Encode(radix.CmdNoKey(cmd, args))
	<-doneCh
}
