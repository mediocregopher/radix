package radix

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/mediocregopher/radix/v3/resp/resp2"
)

// PubSubMessage describes a message being published to a subscribed channel
type PubSubMessage struct {
	Type    string // "message" or "pmessage"
	Pattern string // will be set if Type is "pmessage"
	Channel string
	Message []byte
}

// MarshalRESP implements the Marshaler interface. It will assume the
// PubSubMessage is a PMESSAGE if Pattern is non-empty.
func (m PubSubMessage) MarshalRESP(w io.Writer) error {
	var err error
	marshal := func(m resp.Marshaler) {
		if err == nil {
			err = m.MarshalRESP(w)
		}
	}

	if m.Type == "message" {
		marshal(resp2.ArrayHeader{N: 3})
		marshal(resp2.BulkString{S: m.Type})
	} else if m.Type == "pmessage" {
		marshal(resp2.ArrayHeader{N: 4})
		marshal(resp2.BulkString{S: m.Type})
		marshal(resp2.BulkString{S: m.Pattern})
	} else {
		return errors.New("unknown message Type")
	}
	marshal(resp2.BulkString{S: m.Channel})
	marshal(resp2.BulkStringBytes{B: m.Message})
	return err
}

// UnmarshalRESP implements the Unmarshaler interface
func (m *PubSubMessage) UnmarshalRESP(br *bufio.Reader) error {
	bb := make([][]byte, 0, 4)
	if err := (resp2.Any{I: &bb}).UnmarshalRESP(br); err != nil {
		return err
	}

	if len(bb) < 3 {
		return errors.New("message has too few elements")
	}

	m.Type = string(bytes.ToLower(bb[0]))
	isPat := m.Type == "pmessage"
	if isPat && len(bb) < 4 {
		return errors.New("message has too few elements")
	} else if !isPat && m.Type != "message" {
		return fmt.Errorf("not message or pmessage: %q", m.Type)
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

////////////////////////////////////////////////////////////////////////////////

type chanSet map[string]map[chan<- PubSubMessage]bool

func (cs chanSet) add(s string, ch chan<- PubSubMessage) {
	m, ok := cs[s]
	if !ok {
		m = map[chan<- PubSubMessage]bool{}
		cs[s] = m
	}
	m[ch] = true
}

func (cs chanSet) del(s string, ch chan<- PubSubMessage) bool {
	m, ok := cs[s]
	if !ok {
		return true
	}
	delete(m, ch)
	if len(m) == 0 {
		delete(cs, s)
		return true
	}
	return false
}

func (cs chanSet) missing(ss []string) []string {
	out := ss[:0]
	for _, s := range ss {
		if _, ok := cs[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}

func (cs chanSet) inverse() map[chan<- PubSubMessage][]string {
	inv := map[chan<- PubSubMessage][]string{}
	for s, m := range cs {
		for ch := range m {
			inv[ch] = append(inv[ch], s)
		}
	}
	return inv
}

////////////////////////////////////////////////////////////////////////////////

// PubSubConn wraps an existing Conn to support redis' pubsub system.
// User-created channels can be subscribed to redis channels to receive
// PubSubMessages which have been published.
//
// If any methods return an error it means the PubSubConn has been Close'd and
// subscribed msgCh's will no longer receive PubSubMessages from it. All methods
// are threadsafe and non-blocking.
//
// NOTE if any channels block when being written to they will block all other
// channels from receiving a publish.
type PubSubConn interface {
	// Subscribe subscribes the PubSubConn to the given set of channels. msgCh
	// will receieve a PubSubMessage for every publish written to any of the
	// channels. This may be called multiple times for the same channels and
	// different msgCh's, each msgCh will receieve a copy of the PubSubMessage
	// for each publish.
	Subscribe(msgCh chan<- PubSubMessage, channels ...string) error

	// Unsubscribe unsubscribes the msgCh from the given set of channels, if it
	// was subscribed at all.
	//
	// NOTE even if msgCh is not subscribed to any other redis channels, it
	// should still be considered "active", and therefore still be having
	// messages read from it, until Unsubscribe has returned
	Unsubscribe(msgCh chan<- PubSubMessage, channels ...string) error

	// PSubscribe is like Subscribe, but it subscribes msgCh to a set of
	// patterns and not individual channels.
	PSubscribe(msgCh chan<- PubSubMessage, patterns ...string) error

	// PUnsubscribe is like Unsubscribe, but it unsubscribes msgCh from a set of
	// patterns and not individual channels.
	//
	// NOTE even if msgCh is not subscribed to any other redis channels, it
	// should still be considered "active", and therefore still be having
	// messages read from it, until PUnsubscribe has returned
	PUnsubscribe(msgCh chan<- PubSubMessage, patterns ...string) error

	// Ping performs a simple Ping command on the PubSubConn, returning an error
	// if it failed for some reason
	Ping() error

	// Close closes the PubSubConn so it can't be used anymore. All subscribed
	// channels will stop receiving PubSubMessages from this Conn (but will not
	// themselves be closed).
	//
	// NOTE all msgChs should be considered "active", and therefore still be
	// having messages read from them, until Close has returned.
	Close() error
}

type pubSubConn struct {
	conn Conn

	csL   sync.RWMutex
	subs  chanSet
	psubs chanSet

	// These are used for writing commands and waiting for their response (e.g.
	// SUBSCRIBE, PING). See the do method for how that works.
	cmdL     sync.Mutex
	cmdResCh chan error

	close    sync.Once
	closeErr error

	// This one is optional, and kind of cheating. We use it in persistent to
	// get on-the-fly updates of when the connection fails. Maybe one day this
	// could be exposed if there's a clean way of doing so, or another way
	// accomplishing the same thing could be done instead.
	closeErrCh chan error

	// only used during testing
	testEventCh chan string
}

// PubSub wraps the given Conn so that it becomes a PubSubConn. The passed in
// Conn should not be used after this call.
func PubSub(rc Conn) PubSubConn {
	return newPubSub(rc, nil)
}

func newPubSub(rc Conn, closeErrCh chan error) PubSubConn {
	c := &pubSubConn{
		conn:       rc,
		subs:       chanSet{},
		psubs:      chanSet{},
		cmdResCh:   make(chan error, 1),
		closeErrCh: closeErrCh,
	}
	go c.spin()

	// Periodically call Ping so the connection has a keepalive on the
	// application level. If the Conn is closed Ping will return an error and
	// this will clean itself up.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			if err := c.Ping(); err != nil {
				return
			}
		}
	}()

	return c
}

func (c *pubSubConn) testEvent(str string) {
	if c.testEventCh != nil {
		c.testEventCh <- str
	}
}

func (c *pubSubConn) publish(m PubSubMessage) {
	c.csL.RLock()
	defer c.csL.RUnlock()

	var subs map[chan<- PubSubMessage]bool
	if m.Type == "pmessage" {
		subs = c.psubs[m.Pattern]
	} else {
		subs = c.subs[m.Channel]
	}

	for ch := range subs {
		ch <- m
	}
}

func (c *pubSubConn) spin() {
	for {
		var rm resp2.RawMessage
		err := c.conn.Decode(&rm)
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			c.testEvent("timeout")
			continue
		} else if err != nil {
			c.closeInner(err)
			return
		}

		var m PubSubMessage
		if err := rm.UnmarshalInto(&m); err == nil {
			c.publish(m)
		} else {
			c.cmdResCh <- nil
		}
	}
}

// NOTE cmdL _must_ be held to use do
func (c *pubSubConn) do(exp int, cmd string, args ...string) error {
	rcmd := Cmd(nil, cmd, args...)
	if err := c.conn.Encode(rcmd); err != nil {
		return err
	}

	for i := 0; i < exp; i++ {
		err, ok := <-c.cmdResCh
		if err != nil {
			return err
		} else if !ok {
			return errors.New("connection closed")
		}
	}
	return nil
}

func (c *pubSubConn) closeInner(cmdResErr error) error {
	c.close.Do(func() {
		c.csL.Lock()
		defer c.csL.Unlock()
		c.closeErr = c.conn.Close()
		c.subs = nil
		c.psubs = nil

		if cmdResErr != nil {
			select {
			case c.cmdResCh <- cmdResErr:
			default:
			}
		}
		if c.closeErrCh != nil {
			c.closeErrCh <- cmdResErr
			close(c.closeErrCh)
		}
		close(c.cmdResCh)
	})
	return c.closeErr
}

func (c *pubSubConn) Close() error {
	return c.closeInner(nil)
}

func (c *pubSubConn) Subscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	c.cmdL.Lock()
	defer c.cmdL.Unlock()

	c.csL.RLock()
	missing := c.subs.missing(channels)
	c.csL.RUnlock()

	if len(missing) > 0 {
		if err := c.do(len(missing), "SUBSCRIBE", missing...); err != nil {
			return err
		}
	}

	c.csL.Lock()
	for _, channel := range channels {
		c.subs.add(channel, msgCh)
	}
	c.csL.Unlock()

	return nil
}

func (c *pubSubConn) Unsubscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	c.cmdL.Lock()
	defer c.cmdL.Unlock()

	c.csL.Lock()
	emptyChannels := make([]string, 0, len(channels))
	for _, channel := range channels {
		if empty := c.subs.del(channel, msgCh); empty {
			emptyChannels = append(emptyChannels, channel)
		}
	}
	c.csL.Unlock()

	if len(emptyChannels) == 0 {
		return nil
	}

	return c.do(len(emptyChannels), "UNSUBSCRIBE", emptyChannels...)
}

func (c *pubSubConn) PSubscribe(msgCh chan<- PubSubMessage, patterns ...string) error {
	c.cmdL.Lock()
	defer c.cmdL.Unlock()

	c.csL.RLock()
	missing := c.psubs.missing(patterns)
	c.csL.RUnlock()

	if len(missing) > 0 {
		if err := c.do(len(missing), "PSUBSCRIBE", missing...); err != nil {
			return err
		}
	}

	c.csL.Lock()
	for _, pattern := range patterns {
		c.psubs.add(pattern, msgCh)
	}
	c.csL.Unlock()

	return nil
}

func (c *pubSubConn) PUnsubscribe(msgCh chan<- PubSubMessage, patterns ...string) error {
	c.cmdL.Lock()
	defer c.cmdL.Unlock()

	c.csL.Lock()
	emptyPatterns := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if empty := c.psubs.del(pattern, msgCh); empty {
			emptyPatterns = append(emptyPatterns, pattern)
		}
	}
	c.csL.Unlock()

	if len(emptyPatterns) == 0 {
		return nil
	}

	return c.do(len(emptyPatterns), "PUNSUBSCRIBE", emptyPatterns...)
}

func (c *pubSubConn) Ping() error {
	c.cmdL.Lock()
	defer c.cmdL.Unlock()

	return c.do(1, "PING")
}
