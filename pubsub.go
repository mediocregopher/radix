package radix

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/mediocregopher/radix.v2/resp"
)

type timeoutOkConn struct {
	Conn
}

// timeoutOk returns a Conn which will _not_ call Close on itself if it sees a
// timeout error during a Decode call (though it will still return that error).
// Close will still be called other read/write errors.
// TODO not sure if I want to make this public
func timeoutOk(c Conn) Conn {
	return &timeoutOkConn{c}
}

func (toc *timeoutOkConn) Decode(u resp.Unmarshaler) error {
	err := toc.Conn.Decode(timeoutOkUnmarshaler{u})
	if err == nil {
		return nil
	}
	if te, ok := err.(errTimeout); ok {
		err = te.err
	} else {
		toc.Close()
	}
	return err
}

type timeoutOkUnmarshaler struct {
	u resp.Unmarshaler
}

type errTimeout struct {
	err error
}

func (et errTimeout) Error() string { return et.err.Error() }

func (tou timeoutOkUnmarshaler) UnmarshalRESP(p *resp.Pool, br *bufio.Reader) error {
	err := tou.u.UnmarshalRESP(p, br)
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		err = errTimeout{err}
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////

// PubSubMessage describes a message being published to a subscribed channel
type PubSubMessage struct {
	Type    string // "message" or "pmessage"
	Pattern string // will be set if Type is "pmessage"
	Channel string
	Message []byte
}

// MarshalRESP implements the Marshaler interface. It will assume the
// PubSubMessage is a PMESSAGE if Pattern is non-empty.
func (m PubSubMessage) MarshalRESP(p *resp.Pool, w io.Writer) error {
	var err error
	marshal := func(m resp.Marshaler) {
		if err == nil {
			err = m.MarshalRESP(p, w)
		}
	}

	if m.Type == "message" {
		marshal(resp.ArrayHeader{N: 3})
		marshal(resp.BulkString{B: []byte(m.Type)})
	} else if m.Type == "pmessage" {
		marshal(resp.ArrayHeader{N: 4})
		marshal(resp.BulkString{B: []byte(m.Type)})
		marshal(resp.BulkString{B: []byte(m.Pattern)})
	} else {
		return errors.New("unknown message Type")
	}
	marshal(resp.BulkString{B: []byte(m.Channel)})
	marshal(resp.BulkString{B: m.Message})
	return err
}

// UnmarshalRESP implements the Unmarshaler interface
func (m *PubSubMessage) UnmarshalRESP(p *resp.Pool, br *bufio.Reader) error {
	bb := make([][]byte, 0, 4)
	if err := (resp.Any{I: &bb}).UnmarshalRESP(p, br); err != nil {
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

// PubSubConn wraps a Conn to support redis' pubsub system. User-created
// channels can be subscribed to redis channels to receive PubSubMessages which
// have been published.
//
// If any methods return an error it means the PubSubConn has been Close'd and
// subscribed msgCh's will no longer receive PubSubMessages from it. All methods
// are threadsafe.
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
	Unsubscribe(msgCh chan<- PubSubMessage, channels ...string) error

	// PSubscribe is like Subscribe, but it subscribes msgCh to a set of
	// patterns and not individual channels.
	PSubscribe(msgCh chan<- PubSubMessage, patterns ...string) error

	// PUnsubscribe is like Unsubscribe, but it unsubscribes msgCh from a set of
	// patterns and not individual channels.
	PUnsubscribe(msgCh chan<- PubSubMessage, patterns ...string) error

	// Ping performs a simple Ping command on the PubSubConn, returning an error
	// if it failed for some reason
	Ping() error

	// Close closes the PubSubConn so it can't be used anymore. All subscribed
	// channels will stop receiving PubSubMessages from this Conn.
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
}

// PubSub wraps the given Conn so that it becomes a PubSubConn.
func PubSub(rc Conn) PubSubConn {
	return newPubSub(rc, nil)
}

func newPubSub(rc Conn, closeErrCh chan error) PubSubConn {
	c := &pubSubConn{
		conn:       timeoutOk(rc),
		subs:       chanSet{},
		psubs:      chanSet{},
		cmdResCh:   make(chan error, 1),
		closeErrCh: closeErrCh,
	}
	go c.spin()
	return c
}

func (c *pubSubConn) publish(m PubSubMessage) {
	c.csL.RLock()
	defer c.csL.RUnlock()

	for ch := range c.subs[m.Channel] {
		m.Type = "message"
		m.Pattern = ""
		ch <- m
	}
	for pattern, csm := range c.psubs {
		if !globMatch(pattern, m.Channel) {
			continue
		}
		m.Type = "pmessage"
		m.Pattern = pattern
		for ch := range csm {
			ch <- m
		}
	}
}

func (c *pubSubConn) spin() {
	for {
		var rm resp.RawMessage
		err := c.conn.Decode(&rm)
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			continue
		} else if err != nil {
			c.closeInner(err)
			return
		}

		var m PubSubMessage
		if err := rm.UnmarshalInto(nil, &m); err == nil {
			c.publish(m)
		} else {
			c.cmdResCh <- nil
		}
	}
}

func (c *pubSubConn) do(cmd RawCmd, exp int) error {
	c.cmdL.Lock()
	defer c.cmdL.Unlock()

	if err := c.conn.Encode(cmd); err != nil {
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
	c.csL.Lock()
	defer c.csL.Unlock()
	missing := c.subs.missing(channels)
	if len(missing) > 0 {
		if err := c.do(CmdNoKey("SUBSCRIBE", missing), len(missing)); err != nil {
			return err
		}
	}

	for _, channel := range channels {
		c.subs.add(channel, msgCh)
	}
	return nil
}

func (c *pubSubConn) Unsubscribe(msgCh chan<- PubSubMessage, channels ...string) error {
	c.csL.Lock()
	defer c.csL.Unlock()

	emptyChannels := make([]string, 0, len(channels))
	for _, channel := range channels {
		if empty := c.subs.del(channel, msgCh); empty {
			emptyChannels = append(emptyChannels, channel)
		}
	}

	if len(emptyChannels) == 0 {
		return nil
	}

	return c.do(CmdNoKey("UNSUBSCRIBE", emptyChannels), len(emptyChannels))
}

func (c *pubSubConn) PSubscribe(msgCh chan<- PubSubMessage, patterns ...string) error {
	c.csL.Lock()
	defer c.csL.Unlock()
	missing := c.psubs.missing(patterns)
	if len(missing) > 0 {
		if err := c.do(CmdNoKey("PSUBSCRIBE", missing), len(missing)); err != nil {
			return err
		}
	}

	for _, pattern := range patterns {
		c.psubs.add(pattern, msgCh)
	}
	return nil
}

func (c *pubSubConn) PUnsubscribe(msgCh chan<- PubSubMessage, patterns ...string) error {
	c.csL.Lock()
	defer c.csL.Unlock()

	emptyPatterns := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if empty := c.psubs.del(pattern, msgCh); empty {
			emptyPatterns = append(emptyPatterns, pattern)
		}
	}

	if len(emptyPatterns) == 0 {
		return nil
	}

	return c.do(CmdNoKey("PUNSUBSCRIBE", emptyPatterns), len(emptyPatterns))
}

func (c *pubSubConn) Ping() error {
	return c.do(CmdNoKey("PING"), 1)
}
