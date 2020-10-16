package radix

import (
	"bufio"
	"context"
	"errors"
	"io"
	"time"

	"github.com/mediocregopher/radix/v4/internal/proc"
	"github.com/mediocregopher/radix/v4/resp"
	"github.com/mediocregopher/radix/v4/resp/resp3"
)

// PubSubMessage describes a message being published to a subscribed channel
type PubSubMessage struct {
	Type    string // "message" or "pmessage"
	Pattern string // will be set if Type is "pmessage"
	Channel string
	Message []byte
}

// MarshalRESP implements the Marshaler interface.
func (m PubSubMessage) MarshalRESP(w io.Writer, o *resp.Opts) error {
	var err error
	marshal := func(m resp.Marshaler) {
		if err == nil {
			err = m.MarshalRESP(w, o)
		}
	}

	if m.Type == "message" {
		marshal(resp3.ArrayHeader{NumElems: 3})
		marshal(resp3.BlobString{S: m.Type})
	} else if m.Type == "pmessage" {
		marshal(resp3.ArrayHeader{NumElems: 4})
		marshal(resp3.BlobString{S: m.Type})
		marshal(resp3.BlobString{S: m.Pattern})
	} else {
		return errors.New("unknown message Type")
	}
	marshal(resp3.BlobString{S: m.Channel})
	marshal(resp3.BlobStringBytes{B: m.Message})
	return err
}

var errNotPubSubMessage = errors.New("message is not a PubSubMessage")

// UnmarshalRESP implements the Unmarshaler interface
func (m *PubSubMessage) UnmarshalRESP(br *bufio.Reader, o *resp.Opts) error {
	// This method will fully consume the message on the wire, regardless of if
	// it is a PubSubMessage or not. If it is not then errNotPubSubMessage is
	// returned.

	// When in subscribe mode redis only allows (P)(UN)SUBSCRIBE commands, which
	// all return arrays, and PING, which returns an array when in subscribe
	// mode. HOWEVER, when all channels have been unsubscribed from then the
	// connection will be taken _out_ of subscribe mode. This is theoretically
	// fine, since the driver will still only allow the 5 commands, except PING
	// will return a simple string when in the non-subscribed state. So this
	// needs to check for that.
	if prefix, err := br.Peek(1); err != nil {
		return err
	} else if resp3.Prefix(prefix[0]) == resp3.SimpleStringPrefix {
		// if it's a simple string, discard it (it's probably PONG) and error
		if err := resp3.Unmarshal(br, nil, o); err != nil {
			return err
		}
		return resp.ErrConnUsable{Err: errNotPubSubMessage}
	}

	var ah resp3.ArrayHeader
	if err := ah.UnmarshalRESP(br, o); err != nil {
		return err
	} else if ah.NumElems < 2 {
		return errors.New("message has too few elements")
	}

	var msgType resp3.BlobStringBytes
	if err := msgType.UnmarshalRESP(br, o); err != nil {
		return err
	}

	switch string(msgType.B) {
	case "message":
		m.Type = "message"
		if ah.NumElems != 3 {
			return errors.New("message has wrong number of elements")
		}
	case "pmessage":
		m.Type = "pmessage"
		if ah.NumElems != 4 {
			return errors.New("message has wrong number of elements")
		}

		var pattern resp3.BlobString
		if err := pattern.UnmarshalRESP(br, o); err != nil {
			return err
		}
		m.Pattern = pattern.S
	default:
		// if it's not a PubSubMessage then discard the rest of the array
		for i := 1; i < ah.NumElems; i++ {
			if err := resp3.Unmarshal(br, nil, o); err != nil {
				return err
			}
		}
		return errNotPubSubMessage
	}

	var channel resp3.BlobString
	if err := channel.UnmarshalRESP(br, o); err != nil {
		return err
	}
	m.Channel = channel.S

	var msg resp3.BlobStringBytes
	if err := msg.UnmarshalRESP(br, o); err != nil {
		return err
	}
	m.Message = msg.B

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
	out := make([]string, 0, len(ss))
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
// are threadsafe, but should be called in a different go-routine than that
// which is reading from the PubSubMessage channels.
//
// NOTE the PubSubMessage channels should never block. If any channels block
// when being written to they will block all other channels from receiving a
// publish and block methods from returning.
type PubSubConn interface {
	// Subscribe subscribes the PubSubConn to the given set of channels. msgCh
	// will receieve a PubSubMessage for every publish written to any of the
	// channels. This may be called multiple times for the same channels and
	// different msgCh's, each msgCh will receieve a copy of the PubSubMessage
	// for each publish.
	Subscribe(ctx context.Context, msgCh chan<- PubSubMessage, channels ...string) error

	// Unsubscribe unsubscribes the msgCh from the given set of channels, if it
	// was subscribed at all.
	//
	// NOTE until Unsubscribe has returned it should be assumed that msgCh can
	// still have messages written to it.
	Unsubscribe(ctx context.Context, msgCh chan<- PubSubMessage, channels ...string) error

	// PSubscribe is like Subscribe, but it subscribes msgCh to a set of
	// patterns and not individual channels.
	PSubscribe(ctx context.Context, msgCh chan<- PubSubMessage, patterns ...string) error

	// PUnsubscribe is like Unsubscribe, but it unsubscribes msgCh from a set of
	// patterns and not individual channels.
	PUnsubscribe(ctx context.Context, msgCh chan<- PubSubMessage, patterns ...string) error

	// Ping performs a simple Ping command on the PubSubConn, returning an error
	// if it failed for some reason
	Ping(context.Context) error

	// Close closes the PubSubConn so it can't be used anymore. All subscribed
	// channels will stop receiving PubSubMessages from this Conn (but will not
	// themselves be closed).
	//
	// NOTE until Close returns it should be assumed that all subscribed msgChs
	// can still be written to.
	Close() error
}

type pubSubConn struct {
	proc *proc.Proc
	conn Conn

	subs  chanSet
	psubs chanSet

	// pubCh is used for communicating between spin and pubSpin
	pubCh chan PubSubMessage

	// used for writing commands and waiting for their response (e.g.
	// SUBSCRIBE, PING). See the do method for how that works.
	cmdResCh chan error

	// This one is optional, and kind of cheating. We use it in persistent to
	// get on-the-fly updates of when the connection fails. Maybe one day this
	// could be exposed if there's a clean way of doing so, or another way
	// accomplishing the same thing could be done instead.
	closeErrCh chan error

	// only used during testing
	testEventCh chan string
}

// NewPubSubConn wraps the given Conn so that it becomes a PubSubConn. The
// passed in Conn should not be used after this call.
func NewPubSubConn(rc Conn) PubSubConn {
	return newPubSub(rc, nil)
}

func newPubSub(rc Conn, closeErrCh chan error) PubSubConn {
	c := &pubSubConn{
		proc:       proc.New(),
		conn:       rc,
		subs:       chanSet{},
		psubs:      chanSet{},
		pubCh:      make(chan PubSubMessage, 1024),
		cmdResCh:   make(chan error, 1),
		closeErrCh: closeErrCh,
	}
	c.proc.Run(c.pubSpin)
	c.proc.Run(c.spin)
	c.proc.Run(c.pingSpin)
	return c
}

func (c *pubSubConn) testEvent(str string) {
	if c.testEventCh != nil {
		c.testEventCh <- str
	}
}

func (c *pubSubConn) publish(m PubSubMessage) {
	_ = c.proc.WithRLock(func() error {
		var subs map[chan<- PubSubMessage]bool
		if m.Type == "pmessage" {
			subs = c.psubs[m.Pattern]
		} else {
			subs = c.subs[m.Channel]
		}

		for ch := range subs {
			ch <- m
		}
		return nil
	})
}

func (c *pubSubConn) pubSpin(ctx context.Context) {
	doneCh := ctx.Done()
	for {
		select {
		case <-doneCh:
			return
		case m := <-c.pubCh:
			c.publish(m)
		}
	}
}

const pubSubTimeout = 1 * time.Second

func (c *pubSubConn) spin(ctx context.Context) {
	doneCh := ctx.Done()
	for {
		var m PubSubMessage
		ctx, cancel := context.WithTimeout(ctx, pubSubTimeout)
		err := c.conn.EncodeDecode(ctx, nil, &m)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			c.testEvent("timeout")
			continue
		} else if errors.Is(err, context.Canceled) {
			return
		} else if errors.Is(err, errNotPubSubMessage) {
			c.cmdResCh <- nil
			continue
		} else if err != nil {
			// this must be done in a new go-routine to avoid deadlocks
			go c.closeInner(err)
			return
		}

		select {
		case c.pubCh <- m:
		case <-doneCh:
			return
		default:
			panic("TODO figure out what to do in this case")
		}
	}
}

func (c *pubSubConn) pingSpin(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := c.Ping(ctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

// NOTE proc's lock _must_ be held to use do
func (c *pubSubConn) do(ctx context.Context, exp int, cmd string, args ...string) error {
	rcmd := Cmd(nil, cmd, args...).(resp.Marshaler)
	if err := c.conn.EncodeDecode(ctx, rcmd, nil); err != nil {
		return err
	}

	doneCh := c.proc.ClosedCh()
	for i := 0; i < exp; i++ {
		select {
		case err := <-c.cmdResCh:
			if err != nil {
				return err
			}
		case <-doneCh:
			return proc.ErrClosed
		}
	}
	return nil
}

func (c *pubSubConn) closeInner(cmdResErr error) error {
	return c.proc.Close(func() error {
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
		err := c.conn.Close()
		return err
	})
}

func (c *pubSubConn) Close() error {
	return c.closeInner(nil)
}

func (c *pubSubConn) Subscribe(ctx context.Context, msgCh chan<- PubSubMessage, channels ...string) error {
	return c.proc.WithLock(func() error {
		missing := c.subs.missing(channels)
		if len(missing) > 0 {
			if err := c.do(ctx, len(missing), "SUBSCRIBE", missing...); err != nil {
				return err
			}
		}

		for _, channel := range channels {
			c.subs.add(channel, msgCh)
		}

		return nil
	})
}

func (c *pubSubConn) Unsubscribe(ctx context.Context, msgCh chan<- PubSubMessage, channels ...string) error {
	return c.proc.WithLock(func() error {
		emptyChannels := make([]string, 0, len(channels))
		for _, channel := range channels {
			if empty := c.subs.del(channel, msgCh); empty {
				emptyChannels = append(emptyChannels, channel)
			}
		}

		if len(emptyChannels) == 0 {
			return nil
		}

		return c.do(ctx, len(emptyChannels), "UNSUBSCRIBE", emptyChannels...)
	})
}

func (c *pubSubConn) PSubscribe(ctx context.Context, msgCh chan<- PubSubMessage, patterns ...string) error {
	return c.proc.WithLock(func() error {
		missing := c.psubs.missing(patterns)
		if len(missing) > 0 {
			if err := c.do(ctx, len(missing), "PSUBSCRIBE", missing...); err != nil {
				return err
			}
		}

		for _, pattern := range patterns {
			c.psubs.add(pattern, msgCh)
		}

		return nil
	})
}

func (c *pubSubConn) PUnsubscribe(ctx context.Context, msgCh chan<- PubSubMessage, patterns ...string) error {
	return c.proc.WithLock(func() error {
		emptyPatterns := make([]string, 0, len(patterns))
		for _, pattern := range patterns {
			if empty := c.psubs.del(pattern, msgCh); empty {
				emptyPatterns = append(emptyPatterns, pattern)
			}
		}

		if len(emptyPatterns) == 0 {
			return nil
		}

		return c.do(ctx, len(emptyPatterns), "PUNSUBSCRIBE", emptyPatterns...)
	})
}

func (c *pubSubConn) Ping(ctx context.Context) error {
	return c.proc.WithRLock(func() error {
		return c.do(ctx, 1, "PING")
	})
}
