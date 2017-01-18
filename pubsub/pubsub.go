package pubsub

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"sync"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/mediocregopher/radix.v2/resp"
)

type timeoutOkConn struct {
	radix.Conn
}

// TimeoutOk returns a Conn which will _not_ call Close on itself if it sees a
// timeout error during a Decode call (though it will still return that error).
// It will still do so for all other read/write errors, however.
func TimeoutOk(c radix.Conn) radix.Conn {
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

// Message describes a message being published to a subscribed channel
type Message struct {
	Type    string // "message" or "pmessage"
	Pattern string // will be set if Type is "pmessage"
	Channel string
	Message []byte
}

// MarshalRESP implements the radix.Marshaler interface. It will assume the
// Message is a PMESSAGE if Pattern is non-empty.
func (m Message) MarshalRESP(p *resp.Pool, w io.Writer) error {
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

// UnmarshalRESP implements the radix.Unmarshaler interface
func (m *Message) UnmarshalRESP(p *resp.Pool, br *bufio.Reader) error {
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
	} else if m.Type != "message" {
		return errors.New("not message or pmessage")
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

type res struct {
	rm  resp.RawMessage
	err error
}

type pubSubConn struct {
	radix.Conn
	resCh   chan res
	outCh   chan Message
	errCh   chan error
	closeCh chan struct{}

	close    sync.Once
	closeErr error
}

// New wraps the given Conn to support redis' pubsub. The returned Conn can be
// used to perform (P)SUBSCRIBE, (P)UNSUBSCRIBE, and PING commands. The returned
// Message channel _must_ be read until it is closed once a (P)SUBSCRIBE command
// has been made. The returned error channel is buffered and will have an error
// written to it if one is encountered, or nil if the Conn is closed normally.
//
// The passed in Conn should not be used again, only the returned one.
func New(c radix.Conn) (radix.Conn, <-chan Message, <-chan error) {
	outCh := make(chan Message)
	errCh := make(chan error, 1)
	psc := &pubSubConn{
		Conn:    TimeoutOk(c),
		resCh:   make(chan res),
		outCh:   outCh,
		errCh:   errCh,
		closeCh: make(chan struct{}),
	}
	go psc.spin()
	return psc, outCh, errCh
}

func (psc *pubSubConn) spin() {
	defer psc.Close()
	defer close(psc.outCh)
	defer close(psc.errCh)
	defer close(psc.resCh)
	for {
		var rm resp.RawMessage
		err := psc.Conn.Decode(&rm)
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			continue
		} else if err != nil {
			psc.errCh <- err
			// there's an active call to Decode give them the error too
			select {
			case psc.resCh <- res{err: err}:
			default:
			}
			return
		}

		var m Message
		if err := rm.UnmarshalInto(nil, &m); err == nil {
			select {
			case psc.outCh <- m:
			case <-psc.closeCh:
				return
			}
		} else {
			select {
			case psc.resCh <- res{rm: rm}:
			case <-psc.closeCh:
				return
			}
		}
	}
}

func (psc *pubSubConn) Decode(u resp.Unmarshaler) error {
	res, ok := <-psc.resCh
	if !ok {
		return errors.New("connection closed")
	} else if res.err != nil {
		return res.err
	}

	return res.rm.UnmarshalInto(nil, u)
}

func (psc *pubSubConn) Close() error {
	psc.close.Do(func() {
		close(psc.closeCh)
		psc.closeErr = psc.Conn.Close()
	})
	return psc.closeErr
}
