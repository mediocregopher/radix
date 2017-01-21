package pubsub

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"sync"

	radix "github.com/mediocregopher/radix.v2"
	"github.com/mediocregopher/radix.v2/resp"
)

/*
	// TODO this is an example of using the Stub, but it doesn't currently work,
	// probably because radix.Stub doesn't really do timeouts correctly so
	// pubsub.New doesn't interact with it correctly. pubsub.New is also broken,
	// so that probably isn't helping

	// The channel we'll write our fake messages to. These writes shouldn't do
	// anything, initially, since we haven't subscribed to anything
	stubCh := make(chan pubsub.Message)
	go func() {
		for {
			stubCh <- pubsub.Message{
				Type:    "message",
				Channel: "foo",
				Message: []byte("bar"),
			}
			time.Sleep(1 * time.Second)
		}
	}()

	// Make an underlying stub conn which handles normal redis commands. This
	// one return nil for everything. You _could_ use a real redis connection
	// too if you wanted
	conn := radix.Stub("tcp", "127.0.0.1:6379", func([]string) interface{} {
		return nil
	})
	conn = pubsub.Stub(conn, stubCh)

	// Wrap the conn like we would for a normal redis connection
	pubsubConn, ch, errCh := pubsub.New(conn)

	// Subscribe to "foo"
	if err := radix.Cmd("SUBSCRIBE", "foo").Run(pubsubConn); err != nil {
		log.Fatal(err)
	}

	for m := range ch {
		log.Printf("read m: %#v", m)
	}
	if err := <-errCh; err != nil {
		// this shouldn't really happen with a stub
		log.Fatal(err)
	}
*/

var errPubSubMode = errors.New("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")

type stub struct {
	radix.Conn

	inCh <-chan Message

	closeOnce sync.Once
	closeCh   chan struct{}

	l               sync.Mutex
	pubsubMode      bool
	subbed, psubbed map[string]bool
	buf             *bytes.Buffer
	bufbr           *bufio.Reader

	// this is only used for tests
	mDoneCh chan struct{}
}

// Stub wraps an existing radix.Conn so that Messages read from the given
// channel will be pushed onto it, if SUBSCRIBE or PSUBSCRIBE have been called
// and match the message. Messages written to the channel nead only have their
// Channel and Message fields set.
//
// Stub will intercept (P)SUBSCRIBE, (P)UNSUBSCRIBE, and PING commands so that
// it will behave exactly like a normal redis call. When no channels or patterns
// are subscribed to then all calls will be handled by the given Conn, otherwise
// the Stub will remain in "pubsub mode", just like a normal redis connection.
//
// The given channel may be buffered, but should never be closed.
//
func Stub(conn radix.Conn, ch <-chan Message) radix.Conn {
	s := &stub{
		Conn:    conn,
		inCh:    ch,
		closeCh: make(chan struct{}),
		subbed:  map[string]bool{},
		psubbed: map[string]bool{},
		buf:     new(bytes.Buffer),
		mDoneCh: make(chan struct{}, 1),
	}
	s.bufbr = bufio.NewReader(s.buf)
	go s.spin()
	return s
}

func (s *stub) Encode(m resp.Marshaler) error {
	// first marshal into a RawMessage
	buf := new(bytes.Buffer)
	if err := m.MarshalRESP(nil, buf); err != nil {
		return err
	}
	rm := resp.RawMessage(buf.Bytes())

	// unmarshal that into a string slice, if it's not a valid cmd then forward
	// that to inner Encode
	var ss []string
	if err := rm.UnmarshalInto(nil, resp.Any{I: &ss}); err != nil {
		return err
	} else if len(ss) < 1 {
		return s.Conn.Encode(rm)
	}

	// both write and writeRes assume they're locked

	var err error
	write := func(ii ...interface{}) {
		if err == nil {
			err = resp.Any{I: ii}.MarshalRESP(nil, s.buf)
		}
	}

	writeRes := func(cmd, subj string) {
		c := len(s.subbed) + len(s.psubbed)
		write(cmd, subj, c)
		s.pubsubMode = c > 0
	}

	switch strings.ToUpper(ss[0]) {
	case "PING":
		s.l.Lock()
		if !s.pubsubMode {
			s.l.Unlock()
			return s.Conn.Encode(rm)
		}
		write("pong", "")
		s.l.Unlock()
	case "SUBSCRIBE":
		s.l.Lock()
		for _, channel := range ss[1:] {
			s.subbed[channel] = true
			writeRes("subscribe", channel)
		}
		s.l.Unlock()
	case "UNSUBSCRIBE":
		s.l.Lock()
		for _, channel := range ss[1:] {
			delete(s.subbed, channel)
			writeRes("unsubscribe", channel)
		}
		s.l.Unlock()
	case "PSUBSCRIBE":
		s.l.Lock()
		for _, pattern := range ss[1:] {
			s.psubbed[pattern] = true
			writeRes("psubscribe", pattern)
		}
		s.l.Unlock()
	case "PUNSUBSCRIBE":
		s.l.Lock()
		for _, pattern := range ss[1:] {
			delete(s.psubbed, pattern)
			writeRes("punsubscribe", pattern)
		}
		s.l.Unlock()
	default:
		s.l.Lock()
		if s.pubsubMode {
			err = resp.Any{I: errPubSubMode}.MarshalRESP(nil, s.buf)
			s.l.Unlock()
		} else {
			s.l.Unlock()
			err = s.Conn.Encode(rm)
		}
	}

	return err
}

func (s *stub) Decode(u resp.Unmarshaler) error {
	s.l.Lock()
	var err error
	if s.bufbr.Buffered() > 0 || s.buf.Len() > 0 {
		err = u.UnmarshalRESP(nil, s.bufbr)
		s.l.Unlock()
		return err
	}
	s.l.Unlock()

	return s.Conn.Decode(u)
}

func (s *stub) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closeCh)
		err = s.Conn.Close()
	})
	return err
}

func (s *stub) spin() {
	for {
		select {
		case m, ok := <-s.inCh:
			if !ok {
				panic("pubsub stub message channel was closed")
			}
			// we ignore marshal errors because Message is known to be reliable
			// and it's a stub so whatever
			s.l.Lock()
			for channel := range s.subbed {
				if channel != m.Channel {
					continue
				}
				m.Type = "message"
				m.MarshalRESP(nil, s.buf)
			}
			for pattern := range s.psubbed {
				if !globMatch(pattern, m.Channel) {
					continue
				}
				m.Type = "pmessage"
				m.Pattern = pattern
				m.MarshalRESP(nil, s.buf)
			}
			select {
			case s.mDoneCh <- struct{}{}:
			default:
			}
			s.l.Unlock()
		case <-s.closeCh:
			return
		}
	}
}
