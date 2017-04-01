package radix

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/mediocregopher/radix.v2/resp"
)

var errPubSubMode = resp.Error{
	E: errors.New("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context"),
}

type multiMarshal []resp.Marshaler

func (mm multiMarshal) MarshalRESP(w io.Writer) error {
	for _, m := range mm {
		if err := m.MarshalRESP(w); err != nil {
			return err
		}
	}
	return nil
}

type pubSubStub struct {
	Conn
	fn   func([]string) interface{}
	inCh <-chan PubSubMessage

	closeOnce sync.Once
	closeCh   chan struct{}
	closeErr  error

	l               sync.Mutex
	pubsubMode      bool
	subbed, psubbed map[string]bool

	// this is only used for tests
	mDoneCh chan struct{}
}

// TODO probably make this into an example

// PubSubStub returns a Conn much like Stub does. It differs in that Encode
// calls for (P)SUBSCRIBE, (P)UNSUBSCRIBE, MESSAGE, and PING will be intercepted
// and handled as per redis' expected pubsub functionality. A PubSubMessage may
// be written to the returned channel at any time, and if the PubSubStub has had
// (P)SUBSCRIBE called matching that PubSubMessage it will be written to the
// PubSubStub's internal buffer as expected.
//
// This is intended to be used to so that it can mock services which can perform
// both normal redis commands and pubsub (e.g. a real redis instance, redis
// sentinel). Once created this stub can be wrapped in a normal PubSubConn using
// PubSub and treated like a real connection. Here's an example:
//
//	// Make a pubsub stub conn which handles normal redis commands. This one
//	// will return nil for everything except pubsub commands (which will be
//	// handled automatically)
//	stub, stubCh := radix.PubSubStub("tcp", "127.0.0.1:6379", func([]string) interface{} {
//		return nil
//	})
//
//	// These writes shouldn't do anything, initially, since we haven't
//	// subscribed to anything
//	go func() {
//		for {
//			stubCh <- radix.PubSubMessage{
//				Channel: "foo",
//				Message: []byte("bar"),
//			}
//			time.Sleep(1 * time.Second)
//		}
//	}()
//
//	// Wrap the stub like we would for a normal redis connection
//	pstub := PubSub(stub)
//	msgCh := make(chan PubSubMessage)
//
//	// Subscribe to "foo"
//	if err := pstub.Subscribe(msgCh, "foo"); err != nil {
//		log.Fatal(err)
//	}
//
//	// listen for messages and also periodically Ping the connection
//	pingTick := time.Tick(10 * time.Second)
//	for {
//		select {
//		case <-pingTick:
//			if err := pstub.Ping(); err != nil {
//				log.Fatal(err)
//			}
//		case m := <-msgCh:
//			log.Printf("read m: %#v", m)
//		}
//	}
//
func PubSubStub(remoteNetwork, remoteAddr string, fn func([]string) interface{}) (Conn, chan<- PubSubMessage) {
	ch := make(chan PubSubMessage)
	s := &pubSubStub{
		fn:      fn,
		inCh:    ch,
		closeCh: make(chan struct{}),
		subbed:  map[string]bool{},
		psubbed: map[string]bool{},
		mDoneCh: make(chan struct{}, 1),
	}
	s.Conn = Stub(remoteNetwork, remoteAddr, s.innerFn)
	go s.spin()
	return s, ch
}

func (s *pubSubStub) innerFn(ss []string) interface{} {
	s.l.Lock()
	defer s.l.Unlock()

	writeRes := func(mm multiMarshal, cmd, subj string) multiMarshal {
		c := len(s.subbed) + len(s.psubbed)
		s.pubsubMode = c > 0
		return append(mm, resp.Any{I: []interface{}{cmd, subj, c}})
	}

	switch strings.ToUpper(ss[0]) {
	case "PING":
		if !s.pubsubMode {
			return s.fn(ss)
		}
		return []string{"pong", ""}
	case "SUBSCRIBE":
		var mm multiMarshal
		for _, channel := range ss[1:] {
			s.subbed[channel] = true
			mm = writeRes(mm, "subscribe", channel)
		}
		return mm
	case "UNSUBSCRIBE":
		var mm multiMarshal
		for _, channel := range ss[1:] {
			delete(s.subbed, channel)
			mm = writeRes(mm, "unsubscribe", channel)
		}
		return mm
	case "PSUBSCRIBE":
		var mm multiMarshal
		for _, pattern := range ss[1:] {
			s.psubbed[pattern] = true
			mm = writeRes(mm, "psubscribe", pattern)
		}
		return mm
	case "PUNSUBSCRIBE":
		var mm multiMarshal
		for _, pattern := range ss[1:] {
			delete(s.psubbed, pattern)
			mm = writeRes(mm, "punsubscribe", pattern)
		}
		return mm
	case "MESSAGE":
		m := PubSubMessage{
			Channel: ss[1],
			Message: []byte(ss[2]),
		}

		var mm multiMarshal
		for channel := range s.subbed {
			if channel != m.Channel {
				continue
			}
			m.Type = "message"
			mm = append(mm, m)
		}
		for pattern := range s.psubbed {
			if !globMatch(pattern, m.Channel) {
				continue
			}
			m.Type = "pmessage"
			m.Pattern = pattern
			mm = append(mm, m)
		}
		return mm
	default:
		if s.pubsubMode {
			return errPubSubMode
		}
		return s.fn(ss)
	}
}

func (s *pubSubStub) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeCh)
		s.closeErr = s.Conn.Close()
	})
	return s.closeErr
}

func (s *pubSubStub) spin() {
	for {
		select {
		case m, ok := <-s.inCh:
			if !ok {
				panic("PubSubStub message channel was closed")
			}
			m.Type = "message"
			m.Pattern = ""
			if err := s.Conn.Encode(m); err != nil {
				panic(fmt.Sprintf("error encoding message in PubSubStub: %s", err))
			}
			select {
			case s.mDoneCh <- struct{}{}:
			default:
			}
		case <-s.closeCh:
			return
		}
	}
}
