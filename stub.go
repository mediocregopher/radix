package radix

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v2/resp"
)

type bufferAddr struct {
	network, addr string
}

func (sa bufferAddr) Network() string {
	return sa.network
}

func (sa bufferAddr) String() string {
	return sa.addr
}

type buffer struct {
	net.Conn   // always nil
	remoteAddr bufferAddr

	bufL         *sync.Cond
	buf          *bytes.Buffer
	bufbr        *bufio.Reader
	closed       bool
	readDeadline time.Time
}

func newBuffer(remoteNetwork, remoteAddr string) *buffer {
	buf := new(bytes.Buffer)
	return &buffer{
		remoteAddr: bufferAddr{network: remoteNetwork, addr: remoteAddr},
		bufL:       sync.NewCond(new(sync.Mutex)),
		buf:        buf,
		bufbr:      bufio.NewReader(buf),
	}
}

func (b *buffer) Encode(m resp.Marshaler) error {
	b.bufL.L.Lock()
	var err error
	if b.closed {
		err = b.err("write", errClosed)
	} else {
		err = m.MarshalRESP(b.buf)
	}
	b.bufL.L.Unlock()
	if err != nil {
		return err
	}

	b.bufL.Broadcast()
	return nil
}

func (b *buffer) Decode(u resp.Unmarshaler) error {
	b.bufL.L.Lock()
	defer b.bufL.L.Unlock()

	var timeoutCh chan struct{}
	if b.readDeadline.IsZero() {
		// no readDeadline, timeoutCh will never be written to
	} else if now := time.Now(); b.readDeadline.Before(now) {
		return b.err("read", new(timeoutError))
	} else {
		timeoutCh = make(chan struct{}, 2)
		sleep := b.readDeadline.Sub(now)
		go func() {
			time.Sleep(sleep)
			timeoutCh <- struct{}{}
			b.bufL.Broadcast()
		}()
	}

	for b.buf.Len() == 0 && b.bufbr.Buffered() == 0 {
		if b.closed {
			return b.err("read", errClosed)
		}

		select {
		case <-timeoutCh:
			return b.err("read", new(timeoutError))
		default:
		}

		// we have to periodically wakeup to double-check the timeoutCh, if
		// there is one
		if timeoutCh != nil {
			go func() {
				time.Sleep(1 * time.Second)
				b.bufL.Broadcast()
			}()
		}

		b.bufL.Wait()
	}

	return u.UnmarshalRESP(b.bufbr)
}

func (b *buffer) Close() error {
	b.bufL.L.Lock()
	defer b.bufL.L.Unlock()
	if b.closed {
		return b.err("close", errClosed)
	}
	b.closed = true
	b.bufL.Broadcast()
	return nil
}

func (b *buffer) RemoteAddr() net.Addr {
	return b.remoteAddr
}

func (b *buffer) SetDeadline(t time.Time) error {
	return b.SetReadDeadline(t)
}

func (b *buffer) SetReadDeadline(t time.Time) error {
	b.bufL.L.Lock()
	defer b.bufL.L.Unlock()
	if b.closed {
		return b.err("set", errClosed)
	}
	b.readDeadline = t
	return nil
}

func (b *buffer) err(op string, err error) error {
	return &net.OpError{
		Op:     op,
		Net:    "tcp",
		Source: nil,
		Addr:   b.remoteAddr,
		Err:    err,
	}
}

var errClosed = errors.New("use of closed network connection")

type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

////////////////////////////////////////////////////////////////////////////////

type stub struct {
	*buffer
	fn func([]string) interface{}
}

// Stub returns a Conn which pretends it is a Conn to a real redis instance, but
// is instead using the given callback to service requests.
//
// When Encode is called the given value is marshalled into bytes then
// unmarshalled into a []string, which is passed to the callback. The return
// from the callback is then marshalled and buffered, and will be unmarshalled
// in the next call to Decode.
//
// remoteNetwork and remoteAddr can be empty, but if given will be used as the
// return from the RemoteAddr method.
//
// Decode will block until Encode is called in a separate go-routine, if
// necessary. The SetDeadline and SetReadDeadline methods can be used as usual
// to limit how long Decode blocks. All other inherited net.Conn methods will
// panic.
//
// This can then be used to easily mock a redis instance, like so:
//
//	m := map[string]string{}
//	stub := radix.Stub("tcp", "127.0.0.1:6379", func(args []string) interface{} {
//		switch args[0] {
//		case "GET":
//			return m[args[1]]
//		case "SET":
//			m[args[1]] = args[2]
//			return nil
//		default:
//			return fmt.Errorf("getSet doesn't support command %q", args[0])
//		}
//	})
//
//	radix.Cmd("SET", "foo", 1).Run(stub)
//
//	var foo int
//	radix.Cmd("GET", "foo").Into(&foo).Run(stub)
//	fmt.Printf("foo: %d\n", foo)
//
func Stub(remoteNetwork, remoteAddr string, fn func([]string) interface{}) Conn {
	return &stub{
		buffer: newBuffer(remoteNetwork, remoteAddr),
		fn:     fn,
	}
}

func (s *stub) Do(a Action) error {
	return a.Run(s)
}

func (s *stub) Encode(m resp.Marshaler) error {
	// first marshal into a RawMessage
	buf := new(bytes.Buffer)
	if err := m.MarshalRESP(buf); err != nil {
		return err
	}
	rm := resp.RawMessage(buf.Bytes())

	// unmarshal that into a string slice
	var ss []string
	if err := rm.UnmarshalInto(resp.Any{I: &ss}); err != nil {
		return err
	}

	// get return from callback. Results implementing resp.Marshaler are assumed
	// to be wanting to be written in all cases, otherwise if the result is an
	// error it is assumed to want to be returned directly.
	ret := s.fn(ss)
	if m, ok := ret.(resp.Marshaler); ok {
		return s.buffer.Encode(m)
	} else if err, _ := ret.(error); err != nil {
		return err
	}
	return s.buffer.Encode(resp.Any{I: ret})
}

func (s *stub) NetConn() net.Conn {
	return s.buffer
}
