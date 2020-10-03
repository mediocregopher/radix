package radix

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix/v3/resp"
	"github.com/mediocregopher/radix/v3/resp/resp2"
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

// in the end this is really just a complicated stub of net.Conn
type buffer struct {
	net.Conn   // always nil
	remoteAddr bufferAddr

	bufL   *sync.Cond
	buf    *bytes.Buffer
	bufbr  *bufio.Reader
	closed bool
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

func (b *buffer) Decode(ctx context.Context, u resp.Unmarshaler) error {
	b.bufL.L.Lock()
	defer b.bufL.L.Unlock()

	wakeupTicker := time.NewTicker(250 * time.Millisecond)
	defer wakeupTicker.Stop()

	for b.buf.Len() == 0 && b.bufbr.Buffered() == 0 {
		if b.closed {
			return b.err("read", errClosed)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// we have to periodically wakeup to check if the context is done
		go func() {
			<-wakeupTicker.C
			b.bufL.Broadcast()
		}()

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

////////////////////////////////////////////////////////////////////////////////

type stub struct {
	*buffer
	fn func([]string) interface{}
}

// Stub returns a (fake) Conn which pretends it is a Conn to a real redis
// instance, but is instead using the given callback to service requests. It is
// primarily useful for writing tests.
//
// When EncodeDecode is called the value to be marshaled is converted into a
// []string and passed to the callback. The return from the callback is then
// marshaled into an internal buffer. The value to be decoded is unmarshaled
// into using the internal buffer. If the internal buffer is empty at
// this step then the call will block.
//
// remoteNetwork and remoteAddr can be empty, but if given will be used as the
// return from the RemoteAddr method.
//
func Stub(remoteNetwork, remoteAddr string, fn func([]string) interface{}) Conn {
	return &stub{
		buffer: newBuffer(remoteNetwork, remoteAddr),
		fn:     fn,
	}
}

func (s *stub) Do(ctx context.Context, a Action) error {
	return a.Perform(ctx, s)
}

func (s *stub) EncodeDecode(ctx context.Context, m resp.Marshaler, u resp.Unmarshaler) error {
	if m != nil {
		buf := new(bytes.Buffer)
		if err := m.MarshalRESP(buf); err != nil {
			return err
		}
		br := bufio.NewReader(buf)

		for {
			var ss []string
			if buf.Len() == 0 && br.Buffered() == 0 {
				break
			} else if err := (resp2.Any{I: &ss}).UnmarshalRESP(br); err != nil {
				return err
			}

			// get return from callback. Results implementing resp.Marshaler are
			// assumed to be wanting to be written in all cases, otherwise if
			// the result is an error it is assumed to want to be returned
			// directly.
			ret := s.fn(ss)
			if m, ok := ret.(resp.Marshaler); ok {
				if err := s.buffer.Encode(m); err != nil {
					return err
				}
			} else if err, _ := ret.(error); err != nil {
				return err
			} else if err = s.buffer.Encode(resp2.Any{I: ret}); err != nil {
				return err
			}
		}
	}

	if u != nil {
		if err := s.buffer.Decode(ctx, u); err != nil {
			return err
		}
	}

	return nil
}

func (s *stub) NetConn() net.Conn {
	return s.buffer
}
