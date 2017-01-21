package radix

import (
	"bufio"
	"bytes"
	"net"

	"github.com/mediocregopher/radix.v2/resp"
)

type stubAddr struct {
	network, addr string
}

func (sa stubAddr) Network() string {
	return sa.network
}

func (sa stubAddr) String() string {
	return sa.addr
}

type stub struct {
	net.Conn   // always nil
	remoteAddr stubAddr

	fn    func([]string) interface{}
	pool  *resp.Pool
	buf   *bytes.Buffer
	bufbr *bufio.Reader
}

// Stub returns a Conn which pretends it is a Conn to a real redis instance, but
// is instead using the given callback to service requests.
//
// When Encode is
// called the given value is marshalled into bytes then unmarshalled into a
// []string, which is passed to the callback. The return from the callback is
// then marshalled and buffered, and will be unmarshalled in the next call to
// Decode.
//
// remoteNetwork and remoteAddr can be empty, but if given will be used as the
// return from the RemoteAddr method. All other inherited net.Conn methods will
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
	buf := new(bytes.Buffer)
	return &stub{
		remoteAddr: stubAddr{network: remoteNetwork, addr: remoteAddr},
		fn:         fn,
		pool:       new(resp.Pool),
		buf:        buf,
		bufbr:      bufio.NewReader(buf),
	}
}

func (s *stub) RemoteAddr() net.Addr {
	return s.remoteAddr
}

// TODO need to lock buf/bufbr in encode/decode
// TODO make decode block until encode is called, if empty?

func (s *stub) Encode(m resp.Marshaler) error {
	// first marshal into a RawMessage
	buf := new(bytes.Buffer)
	if err := m.MarshalRESP(s.pool, buf); err != nil {
		return err
	}
	rm := resp.RawMessage(buf.Bytes())

	// unmarshal that into a string slice
	var ss []string
	if err := rm.UnmarshalInto(s.pool, resp.Any{I: &ss}); err != nil {
		return err
	}

	// get return from callback
	ret := s.fn(ss)
	if err, _ := ret.(error); err != nil {
		return err
	}

	// marshal return back into the Stubs buffer, so it'll get read on next
	// decode
	return resp.Any{I: ret}.MarshalRESP(s.pool, s.buf)
}

func (s *stub) Decode(u resp.Unmarshaler) error {
	return u.UnmarshalRESP(s.pool, s.bufbr)
}

func (s *stub) Close() error {
	return nil
}
