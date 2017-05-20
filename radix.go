// Package radix is a simple redis driver. It needs better docs
package radix

import (
	"bufio"
	"net"
	"time"

	"github.com/mediocregopher/radix.v2/resp"
)

// TODO expose stats for Clients in some way

// Client describes an entity which can carry out Actions, e.g. a connection
// pool for a single redis instance or the cluster client.
type Client interface {
	Do(Action) error

	// Once Close() is called all future method calls on the Client will return
	// an error
	Close() error

	// Stats returns any runtime stats that the implementation of Client wishes
	// to return, or nil if it doesn't want to return any. This method aims to
	// help support logging and debugging, not necessarily to give any
	// actionable information to the program during runtime.
	//
	// Examples of useful runtime stats for a pool might be: number of
	// connections currently available, number of connections currently lent
	// out, number of connections ever created, number of connections ever
	// closed, average time to create a new connection, and so on.
	//
	// TODO I'm not sure if I actually like this
	//
	//Stats() map[string]interface{}
}

// Conn is a Client which synchronously reads/writes data on a network
// connection using the redis resp protocol.
//
// Encode and Decode may be called at the same time by two different
// go-routines, but each should only be called once at a time (i.e. two routines
// shouldn't call Encode at the same time, same with Decode).
//
// If either Encode or Decode encounter a net.Error the Conn will be
// automatically closed.
type Conn interface {
	Encode(resp.Marshaler) error
	Decode(resp.Unmarshaler) error

	// The underlying connection's methods are exposed, all may be used except
	// Read and Write. When Close is called Encode and Decode will return an
	// error on all future calls.
	net.Conn
}

// TODO connClient is weird, if Conn was a Client also that'd be hella
// convenient and make more sense

type connClient struct {
	Conn
}

// ConnClient wraps a Conn so it may be directly used as a Client
func ConnClient(conn Conn) Client {
	return connClient{conn}
}

// Do implements the method for the Client interface, directly passing in the
// underlying Conn into the Action's Run method
func (cc connClient) Do(a Action) error {
	return a.Run(cc.Conn)
}

type connWrap struct {
	net.Conn
	brw *bufio.ReadWriter
}

// NewConn takes an existing net.Conn and wraps it to support the Conn interface
// of this package. The Read and Write methods on the original net.Conn should
// not be used after calling this method.
//
// In both the Encode and Decode methods of the returned Conn, if a net.Error is
// encountered the Conn will have Close called on it automatically.
func NewConn(conn net.Conn) Conn {
	return connWrap{
		Conn: conn,
		brw:  bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
	}
}

func (cw connWrap) Encode(m resp.Marshaler) error {
	err := m.MarshalRESP(cw.brw)
	defer func() {
		if _, ok := err.(net.Error); ok {
			cw.Close()
		}
	}()

	if err != nil {
		return err
	}
	err = cw.brw.Flush()
	return err
}

func (cw connWrap) Decode(u resp.Unmarshaler) error {
	err := u.UnmarshalRESP(cw.brw.Reader)
	if _, ok := err.(net.Error); ok {
		cw.Close()
	}
	return err
}

// DialFunc is a function which returns an initialized, ready-to-be-used Conn.
// Functions like NewPool or NewCluster take in a DialFunc in order to allow for
// things like calls to AUTH on each new connection, setting timeouts, custom
// Conn implementations, etc...
type DialFunc func(network, addr string) (Conn, error)

// Dial creates a network connection using net.Dial and passes it into NewConn.
func Dial(network, addr string) (Conn, error) {
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return NewConn(c), nil
}

type timeoutConn struct {
	net.Conn
	timeout time.Duration
}

func (tc *timeoutConn) setDeadline() {
	if tc.timeout > 0 {
		tc.Conn.SetDeadline(time.Now().Add(tc.timeout))
	}
}

func (tc *timeoutConn) Read(b []byte) (int, error) {
	tc.setDeadline()
	return tc.Conn.Read(b)
}

func (tc *timeoutConn) Write(b []byte) (int, error) {
	tc.setDeadline()
	return tc.Conn.Write(b)
}

// DialTimeout is like Dial, but the given timeout is used to set read/write
// deadlines on all reads/writes
func DialTimeout(network, addr string, timeout time.Duration) (Conn, error) {
	c, err := net.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}
	return NewConn(&timeoutConn{Conn: c, timeout: timeout}), nil
}
