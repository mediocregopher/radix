// Package radix is a simple redis driver. It needs better docs
package radix

import (
	"io"
	"net"
	"sync"
	"time"
)

// Client describes an entity which can carry out Actions, e.g. a connection
// pool for a single redis instance or the cluster client.
type Client interface {
	Do(Action) error
	Close() error
}

// AppErr wraps the error type. It is used to indicate that the error being
// returned is an application level error (e.g. "WRONGTYPE" for a key) as
// opposed to a network level error (e.g. timeout or io.EOF)
//
// TODO example
type AppErr struct {
	Err error
}

func (ae AppErr) Error() string {
	return ae.Err.Error()
}

// LenReader adds an additional method to io.Reader, returning how many bytes
// are left till be read until an io.EOF is reached.
type LenReader interface {
	io.Reader
	Len() int
}

// Conn is an entity which reads/writes data using the redis resp protocol. The
// methods are synchronous. Encode and Decode may be called at the same time by
// two different go-routines, but each should only be called once at a time
// (i.e. two routines shouldn't call Encode at the same time, same with Decode).
type Conn interface {
	Encoder
	Decoder

	// Close closes the Conn and cleans up its resources. No methods may be
	// called after Close.
	Close() error
}

type rwcWrap struct {
	rwc io.ReadWriteCloser
	e   Encoder
	d   Decoder
	*sync.Once
}

// NewConn takes an existing io.ReadWriteCloser and wraps it to support the Conn
// interface. The original io.ReadWriteCloser should not be used after calling
// this.
//
// In both the Encode and Decode methods of the returned Conn, if a network
// error is encountered the Conn will have Close called on it automatically.
func NewConn(rwc io.ReadWriteCloser) Conn {
	return rwcWrap{
		rwc:  rwc,
		e:    NewEncoder(rwc),
		d:    NewDecoder(rwc),
		Once: new(sync.Once),
	}
}

func (rwc rwcWrap) Encode(i interface{}) error {
	err := rwc.e.Encode(i)
	if _, ok := err.(net.Error); ok {
		rwc.Close()
	}
	return err
}

func (rwc rwcWrap) Decode(i interface{}) error {
	err := rwc.d.Decode(i)
	if _, ok := err.(net.Error); ok {
		rwc.Close()
	}
	return err
}

func (rwc rwcWrap) Close() error {
	var err error
	rwc.Once.Do(func() {
		err = rwc.rwc.Close()
	})
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

type timeoutOkConn struct {
	Conn
}

// TimeoutOk returns a Conn which will _not_ call Close on itself if it sees a
// timeout error during a Decode call (though it will still return that error).
// It will still do so for all other read/write errors, however.
func TimeoutOk(c Conn) Conn {
	return &timeoutOkConn{c}
}

func (toc *timeoutOkConn) Decode(i interface{}) error {
	err := toc.Conn.Decode(timeoutOkUnmarshaler{i})
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
	i interface{}
}

type errTimeout struct {
	err error
}

func (et errTimeout) Error() string { return et.err.Error() }

func (tou timeoutOkUnmarshaler) Unmarshal(fn func(interface{}) error) error {
	err := fn(tou.i)
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		err = errTimeout{err}
	}
	return err
}
