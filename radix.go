// Package radix is a simple redis driver. It needs better docs
package radix

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"net"
	"strings"
	"time"
)

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
	Encoder
	Decoder
}

// NewConn takes an existing io.ReadWriteCloser and wraps it to support the Conn
// interface. The original io.ReadWriteCloser should not be used after calling
// this.
func NewConn(rwc io.ReadWriteCloser) Conn {
	return rwcWrap{
		rwc:     rwc,
		Encoder: NewEncoder(rwc),
		Decoder: NewDecoder(rwc),
	}
}

func (rwc rwcWrap) Close() error {
	return rwc.rwc.Close()
}

// DialFunc is a function which returns an initialized, ready-to-be-used Conn.
// Functions like NewPool or NewCluster take in a DialFunc in order to allow for
// things like calls to AUTH on each new connection, setting timeouts, custom
// Conn implementations, etc...
type DialFunc func(network, addr string) (Conn, error)

// Dial creates a network connection using net.Dial and wraps it to support the
// Conn interface.
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

func newCmdLegacy(cmd string, args ...interface{}) Cmd {
	cc := Cmd{}.C(cmd)
	for _, a := range args {
		cc = cc.A(a)
	}
	return cc
}

// ConnCmd writes the given command to the Conn, and unmarshals the result into
// the res variable (which should be a pointer to something). It calls Close on
// the Conn if any errors occur.
//
// See the Cmd docs for more on how Cmds are marshalled, see the Decoder docs
// for more on how results are unmarshalled.
func ConnCmd(c Conn, res interface{}, cmd Cmd) error {
	// TODO this whole function needs to be refactored or it needs to go away
	if err := c.Encode(cmd); err != nil {
		c.Close()
		return err
	} else if err := c.Decode(res); err != nil {
		c.Close()
		return err
	}
	return nil
}

// Pipeline is used to write multiple commands to a Conn in a single operation,
// and then read off all of their responses in a single operation, reducing
// round-trip time. When Append is called the command and value to decode its
// response into are stored until Run is called, and then all Cmd's appended
// will be performed sequentially in one round-trip.
//
// A single Pipeline can be used multiple times, or only a single time. It is
// very cheap to create a pipeline.
//
//  var fooVal string
//	p := radix.Pipeline{Conn: c}
//	p.Append(nil, radix.Cmd{}.C("SET").K("foo").A("bar"))
//	p.Append(&fooVal, radix.Cmd{}.C("GET").K("foo"))
//
//	if err := p.Run(); err != nil {
//		panic(err)
//	}
//	fmt.Printf("fooVal: %q\n", fooVal)
//
type Pipeline struct {
	Conn

	cmds []struct {
		res interface{}
		cmd Cmd
	}
}

// Append does not actually perform the command, but buffers it till Run is
// called.
func (p *Pipeline) Append(res interface{}, cmd Cmd) {
	p.cmds = append(p.cmds, struct {
		res interface{}
		cmd Cmd
	}{
		res: res,
		cmd: cmd,
	})
}

// Run will actually run all commands buffered by calls to Cmd so far, and
// decode their responses into their given receivers. If a network error is
// encountered the Conn will be Close'd and the error returned immediately.
//
// Reset is automatically called after each run.
func (p *Pipeline) Run() error {
	defer p.Reset()
	cmds := p.cmds
	for _, c := range cmds {
		if err := p.Conn.Encode(c.cmd); err != nil {
			p.Conn.Close()
			return err
		}
	}

	for _, c := range cmds {
		if err := p.Conn.Decode(c.res); err != nil {
			p.Conn.Close()
			return err
		}
	}

	return nil
}

// Reset undoes all Append calls that have been done so far on the Pipeline,
// effectively making it as if it was just initialized.
func (p *Pipeline) Reset() {
	p.cmds = p.cmds[:0]
}

// LuaEval calls the EVAL command on the given Conn for the given script,
// passing the key count and argument list in as well. See
// http://redis.io/commands/eval for more on how EVAL works and for the meaning
// of the keys argument.
//
// LuaEval will automatically try to call EVALSHA first in order to preserve
// bandwidth, and only falls back on EVAL if the script has never been used
// before.
func LuaEval(c Conn, res interface{}, script string, keys int, args ...interface{}) error {
	sumRaw := sha1.Sum([]byte(script))
	sum := hex.EncodeToString(sumRaw[:])

	cmd := Cmd{}.C("EVALSHA").A(sum).A(keys).A(args)
	err := ConnCmd(c, res, cmd)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		cmd = cmd.Reset().C("EVAL").A(script).A(keys).A(args)
		err = ConnCmd(c, res, cmd)
	}
	return err
}
