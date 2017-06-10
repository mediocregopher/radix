package radix

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/mediocregopher/radix.v2/resp"
)

// Action can perform one or more tasks using a Conn
type Action interface {
	// Key returns a key which will be acted on. If the Action will act on more
	// than one key then any one can be returned. If no keys will be acted on
	// then nil should be returned.
	Key() []byte

	// Run actually performs the Action using the given Conn
	Run(c Conn) error
}

// CmdAction is a specific type of Action for which a command is marshaled and
// sent to the server and the command's response is read and unmarshaled into a
// receiver within the CmdAction.
//
// A CmdAction can be used like an Action, but it can also be used by marshaling
// the command and unmarshaling the response manually.
type CmdAction interface {
	Action
	resp.Marshaler
	resp.Unmarshaler
}

var noKeyCmds = map[string]bool{
	"SENTINEL": true,

	"CLUSTER":   true,
	"READONLY":  true,
	"READWRITE": true,
	"ASKING":    true,

	"AUTH":   true,
	"ECHO":   true,
	"PING":   true,
	"QUIT":   true,
	"SELECT": true,
	"SWAPDB": true,

	"KEYS":      true,
	"MIGRATE":   true,
	"OBJECT":    true,
	"RANDOMKEY": true,
	"WAIT":      true,
	"SCAN":      true,

	"EVAL":    true,
	"EVALSHA": true,
	"SCRIPT":  true,

	"BGREWRITEAOF": true,
	"BGSAVE":       true,
	"CLIENT":       true,
	"COMMAND":      true,
	"CONFIG":       true,
	"DBSIZE":       true,
	"DEBUG":        true,
	"FLUSHALL":     true,
	"FLUSHDB":      true,
	"INFO":         true,
	"LASTSAVE":     true,
	"MONITOR":      true,
	"ROLE":         true,
	"SAVE":         true,
	"SHUTDOWN":     true,
	"SLAVEOF":      true,
	"SLOWLOG":      true,
	"SYNC":         true,
	"TIME":         true,

	//"BITOPS": true, // TODO specially handle this?

	// TODO cluster needs to support WithConnForKey or something like that
	"DISCARD": true,
	"EXEC":    true,
	"MULTI":   true,
	"UNWATCH": true,
	"WATCH":   true,
}

func cmdString(m resp.Marshaler) string {
	// we go way out of the way here to display the command as it would be sent
	// to redis. This is pretty similar logic to what the stub does as well
	buf := new(bytes.Buffer)
	if err := m.MarshalRESP(buf); err != nil {
		return fmt.Sprintf("error creating string: %q", err.Error())
	}
	var ss []string
	err := resp.RawMessage(buf.Bytes()).UnmarshalInto(resp.Any{I: &ss})
	if err != nil {
		return fmt.Sprintf("error creating string: %q", err.Error())
	}
	for i := range ss {
		ss[i] = strconv.QuoteToASCII(ss[i])
	}
	return "[" + strings.Join(ss, " ") + "]"
}

////////////////////////////////////////////////////////////////////////////////

type cmdAction struct {
	rcv  interface{}
	cmd  string
	args []string
}

// Cmd TODO needs docs
func Cmd(rcv interface{}, cmd string, args ...string) CmdAction {
	return &cmdAction{
		rcv:  rcv,
		cmd:  cmd,
		args: args,
	}
}

func (c *cmdAction) Key() []byte {
	if noKeyCmds[strings.ToUpper(c.cmd)] || len(c.args) == 0 {
		return nil
	}
	return []byte(c.args[0])
}

func (c *cmdAction) MarshalRESP(w io.Writer) error {
	if err := (resp.ArrayHeader{N: len(c.args) + 1}).MarshalRESP(w); err != nil {
		return err
	} else if err := (resp.BulkStringStr{S: c.cmd}).MarshalRESP(w); err != nil {
		return err
	}
	for i := range c.args {
		if err := (resp.BulkStringStr{S: c.args[i]}).MarshalRESP(w); err != nil {
			return err
		}
	}
	return nil
}

func (c *cmdAction) UnmarshalRESP(br *bufio.Reader) error {
	return resp.Any{I: c.rcv}.UnmarshalRESP(br)
}

func (c *cmdAction) Run(conn Conn) error {
	if err := conn.Encode(c); err != nil {
		return err
	}
	return conn.Decode(c)
}

func (c *cmdAction) String() string {
	return cmdString(c)
}

////////////////////////////////////////////////////////////////////////////////

type flatCmdAction struct {
	rcv      interface{}
	cmd, key string
	args     []interface{}
}

// FlatCmd TODO needs docs
func FlatCmd(rcv interface{}, cmd, key string, args ...interface{}) CmdAction {
	return &flatCmdAction{
		rcv:  rcv,
		cmd:  cmd,
		key:  key,
		args: args,
	}
}

func (c *flatCmdAction) Key() []byte {
	return []byte(c.key)
}

func (c *flatCmdAction) MarshalRESP(w io.Writer) error {
	var err error
	marshal := func(m resp.Marshaler) {
		if err == nil {
			err = m.MarshalRESP(w)
		}
	}

	a := resp.Any{
		I:                     c.args,
		MarshalBulkString:     true,
		MarshalNoArrayHeaders: true,
	}
	arrL := 2 + a.NumElems()
	marshal(resp.ArrayHeader{N: arrL})
	marshal(resp.BulkStringStr{S: c.cmd})
	marshal(resp.BulkStringStr{S: c.key})
	marshal(a)
	return err
}

func (c *flatCmdAction) UnmarshalRESP(br *bufio.Reader) error {
	return resp.Any{I: c.rcv}.UnmarshalRESP(br)
}

func (c *flatCmdAction) Run(conn Conn) error {
	if err := conn.Encode(c); err != nil {
		return err
	}
	return conn.Decode(c)
}

func (c *flatCmdAction) String() string {
	return cmdString(c)
}

////////////////////////////////////////////////////////////////////////////////

var (
	evalsha = []byte("EVALSHA")
	eval    = []byte("EVAL")
)

type lua struct {
	script string
	keys   []string
	args   []interface{}
	rcv    interface{}

	eval bool
}

// Lua TODO docs
func Lua(rcv interface{}, script string, keys []string, args ...interface{}) Action {
	return lua{
		script: script,
		keys:   keys,
		args:   args,
		rcv:    rcv,
	}
}

// Key implements the Key method of the Action interface.
func (lc lua) Key() []byte {
	if len(lc.keys) == 0 {
		return nil
	}
	return []byte(lc.keys[0])
}

func (lc lua) MarshalRESP(w io.Writer) error {
	var err error
	marshal := func(m resp.Marshaler) {
		if err != nil {
			return
		}
		err = m.MarshalRESP(w)
	}

	a := resp.Any{
		I:                     lc.args,
		MarshalBulkString:     true,
		MarshalNoArrayHeaders: true,
	}
	numKeys := len(lc.keys)

	// EVAL(SHA) script/sum numkeys keys... args...
	marshal(resp.ArrayHeader{N: 3 + numKeys + a.NumElems()})
	if lc.eval {
		marshal(resp.BulkString{B: eval})
		marshal(resp.BulkString{B: []byte(lc.script)})
	} else {
		sumRaw := sha1.Sum([]byte(lc.script))
		sum := hex.EncodeToString(sumRaw[:])
		marshal(resp.BulkString{B: evalsha})
		marshal(resp.BulkString{B: []byte(sum)})
	}
	marshal(resp.Any{I: numKeys, MarshalBulkString: true})
	for _, k := range lc.keys {
		marshal(resp.BulkString{B: []byte(k)})
	}
	marshal(a)
	return err
}

func (lc lua) Run(conn Conn) error {
	run := func(eval bool) error {
		lc.eval = eval
		if err := conn.Encode(lc); err != nil {
			return err
		}
		return conn.Decode(resp.Any{I: lc.rcv})
	}

	err := run(false)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		err = run(true)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////

type pipeline []CmdAction

// Pipeline returns an Action which first writes multiple commands to a Conn in
// a single write, then reads their responses in a single read. This reduces
// network delay into a single round-trip.
//
//	var fooVal string
//	p := Pipeline(
//		Cmd(nil, "SET", "foo", "bar"),
//		Cmd(&fooVal, "GET", "foo"),
//	)
//	if err := conn.Do(p); err != nil {
//		panic(err)
//	}
//	fmt.Printf("fooVal: %q\n", fooVal)
//
func Pipeline(cmds ...CmdAction) Action {
	return pipeline(cmds)
}

func (p pipeline) Key() []byte {
	for _, rc := range p {
		if k := rc.Key(); k != nil {
			return k
		}
	}
	return nil
}

func (p pipeline) Run(c Conn) error {
	for _, cmd := range p {
		if err := c.Encode(cmd); err != nil {
			return err
		}
	}
	for _, cmd := range p {
		if err := c.Decode(cmd); err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type withConn struct {
	key []byte
	fn  func(Conn) error
}

// WithConn is used to perform a set of independent Actions on the same Conn.
// key should be a key which one or more of the inner Actions is acting on, or
// nil if no keys are being acted on. The callback function is what should
// actually carry out the inner actions, and the error it returns will be
// returned by the Run method.
//
//	err := pool.Do(WithConn("someKey", func(conn Conn) error {
//		var curr int
//		if err := conn.Do(radix.Cmd(&curr, "GET", "someKey")); err != nil {
//			return err
//		}
//
//		curr++
//		return conn.Do(radix.Cmd(nil, "SET", "someKey", curr))
//	})
//
// NOTE that WithConn only ensures all inner Actions are performed on the same
// Conn, it doesn't make them transactional. Use MULTI/WATCH/EXEC within a
// WithConn or Pipeline for transactions, or use LuaCmd.
func WithConn(key []byte, fn func(Conn) error) Action {
	// TODO don't like that key is []byte here, string would be better
	return withConn{[]byte(key), fn}
}

func (wc withConn) Key() []byte {
	return wc.key
}

func (wc withConn) Run(c Conn) error {
	return wc.fn(c)
}
