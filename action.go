package radix

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strings"

	"github.com/mediocregopher/radix.v2/resp"
)

// Action is an entity which can perform one or more tasks using a Conn
type Action interface {
	// OnKey returns a key which will be acted on. If the Action will act on
	// more than one key then any one can be returned. If no keys will be acted
	// on then nil should be returned.
	OnKey() []byte

	// Run actually performs the action using the given Conn
	Run(c Conn) error
}

// RawCmd implements the Action interface and describes a single redis command
// to be performed.
type RawCmd struct {
	// The name of the redis command to be performed. Always required
	Cmd []byte

	// The key being operated on. May be left nil if the command doesn't operate
	// on any specific key (e.g.  SCAN)
	Key []byte

	// Args are any extra arguments to the command and can be almost any thing
	// TODO more deets
	Args []interface{}

	// Pointer value into which results from the command will be unmarshalled.
	// The Into method can be used to set this as well. See the Decoder docs
	// for more on unmarshalling
	Rcv interface{}
}

// Cmd returns an initialized RawCmd, populating the fields with the given
// values. Use CmdNoKey for commands which don't have an actual key (e.g. MULTI
// or PING). You can chain the Into method to conveniently set a result
// receiver.
func Cmd(cmd, key string, args ...interface{}) RawCmd {
	return RawCmd{
		Cmd:  []byte(cmd),
		Key:  []byte(key),
		Args: args,
	}
}

// CmdNoKey is like Cmd, but the returned RawCmd will not have its Key field set
func CmdNoKey(cmd string, args ...interface{}) RawCmd {
	return RawCmd{
		Cmd:  []byte(cmd),
		Args: args,
	}
}

// Into returns a RawCmd with all the same fields as the original, except the
// Rcv field set to the given value.
func (rc RawCmd) Into(rcv interface{}) RawCmd {
	rc.Rcv = rcv
	return rc
}

// OnKey implements the OnKey method of the Action interface.
func (rc RawCmd) OnKey() []byte {
	return rc.Key
}

// MarshalRESP implements the resp.Marshaler interface.
// TODO describe how commands are written
func (rc RawCmd) MarshalRESP(p *resp.Pool, w io.Writer) error {
	var err error
	marshal := func(m resp.Marshaler) {
		if err == nil {
			err = m.MarshalRESP(p, w)
		}
	}

	a := resp.Any{
		I:                     rc.Args,
		MarshalBulkString:     true,
		MarshalNoArrayHeaders: true,
	}
	arrL := 1 + a.NumElems()
	if rc.Key != nil {
		arrL++
	}
	marshal(resp.ArrayHeader{N: arrL})
	marshal(resp.BulkString{B: rc.Cmd})
	if rc.Key != nil {
		marshal(resp.BulkString{B: rc.Key})
	}
	marshal(a)
	return err
}

// Run implements the Run method of the Action interface. It writes the RawCmd
// to the Conn, and unmarshals the result into the Rcv field (if set).
func (rc RawCmd) Run(conn Conn) error {
	if err := conn.Encode(rc); err != nil {
		return err
	}

	// Any will discard the data if its I is nil
	return conn.Decode(resp.Any{I: rc.Rcv})
}

// TODO RawCmd.String() would be convenient

////////////////////////////////////////////////////////////////////////////////

var (
	evalsha = []byte("EVALSHA")
	eval    = []byte("EVAL")
)

// RawLuaCmd is an Action similar to RawCmd, but it runs a lua script on the
// redis server instead of a single Cmd. See redis' EVAL docs for more on how
// that works.
type RawLuaCmd struct {
	// The actual lua script which will be run.
	Script string

	// The keys being operated on, and may be left empty if the command doesn't
	// operate on any specific key(s)
	Keys []string

	// Args are any extra arguments to the command and can be almost any thing
	// TODO more deets
	Args []interface{}

	// Pointer value into which results from the command will be unmarshalled.
	// The Into method can be used to set this as well. See the Decoder docs
	// for more on unmarshalling
	Rcv interface{}
}

// LuaCmd returns an initialized RawLuraCmd, populating the fields with the given
// values. You can chain the Into method to conveniently set a result receiver.
func LuaCmd(script string, keys []string, args ...interface{}) RawLuaCmd {
	return RawLuaCmd{
		Script: script,
		Keys:   keys,
		Args:   args,
	}
}

// Into returns a RawLuaCmd with all the same fields as the original, except the
// Rcv field set to the given value.
func (rlc RawLuaCmd) Into(rcv interface{}) RawLuaCmd {
	rlc.Rcv = rcv
	return rlc
}

// OnKey implements the OnKey method of the Action interface.
func (rlc RawLuaCmd) OnKey() []byte {
	if len(rlc.Keys) == 0 {
		return nil
	}
	return []byte(rlc.Keys[0])
}

type mRawLuaCmd struct {
	RawLuaCmd
	eval bool
}

func (mrlc mRawLuaCmd) MarshalRESP(p *resp.Pool, w io.Writer) error {
	var err error
	marshal := func(m resp.Marshaler) {
		if err != nil {
			return
		}
		err = m.MarshalRESP(p, w)
	}

	a := resp.Any{
		I:                     mrlc.Args,
		MarshalBulkString:     true,
		MarshalNoArrayHeaders: true,
	}
	numKeys := len(mrlc.Keys)

	// EVAL(SHA) script/sum numkeys keys... args...
	marshal(resp.ArrayHeader{N: 3 + numKeys + a.NumElems()})
	if mrlc.eval {
		marshal(resp.BulkString{B: eval})
		marshal(resp.BulkString{B: []byte(mrlc.Script)})
	} else {
		// TODO alloc here isn't great
		sumRaw := sha1.Sum([]byte(mrlc.Script))
		sum := hex.EncodeToString(sumRaw[:])
		marshal(resp.BulkString{B: evalsha})
		marshal(resp.BulkString{B: []byte(sum)})
	}
	marshal(resp.Any{I: numKeys, MarshalBulkString: true})
	for _, k := range mrlc.Keys {
		marshal(resp.BulkString{B: []byte(k)})
	}
	marshal(a)
	return err
}

func (mrlc mRawLuaCmd) Run(conn Conn) error {
	if err := conn.Encode(mrlc); err != nil {
		return err
	}
	return conn.Decode(resp.Any{I: mrlc.Rcv})
}

// Run implements the Run method of the Action interface. It will first attempt
// to perform the command using an EVALSHA, but will fallback to a normal EVAL
// if that doesn't work.
func (rlc RawLuaCmd) Run(conn Conn) error {
	err := mRawLuaCmd{RawLuaCmd: rlc}.Run(conn)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		err = mRawLuaCmd{RawLuaCmd: rlc, eval: true}.Run(conn)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////

type pipeline []RawCmd

// Pipeline returns an Action which first writes multiple commands to a Conn in
// a single write, then reads their responses in a single read. This reduces
// network delay into a single round-trip.
//
//	var fooVal string
//	p := Pipeline(
//		Cmd("SET", "foo", "bar"),
//		Cmd("GET", "foo").Into(&fooVal),
//	)
//	if err := p.Run(conn); err != nil {
//		panic(err)
//	}
//	fmt.Printf("fooVal: %q\n", fooVal)
//
func Pipeline(cmds ...RawCmd) Action {
	return pipeline(cmds)
}

func (p pipeline) OnKey() []byte {
	for _, rc := range p {
		if k := rc.OnKey(); k != nil {
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
		if err := c.Decode(resp.Any{I: cmd.Rcv}); err != nil {
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
//		if err := Cmd("GET", "someKey").Into(&curr).Run(conn); err != nil {
//			return err
//		}
//
//		curr++
//		return Cmd("SET", "someKey", curr).Run(conn)
//	})
//
// NOTE that WithConn only ensures all inner Actions are performed on the same
// Conn, it doesn't make them transactional. Use MULTI/WATCH/EXEC within a
// WithConn or Pipeline for transactions, or use LuaCmd.
func WithConn(key []byte, fn func(Conn) error) Action {
	// TODO don't like that key is []byte here, string would be better
	return withConn{[]byte(key), fn}
}

func (wc withConn) OnKey() []byte {
	return wc.key
}

func (wc withConn) Run(c Conn) error {
	return wc.fn(c)
}
