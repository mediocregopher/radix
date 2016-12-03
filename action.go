package radix

import (
	"crypto/sha1"
	"encoding/hex"
	"strings"
)

// Action is an entity which can perform one or more tasks using a Conn
type Action interface {
	// Key returns a key which will be acted on. If the Action will act on more
	// than one key than any one can be returned. If no keys will be acted on
	// then nil should be returned.
	Key() []byte

	// Run actually performs the action using the given Conn
	Run(c Conn) error
}

// RawCmd implements the Action interface and describes a single redis command
// to be performed.
type RawCmd struct {
	// The name of the redis command to be performed. Always required
	Cmd []byte

	// The keys being operated on, and may be left empty if the command doesn't
	// operate on any specific key(s) (e.g.  SCAN)
	Keys [][]byte

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
		Keys: [][]byte{[]byte(key)},
		Args: args,
	}
}

// CmdNoKey is like Cmd, but the returned RawCmd will not have its Keys field
// set.
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

// Key implements the Key method of the Action interface.
func (rc RawCmd) Key() []byte {
	if len(rc.Keys) == 0 {
		return nil
	}
	return rc.Keys[0]
}

// Run implements the Run method of the Action interface. It writes the RawCmd
// to the Conn, and unmarshals the result into the Rcv field (if set). It calls
// Close on the Conn if any errors occur.
func (rc RawCmd) Run(conn Conn) error {
	// TODO if the Marshaler interface handled low-level operations then RawCmd
	// wouldn't need a special case in the Encoder.
	if err := conn.Encode(rc); err != nil {
		conn.Close()
		return err
	} else if err := conn.Decode(rc.Rcv); err != nil {
		// TODO just make the rwcConn thing do this inside its Decode method and
		// document it there
		if !isAppErr(err) {
			conn.Close()
		}
		return err
	}
	return nil
}

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

// Key implements the Key method of the Action interface.
func (rlc RawLuaCmd) Key() []byte {
	if len(rlc.Keys) == 0 {
		return nil
	}
	return []byte(rlc.Keys[0])
}

// Run implements the Run method of the Action interface. It will first attempt
// to perform the command using an EVALSHA, but will fallback to a normal EVAL
// if that doesn't work.
func (rlc RawLuaCmd) Run(conn Conn) error {
	// TODO if the Marshaler interface handled low-level operations, like we need
	// here, better then we could probably get rid of all this weird slice
	// shifting nonsense.

	rc := RawCmd{
		Cmd:  evalsha,
		Args: rlc.Args,
		Rcv:  rlc.Rcv,
	}

	// TODO alloc here probably isn't necessary
	sumRaw := sha1.Sum([]byte(rlc.Script))
	sum := hex.EncodeToString(sumRaw[:])

	// shift the arguments in place to allow room for the script hash, key
	// count, and keys themselves
	// TODO this isn't great because if the Args are re-allocated we don't
	// actually give back to the original Cmd
	shiftBy := 2 + len(rlc.Keys)
	for i := 0; i < shiftBy; i++ {
		rc.Args = append(rc.Args, nil)
	}
	copy(rc.Args[shiftBy:], rc.Args)
	rc.Args[0] = sum
	rc.Args[1] = len(rlc.Keys)
	for i := range rlc.Keys {
		rc.Args[i+2] = rlc.Keys[i]
	}

	err := rc.Run(conn)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		rc.Cmd = eval
		rc.Args[0] = rlc.Script
		err = rc.Run(conn)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Pipeline is an Action which first writes multiple commands to a Conn in a
// single write, then reads their responses in a single step. This effectively
// reduces network delay into a single round-trip
//
//	var fooVal string
//	p := Pipeline{
//		Cmd("SET", "foo", "bar"),
//		Cmd("GET", "foo").Into(&fooVal),
//	}
//	if err := conn.Do(p); err != nil {
//		panic(err)
//	}
//	fmt.Printf("fooVal: %q\n", fooVal)
//
type Pipeline []RawCmd

// Run implements the method for the Action interface. It will write all RawCmds
// in it sequentially, then read all of their responses sequentially.
//
// If an error is encountered the error will be returned immediately.
func (p Pipeline) Run(c Conn) error {
	for _, cmd := range p {
		if err := c.Encode(cmd); err != nil {
			c.Close()
			return err
		}
	}

	for _, cmd := range p {
		if err := c.Decode(cmd.Rcv); err != nil {
			// TODO this isn't ideal, we could just make the rwcConn do this
			// always
			if !isAppErr(err) {
				c.Close()
			}
			return err
		}
	}

	return nil
}

// pipeConn is used by pipeline in order to "turn on" and "turn off" the
// Encode/Decode methods on a Conn
type pipeConn struct {
	Conn
	enc, dec bool
}

func (pc pipeConn) Encode(i interface{}) error {
	if pc.enc {
		return pc.Conn.Encode(i)
	}
	return nil
}

func (pc pipeConn) Decode(i interface{}) error {
	if pc.dec {
		return pc.Conn.Decode(i)
	}
	return nil
}
