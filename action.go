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

// var c Action = Cmd(&rcv, "GET", args...)
//	* can't set Key
// var c Action = Cmd("GET", args...).Into(&rcv)
//	* also doesn't set Key
//	* too cute, not necessary
// var c Action = Cmd(&rcv, `GET ?`, arg)
//	* assume's first is key?
// var c Action = Cmd(&rcv, `GET key:?`, arg)
//	* Can't know key, can't flatten slices/maps
// var c Action = C("GET").K("fooKey").R(&fooVal)
//	* ugly as butt, bulky
//	* lot's of api surface area
//
// * None of these would have a nice Cmd type that could be manually populated
//   by speed freaks
//
// type CmdPart interface{ ... } // Key, Arg are CmdParts
// var c Action = Cmd(&rcv, "SET", Key("foo"), Arg("bar"))
//	* maybe confusing?
//	* more api surface area
//

// Cmd implements the Action interface and describes a single redis command to
// be performed. The Cmd field is the name of the redis command to be performend
// and is always required. Keys are the keys being operated on, and may be left
// empty if the command doesn't operate on any specific key(s) (e.g. SCAN). Args
// are any extra arguments to the command and can be almost any thing (TODO
// flesh that statement out).
//
// See the Decoder docs for more on how results are unmarshalled into Rcv.
//
// A Cmd can be filled in manually, or the shortcut methods may be used for more
// convenience. For example to set a key:
//
//	cmd := radix.Cmd{}.C("SET").K("fooKey").A("will be set to this string")
//
// And then to retrieve that key:
//
//	var fooVal string
//	cmd := radix.Cmd{}.C("GET").K("fooKey").R(&fooVal)
type Cmd struct {
	Cmd  []byte
	Keys [][]byte
	Args []interface{}
	Rcv  interface{}
}

// C (short for "Cmd") sets the Cmd field to the given string and returns the
// new Cmd object
func (c Cmd) C(cmd string) Cmd {
	c.Cmd = append(c.Cmd[:0], cmd...)
	return c
}

// K (short for "Key") appends the given key to the Keys field and returns the
// new Cmd object
func (c Cmd) K(key string) Cmd {
	if len(c.Keys) == cap(c.Keys) {
		// if keys is full we'll just have to bite the bullet and allocate the
		// memory
		c.Keys = append(c.Keys, []byte(key))
		return c
	}
	i := len(c.Keys)
	c.Keys = c.Keys[:i+1]
	c.Keys[i] = append(c.Keys[i][:0], key...)
	return c
}

// Key implements the Key method for that Action interface.
func (c Cmd) Key() []byte {
	if len(c.Keys) == 0 {
		return nil
	}
	return c.Keys[0]
}

// A (short for "Arg") appends the given argument to the Args slice and returns
// the new Cmd object.
func (c Cmd) A(arg interface{}) Cmd {
	c.Args = append(c.Args, arg)
	return c
}

// R (short for "Rcv") sets the Rcv field to the given receiver (which should be
// a pointer) and returns the new Cmd object. The receiver is what the response
// to the Cmd is unmarshalled into.
//
// If Rcv isn't set then the command's return value will be discarded.
func (c Cmd) R(v interface{}) Cmd {
	c.Rcv = v
	return c
}

// Reset can be used to reuse a Cmd object. In cases where memory allocations
// are a concern this allows the user of radix to conserve them easily, either
// by always using the same Cmd object within a go-routine or by creating a Cmd
// pool.
//
// The returned Cmd object can be used as if it was just instantiated.
func (c Cmd) Reset() Cmd {
	c.Cmd = c.Cmd[:0]
	c.Keys = c.Keys[:0]
	c.Args = c.Args[:0]
	c.Rcv = nil
	return c
}

// Run implements the Run method of the Action interface. It writes the Cmd to
// the Conn, and unmarshals the result into the Rcv field (if set). It calls
// Close on the Conn if any errors occur.
func (c Cmd) Run(conn Conn) error {
	if err := conn.Encode(c); err != nil {
		conn.Close()
		return err
	} else if err := conn.Decode(c.Rcv); err != nil {
		if !isAppErr(err) {
			conn.Close()
		}
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// TODO this isn't really all that clean. A lot of the difficulty stems from how
// Cmds are created (using the chaining thing). If they could be created in a
// saner way it'd be easier to have a LuaCmd type or something that mirrors them
// without adding a ton of API surface area.

// Lua TODO
func (c Cmd) Lua(script string) Action {
	// TODO alloc here probably isn't necessary
	sumRaw := sha1.Sum([]byte(script))
	sum := hex.EncodeToString(sumRaw[:])

	// shift the arguments in place to allow room for the script hash, key
	// count, and keys themselves
	// TODO this isn't great because if the Args are re-allocated we don't
	// actually give back to the original Cmd
	shiftBy := 2 + len(c.Keys)
	for i := 0; i < shiftBy; i++ {
		c.Args = append(c.Args, nil)
	}
	copy(c.Args[shiftBy:], c.Args)
	c.Args[0] = sum
	c.Args[1] = len(c.Keys)
	for i := range c.Keys {
		c.Args[i+2] = c.Keys[i]
	}

	return luaCmd{
		Cmd: Cmd{
			Cmd:  evalsha,
			Keys: c.Keys,
			Args: c.Args,
			Rcv:  c.Rcv,
		},
		script: script,
	}
}

type luaCmd struct {
	Cmd
	script string
}

var (
	evalsha = []byte("EVALSHA")
	eval    = []byte("EVAL")
)

func (l luaCmd) Run(conn Conn) error {
	cmd := l.Cmd
	cmd.Keys = nil
	err := cmd.Run(conn)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		cmd.Cmd = eval
		cmd.Args[0] = l.script
		err = cmd.Run(conn)
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Pipeline is an Action used to write multiple commands to a Conn in a single
// operation, and then read off all of their responses in a single operation,
// reducing round-trip time.
//
//	var fooVal string
//	p := Pipeline{
//		radix.Cmd{}.C("SET").K("foo").A("bar"),
//		radix.Cmd{}.C("GET").K("foo").R(&fooVal),
//	}
//
//	if err := conn.Do(p); err != nil {
//		panic(err)
//	}
//	fmt.Printf("fooVal: %q\n", fooVal)
//
type Pipeline []Cmd

// Run implements the method for the Action interface. It will actually run all
// commands buffered by calls to Cmd so far and decode their responses into
// their receivers. If a network error is encountered the Conn will be Close'd
// and the error returned immediately.
func (p Pipeline) Run(c Conn) error {
	for _, cmd := range p {
		if err := c.Encode(cmd); err != nil {
			c.Close()
			return err
		}
	}

	for _, cmd := range p {
		if err := c.Decode(cmd.Rcv); err != nil {
			// TODO this isn't ideal
			if !isAppErr(err) {
				c.Close()
			}
			return err
		}
	}

	return nil
}
