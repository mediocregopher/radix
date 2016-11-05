package radix

import "errors"

var (
	delim    = []byte{'\r', '\n'}
	delimEnd = delim[len(delim)-1]
	delimLen = len(delim)
)

var (
	simpleStrPrefix = []byte{'+'}
	errPrefix       = []byte{'-'}
	intPrefix       = []byte{':'}
	bulkStrPrefix   = []byte{'$'}
	arrayPrefix     = []byte{'*'}
	nilBulkStr      = []byte("$-1\r\n")
	nilArray        = []byte("*-1\r\n")
)

const (
	rSimpleStr = iota
	rBulkStr
	rAppErr // An error returned by redis, e.g. WRONGTYPE
	rInt
	rArray
)

// Cmd describes a single redis command to be performed. In general you won't
// have to use this directly, and instead can just use the Cmd method on most
// things. This is mostly useful for lower level operations.
type Cmd struct {
	Cmd  string
	Args []interface{}
}

// NewCmd is a convenient helper for creating Cmd structs
func NewCmd(cmd string, args ...interface{}) Cmd {
	return Cmd{
		Cmd:  cmd,
		Args: args,
	}
}

// used to short-circuit the walk in FirstArg
var errStop = errors.New("stahp")

// FirstArg returns the first argument to the command after flatterning and
// being converted to a string. This is useful for determining which key a
// command will operate on when the arguments make it ambiguous (like when
// they're slices or maps).
//
// If the first argument is map then a random key will be picked from it as the
// first argument.
//
// If the first argument is an Unmarshaler or LenReader then this will panic
func (c Cmd) FirstArg() string {
	return "TODO"
}

// Resp can be used to encode or decode exact values of the resp protocol (the
// network protocol that redis uses). When encoding, the first non-nil field (or
// one of the nil booleans) will be used as the resp value. When decoding the
// value being read will be filled into the corresponding field based on its
// type, the others being left nil.
//
// When all fields are their zero value (i.e. Resp{}) the Int field is the one
// used, and the Resp will encode/decode as an int resp of the value 0.
type Resp struct {
	SimpleStr  []byte
	BulkStr    []byte
	Err        error
	Arr        []Resp
	BulkStrNil bool
	ArrNil     bool

	Int int64
}

// Marshaler will be used by the Encoder when writing a type implementing it,
// the value returned by Marshal will be used instead of the value itself.
type Marshaler interface {
	Marshal() (interface{}, error)
}

// Unmarshaler will be used by the Decoder when reading into a type implementing
// it. The function given can be used to read the data into a separate temporary
// value first.
//
// TODO example
type Unmarshaler interface {
	Unmarshal(func(interface{}) error) error
}
