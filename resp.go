package radix

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
