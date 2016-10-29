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
	riSimpleStr = iota
	riBulkStr
	riAppErr // An error returned by redis, e.g. WRONGTYPE
	riInt
	riArray
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

// FirstArg returns the first argument (not the Cmd) in this Cmd. If there are
// none it will return empty string.
//
// Since Cmd will automatically flatten all arguments into a flat slice of
// strings during encoding it's not that easy to determine what the first
// argument will be, so this method is provided as a convenience. In the case
// where Args contains a map this command may return something different on
// every call.
func (c Cmd) FirstArg() string {
	// TODO
	return ""
}

// TODO I'm not super thrilled about this Resp type

// Resp can be used to encode or decode exact values of the resp protocol (the
// network protocol that redis uses). When encoding, the first non-nil field (or
// one of the nil booleans) will be used as the resp value. When decoding the
// value being read will be filled into the corresponding field based on its
// type, the others being left nil.
type Resp struct {
	SimpleStr  *string
	BulkStr    []byte
	Err        *AppErr
	Int        *int64
	Arr        []Resp
	BulkStrNil bool
	ArrNil     bool
}
