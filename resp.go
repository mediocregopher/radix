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
// Errors returned from Unmarshal will automatically be wrapped in the
// UnmarshalErr type by the Decoder.
//
// TODO example
type Unmarshaler interface {
	Unmarshal(func(interface{}) error) error
}
