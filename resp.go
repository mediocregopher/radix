package radix

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// Resp represents a single response or message being sent to/from a redis
// server. Each Resp has a type (see RespType) and a value. Values can be
// retrieved using any of the casting methods on this type (e.g. Str)
type Resp struct {
	Err error

	respInt
	arr []Resp
}

// most of the encoding/decoding logic is actually in resp_int.go, this just
// puts it all together

// IOErr wraps an error to indicate that the error was a result of failing to
// read or write data, as opposed to an application layer error like WRONGTYPE
//
// TODO example of casting
type IOErr struct {
	error
}

// Timeout returns true if this IOErr is the result of a network timeout
func (ie IOErr) Timeout() bool {
	t, ok := ie.error.(*net.OpError)
	return ok && t.Timeout()
}

func ioErrResp(err error) Resp {
	return Resp{Err: IOErr{err}}
}

////////////////////////////////////////////////////////////////////////////////
// Reading/Writing

// NewResp takes the given value and interprets it into a Resp instance of the
// appropriate type.
func NewResp(m interface{}) Resp {
	rib := &respIntBuf{}
	rib.srcAny(m)
	return rib.dstResp()
}

// NewSimpleString returns a simple string type Resp for the given string.
// NewResp will always use a bulk string type when it encounters a string, so
// this function is given for the few times it's needed.
func NewSimpleString(s string) Resp {
	return Resp{respInt: respInt{
		riType: riSimpleStr,
		body:   []byte(s),
	}}
}

// RespReader is a wrapper around an io.Reader which will read Resp messages off
// of the io.Reader
type RespReader struct {
	br  *bufio.Reader
	rib respIntBuf
}

// NewRespReader creates and returns a new RespReader which will read from the
// given io.Reader. Once passed in the io.Reader shouldn't be read from by any
// other processes
func NewRespReader(r io.Reader) *RespReader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &RespReader{
		br:  br,
		rib: make(respIntBuf, 16),
	}
}

// Read will read a single Resp off of the underlying io.Reader. The Resp's Err
// field will be an IOErr if there was an error reading or parsing
func (rr *RespReader) Read() Resp {
	rr.rib.reset()
	if err := rr.rib.srcReader(rr.br); err != nil {
		return ioErrResp(err)
	}

	return rr.rib.dstResp()
}

// RespWriter is a wrapper around an io.Writer which will write Resp messages to
// the io.Reader
type RespWriter struct {
	bw          *bufio.Writer
	rib, ribCmd respIntBuf
}

// NewRespWriter creates and returns a new RespWriter which will write to the
// given io.Writer. Once passed in the io.Writer shouldn't be read from by any
// other processes
func NewRespWriter(w io.Writer) *RespWriter {
	bw, ok := w.(*bufio.Writer)
	if !ok {
		bw = bufio.NewWriter(w)
	}
	return &RespWriter{
		bw:     bw,
		rib:    make(respIntBuf, 16),
		ribCmd: make(respIntBuf, 16),
	}
}

// Write translates whatever is given into a Resp and writes the encoded form to
// the underlying io.Writer. See the Conn interface for more on how this does
// its conversions.
func (rw *RespWriter) Write(m interface{}) error {
	rw.rib.reset()
	rw.rib.srcAny(m)
	return rw.rib.dstWriter(rw.bw)
}

////////////////////////////////////////////////////////////////////////////////
// Casting

func (r Resp) typeStr() string {
	if _, ok := r.Err.(IOErr); ok {
		return "IOErr"
	}

	if r.isNil {
		return "Nil"
	}

	switch r.riType {
	case riAppErr:
		return "AppErr"
	case riSimpleStr, riBulkStr:
		return "Str"
	case riInt:
		return "Int"
	case riArray:
		return "Array"
	default:
		return "UNKNOWN"
	}
}

func castErr(r Resp, castTo string) error {
	return fmt.Errorf("could not convert %s resp to %s", r.typeStr(), castTo)
}

// Nil returns true if this is a Nil response. Only returns an error if the Err
// field is set.
func (r Resp) Nil() (bool, error) {
	return r.isNil, r.Err
}

// Bytes returns a byte slice representing the value of the Resp.
// * If the Resp is a string type it returns that.
// * If the Resp is a nil type it returns nil.
// * If the Resp's Err field is set it returns that.
func (r Resp) Bytes() ([]byte, error) {
	if r.isNil {
		return nil, nil
	} else if r.riType&riStr > 0 {
		return r.body, nil
	} else if r.Err != nil {
		return nil, r.Err
	}
	return nil, castErr(r, "string")
}

// Str is a wrapper around Bytes which returns the result as a string instead of
// a byte slice. If the byte slice from Bytes would be nil then empty string is
// returned.
func (r Resp) Str() (string, error) {
	b, err := r.Bytes()
	return string(b), err
}

// Int64 returns an int64 representing the value of the Resp.
// * If the Resp is an int type it returns the int
// * If the Resp is a string type it attempts to parse as base-10 int
// * If the Resp is a nil type it returns 0
// * If the Resp's Err field is set it returns that.
func (r Resp) Int64() (int64, error) {
	if r.isNil {
		return 0, nil
	} else if r.riType&(riStr|riInt) > 0 {
		return strconv.ParseInt(string(r.body), 10, 64)
	} else if r.Err != nil {
		return 0, r.Err
	}
	return 0, castErr(r, "int64")
}

// Int is a wrapper around Int64 which casts the return to an int
func (r Resp) Int() (int, error) {
	i, err := r.Int64()
	return int(i), err
}

// Float64 returns an int64 representing the value of the Resp.
// * If the Resp is a string type it attempts to parse as base-10 float64
// * If the Resp is an int type it casts to float64
// * If the Resp is a nil type it returns 0
// * If the Resp's Err field is set it returns that.
func (r Resp) Float64() (float64, error) {
	if r.isNil {
		return 0, nil
	} else if r.riType&(riStr|riInt) > 0 {
		return strconv.ParseFloat(string(r.body), 64)
	} else if r.Err != nil {
		return 0, r.Err
	}
	return 0, castErr(r, "float64")
}

// Array returns the Resp slice encompassed by this Resp.
// * If the Resp is an array it returns the slice of child Resps
// * If the Resp is a nil type it returns nil
// * If the Resp's Err field is set it returns that.
func (r Resp) Array() ([]Resp, error) {
	if r.isNil {
		return nil, nil
	} else if r.riType == riArray {
		return r.arr, nil
	} else if r.Err != nil {
		return nil, r.Err
	}
	return nil, castErr(r, "array")
}

// List is a wrapper around Array which returns the result as a list of strings,
// calling Str() on each Resp which Array returns. Any errors encountered are
// immediately returned.
func (r *Resp) List() ([]string, error) {
	arr, err := r.Array()
	if arr == nil || err != nil {
		return nil, err
	}

	ss := make([]string, len(arr))
	for i := range arr {
		if ss[i], err = arr[i].Str(); err != nil {
			return nil, err
		}
	}
	return ss, nil
}

// ListBytes is a wrapper around Array which returns the result as a list of
// byte slices, calling Bytes() on each Resp which Array returns. Any errors
// encountered are immediately returned.
func (r *Resp) ListBytes() ([][]byte, error) {
	arr, err := r.Array()
	if arr == nil || err != nil {
		return nil, err
	}

	bb := make([][]byte, len(arr))
	for i := range arr {
		if bb[i], err = arr[i].Bytes(); err != nil {
			return nil, err
		}
	}
	return bb, nil
}

// Map is a wrapper around Array which returns the result as a map of strings,
// calling Str() on alternating key/values for the map.
func (r *Resp) Map() (map[string]string, error) {
	arr, err := r.Array()
	if arr == nil || err != nil {
		return nil, err
	}

	if len(arr)%2 != 0 {
		return nil, errors.New("reply has odd number of elements")
	}

	m := make(map[string]string, len(arr)/2)
	for {
		if len(arr) == 0 {
			return m, nil
		}
		k, v := arr[0], arr[1]
		arr = arr[2:]

		ks, err := k.Str()
		if err != nil {
			return nil, err
		}

		vs, err := v.Str()
		if err != nil {
			return nil, err
		}

		m[ks] = vs
	}
}

// String returns a string representation of the Resp. This method is for
// debugging, use Str() for reading a Str reply
func (r *Resp) String() string {
	switch {
	case r.Err != nil:
		return fmt.Sprintf("Resp(%s %s)", r.typeStr(), r.Err)
	case r.isNil:
		return "Resp(Nil)"
	case r.riType&riStr > 0:
		return fmt.Sprintf("Resp(%s %q)", r.typeStr(), string(r.body))
	case r.riType&riInt > 0:
		return fmt.Sprintf("Resp(%s %s)", r.typeStr(), string(r.body))
	case r.riType&riArray > 0:
		kidsStr := make([]string, len(r.arr))
		for i := range r.arr {
			kidsStr[i] = r.arr[i].String()
		}
		return fmt.Sprintf("[]Resp{%s}", strings.Join(kidsStr, " "))
	default:
		return fmt.Sprintf("Resp(UNKNOWN)")
	}
}

////////////////////////////////////////////////////////////////////////////////
// Casting

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

// Resp returns the Resp which would be send to redis if this Cmd was to be
// written. See the Conn interface for more on how that conversion works.
func (c Cmd) Resp() Resp {
	rib := &respIntBuf{}
	rib.srcCmd(c)
	return rib.dstResp()
}
