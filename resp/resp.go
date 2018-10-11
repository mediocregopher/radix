// Package resp implements the redis RESP protocol, a plaintext protocol which
// is also binary safe. Redis uses the RESP protocol to communicate with its
// clients, but there's nothing about the protocol which ties it to redis, it
// could be used for almost anything.
//
// See https://redis.io/topics/protocol for more details on the protocol.
package resp

import (
	"bufio"
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strconv"
	"sync"
)

var delim = []byte{'\r', '\n'}

var (
	simpleStrPrefix = []byte{'+'}
	errPrefix       = []byte{'-'}
	intPrefix       = []byte{':'}
	bulkStrPrefix   = []byte{'$'}
	arrayPrefix     = []byte{'*'}
	nilBulkString   = []byte("$-1\r\n")
	nilArray        = []byte("*-1\r\n")
)

var bools = [][]byte{
	{'0'},
	{'1'},
}

func anyIntToInt64(m interface{}) int64 {
	switch mt := m.(type) {
	case int:
		return int64(mt)
	case int8:
		return int64(mt)
	case int16:
		return int64(mt)
	case int32:
		return int64(mt)
	case int64:
		return mt
	case uint:
		return int64(mt)
	case uint8:
		return int64(mt)
	case uint16:
		return int64(mt)
	case uint32:
		return int64(mt)
	case uint64:
		return int64(mt)
	}
	panic(fmt.Sprintf("anyIntToInt64 got bad arg: %#v", m))
}

var bytePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 64)
		return &b
	},
}

func getBytes() *[]byte {
	return bytePool.Get().(*[]byte)
}

func putBytes(b *[]byte) {
	(*b) = (*b)[:0]
	bytePool.Put(b)
}

// parseInt is a specialized version of strconv.ParseInt that parses a base-10 encoded signed integer.
func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty slice given to parseInt")
	}

	var neg bool
	if b[0] == '-' || b[0] == '+' {
		neg = b[0] == '-'
		b = b[1:]
	}

	n, err := parseUint(b)
	if err != nil {
		return 0, err
	}

	if neg {
		return -int64(n), nil
	}

	return int64(n), nil
}

// parseUint is a specialized version of strconv.ParseUint that parses a base-10 encoded integer
func parseUint(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty slice given to parseUint")
	}

	var n uint64

	for i, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid character %c at position %d in parseUint", c, i)
		}

		n *= 10
		n += uint64(c - '0')
	}

	return n, nil
}

// Marshaler is the interface implemented by types that can marshal themselves
// into valid RESP.
type Marshaler interface {
	MarshalRESP(io.Writer) error
}

// Unmarshaler is the interface implemented by types that can unmarshal a RESP
// description of themselves.
//
// Note that, unlike Marshaler, Unmarshaler _must_ take in a *bufio.Reader.
type Unmarshaler interface {
	UnmarshalRESP(*bufio.Reader) error
}

////////////////////////////////////////////////////////////////////////////////

func expand(b []byte, to int) []byte {
	if cap(b) < to {
		nb := make([]byte, to)
		copy(nb, b)
		return nb
	}
	return b[:to]
}

// effectively an assert that the reader data starts with the given slice,
// discarding the slice at the same time
func bufferedPrefix(br *bufio.Reader, prefix []byte) error {
	b, err := br.ReadSlice(prefix[len(prefix)-1])
	if err != nil {
		return err
	} else if !bytes.Equal(b, prefix) {
		return fmt.Errorf("expected prefix %q, got %q", prefix, b)
	}
	return nil
}

// reads bytes up to a delim and returns them, or an error
func bufferedBytesDelim(br *bufio.Reader) ([]byte, error) {
	b, err := br.ReadSlice('\n')
	if err != nil {
		return nil, err
	} else if len(b) < 2 {
		return nil, fmt.Errorf("malformed resp %q", b)
	}
	return b[:len(b)-2], err
}

// reads an integer out of the buffer, followed by a delim. It parses the
// integer, or returns an error
func bufferedIntDelim(br *bufio.Reader) (int64, error) {
	b, err := bufferedBytesDelim(br)
	if err != nil {
		return 0, err
	}
	return parseInt(b)
}

// readNAppend appends exactly n bytes from r into b.
func readNAppend(r io.Reader, b []byte, n int) ([]byte, error) {
	if n == 0 {
		return b, nil
	}
	m := len(b)
	b = expand(b, len(b)+n)
	_, err := io.ReadFull(r, b[m:])
	return b, err
}

func multiWrite(w io.Writer, bb ...[]byte) error {
	for _, b := range bb {
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func readInt(r io.Reader, n int) (int64, error) {
	scratch := getBytes()
	defer putBytes(scratch)

	var err error
	if *scratch, err = readNAppend(r, *scratch, n); err != nil {
		return 0, err
	}
	return parseInt(*scratch)
}

func readUint(r io.Reader, n int) (uint64, error) {
	scratch := getBytes()
	defer putBytes(scratch)

	var err error
	if *scratch, err = readNAppend(r, *scratch, n); err != nil {
		return 0, err
	}
	return parseUint(*scratch)
}

func readFloat(r io.Reader, precision, n int) (float64, error) {
	scratch := getBytes()
	defer putBytes(scratch)

	var err error
	if *scratch, err = readNAppend(r, *scratch, n); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(*scratch), precision)
}

////////////////////////////////////////////////////////////////////////////////

// SimpleString represents the simple string type in the RESP protocol
type SimpleString struct {
	S string
}

// MarshalRESP implements the Marshaler method
func (ss SimpleString) MarshalRESP(w io.Writer) error {
	return multiWrite(w, simpleStrPrefix, []byte(ss.S), delim)
}

// UnmarshalRESP implements the Unmarshaler method
func (ss *SimpleString) UnmarshalRESP(br *bufio.Reader) error {
	if err := bufferedPrefix(br, simpleStrPrefix); err != nil {
		return err
	}
	b, err := bufferedBytesDelim(br)
	if err != nil {
		return err
	}

	ss.S = string(b)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// Error represents an error type in the RESP protocol. Note that this only
// represents an actual error message being read/written on the stream, it is
// separate from network or parsing errors. An E value of nil is equivalent to
// an empty error string
type Error struct {
	E error
}

func (e Error) Error() string {
	return e.E.Error()
}

// MarshalRESP implements the Marshaler method
func (e Error) MarshalRESP(w io.Writer) error {
	if e.E == nil {
		return multiWrite(w, errPrefix, delim)
	}
	scratch := getBytes()
	defer putBytes(scratch)
	*scratch = append(*scratch, e.E.Error()...)
	return multiWrite(w, errPrefix, *scratch, delim)
}

// UnmarshalRESP implements the Unmarshaler method
func (e *Error) UnmarshalRESP(br *bufio.Reader) error {
	if err := bufferedPrefix(br, errPrefix); err != nil {
		return err
	}
	b, err := bufferedBytesDelim(br)
	e.E = errors.New(string(b))
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Int represents an int type in the RESP protocol
type Int struct {
	I int64
}

// MarshalRESP implements the Marshaler method
func (i Int) MarshalRESP(w io.Writer) error {
	scratch := getBytes()
	defer putBytes(scratch)
	*scratch = strconv.AppendInt(*scratch, int64(i.I), 10)
	return multiWrite(w, intPrefix, *scratch, delim)
}

// UnmarshalRESP implements the Unmarshaler method
func (i *Int) UnmarshalRESP(br *bufio.Reader) error {
	if err := bufferedPrefix(br, intPrefix); err != nil {
		return err
	}
	n, err := bufferedIntDelim(br)
	i.I = n
	return err
}

////////////////////////////////////////////////////////////////////////////////

// BulkStringBytes represents the bulk string type in the RESP protocol using a
// go byte slice. A B value of nil indicates the nil bulk string message, versus
// a B value of []byte{} which indicates a bulk string of length 0.
type BulkStringBytes struct {
	B []byte

	// If true then this won't marshal the nil RESP value when B is nil, it will
	// marshal as an empty string instead
	MarshalNotNil bool
}

// MarshalRESP implements the Marshaler method
func (b BulkStringBytes) MarshalRESP(w io.Writer) error {
	if b.B == nil && !b.MarshalNotNil {
		return multiWrite(w, nilBulkString)
	}
	scratch := getBytes()
	defer putBytes(scratch)
	*scratch = strconv.AppendInt(*scratch, int64(len(b.B)), 10)
	return multiWrite(w, bulkStrPrefix, *scratch, delim, b.B, delim)
}

// UnmarshalRESP implements the Unmarshaler method
func (b *BulkStringBytes) UnmarshalRESP(br *bufio.Reader) error {
	if err := bufferedPrefix(br, bulkStrPrefix); err != nil {
		return err
	}
	n, err := bufferedIntDelim(br)
	nn := int(n)
	if err != nil {
		return err
	} else if n == -1 {
		b.B = nil
		return nil
	} else {
		b.B = expand(b.B, nn)
		if b.B == nil {
			b.B = []byte{}
		}
	}

	if _, err := io.ReadFull(br, b.B); err != nil {
		return err
	} else if _, err := bufferedBytesDelim(br); err != nil {
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BulkString represents the bulk string type in the RESP protocol using a go
// string.
type BulkString struct {
	S string
}

// MarshalRESP implements the Marshaler method
func (b BulkString) MarshalRESP(w io.Writer) error {
	scratch := getBytes()
	defer putBytes(scratch)
	*scratch = strconv.AppendInt(*scratch, int64(len(b.S)), 10)
	ll := len(*scratch)
	*scratch = append(*scratch, b.S...)
	return multiWrite(w, bulkStrPrefix, (*scratch)[:ll], delim, (*scratch)[ll:], delim)
}

// UnmarshalRESP implements the Unmarshaler method. This treats a Nil bulk
// string message as empty string.
func (b *BulkString) UnmarshalRESP(br *bufio.Reader) error {
	if err := bufferedPrefix(br, bulkStrPrefix); err != nil {
		return err
	}
	n, err := bufferedIntDelim(br)
	if err != nil {
		return err
	} else if n == -1 {
		b.S = ""
		return nil
	}

	scratch := getBytes()
	defer putBytes(scratch)
	*scratch = expand(*scratch, int(n))

	if _, err := io.ReadFull(br, *scratch); err != nil {
		return err
	} else if _, err := bufferedBytesDelim(br); err != nil {
		return err
	}

	b.S = string(*scratch)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// BulkReader is like BulkString, but it only supports marshalling and will use
// the given LenReader to do so. If LR is nil then the nil bulk string RESP will
// be written
type BulkReader struct {
	LR LenReader
}

// MarshalRESP implements the Marshaler method
func (b BulkReader) MarshalRESP(w io.Writer) error {
	if b.LR == nil {
		return multiWrite(w, nilBulkString)
	}
	scratch := getBytes()
	defer putBytes(scratch)
	l := b.LR.Len()
	*scratch = strconv.AppendInt(*scratch, l, 10)
	if err := multiWrite(w, bulkStrPrefix, *scratch, delim); err != nil {
		return err
	}
	if _, err := io.CopyN(w, b.LR, l); err != nil {
		return err
	}
	return multiWrite(w, delim)
}

////////////////////////////////////////////////////////////////////////////////

// ArrayHeader represents the header sent preceding array elements in the RESP
// protocol. It does not actually encompass any elements itself, it only
// declares how many elements will come after it.
//
// An N of -1 may also be used to indicate a nil response, as per the RESP spec
type ArrayHeader struct {
	N int
}

// MarshalRESP implements the Marshaler method
func (ah ArrayHeader) MarshalRESP(w io.Writer) error {
	scratch := getBytes()
	defer putBytes(scratch)
	*scratch = strconv.AppendInt(*scratch, int64(ah.N), 10)
	return multiWrite(w, arrayPrefix, *scratch, delim)
}

// UnmarshalRESP implements the Unmarshaler method
func (ah *ArrayHeader) UnmarshalRESP(br *bufio.Reader) error {
	if err := bufferedPrefix(br, arrayPrefix); err != nil {
		return err
	}
	n, err := bufferedIntDelim(br)
	ah.N = int(n)
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Array represents an array of RESP elements which will be marshaled as a RESP
// array. If A is Nil then a Nil RESP will be marshaled.
type Array struct {
	A []Marshaler
}

// MarshalRESP implements the Marshaler method
func (a Array) MarshalRESP(w io.Writer) error {
	ah := ArrayHeader{N: len(a.A)}
	if a.A == nil {
		ah.N = -1
	}

	if err := ah.MarshalRESP(w); err != nil {
		return err
	}
	for _, el := range a.A {
		if err := el.MarshalRESP(w); err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// Any represents any primitive go type, such as integers, floats, strings,
// bools, etc... It also includes encoding.Text(Un)Marshalers and
// encoding.(Un)BinaryMarshalers. It will _not_ marshal resp.Marshalers.
//
// Most things will be treated as bulk strings, except for those that have their
// own corresponding type in the RESP protocol (e.g. ints). strings and []bytes
// will always be encoded as bulk strings, never simple strings.
//
// Arrays and slices will be treated as RESP arrays, and their values will be
// treated as if also wrapped in an Any struct. Maps will be similarly treated,
// but they will be flattened into arrays of their alternating keys/values
// first.
//
// When using UnmarshalRESP the value of I must be a pointer or nil. If it is
// nil then the RESP value will be read and discarded.
//
// If an error type is read in the UnmarshalRESP method then a resp.Error will
// be returned with that error, and the value of I won't be touched.
type Any struct {
	I interface{}

	// If true then the MarshalRESP method will marshal all non-array types as
	// bulk strings. This primarily effects integers and errors.
	MarshalBulkString bool

	// If true then no array headers will be sent when MarshalRESP is called.
	// For I values which are non-arrays this means no behavior change. For
	// arrays and embedded arrays it means only the array elements will be
	// written, and an ArrayHeader must have been manually marshalled
	// beforehand.
	MarshalNoArrayHeaders bool
}

func (a Any) cp(i interface{}) Any {
	a.I = i
	return a
}

var byteSliceT = reflect.TypeOf([]byte{})

// NumElems returns the number of non-array elements which would be marshalled
// based on I. For example:
//
// Any{I: "foo"}.NumElems() == 1
// Any{I: []string{}}.NumElems() == 0
// Any{I: []string{"foo"}}.NumElems() == 2
// Any{I: []string{"foo", "bar"}}.NumElems() == 2
// Any{I: [][]string{{"foo"}, {"bar", "baz"}, {}}}.NumElems() == 3
//
func (a Any) NumElems() int {
	vv := reflect.ValueOf(a.I)
	switch vv.Kind() {
	case reflect.Slice, reflect.Array:
		if vv.Type() == byteSliceT {
			return 1
		}

		l := vv.Len()
		var c int
		for i := 0; i < l; i++ {
			c += Any{I: vv.Index(i).Interface()}.NumElems()
		}
		return c

	case reflect.Map:
		kkv := vv.MapKeys()
		var c int
		for _, kv := range kkv {
			c += Any{I: kv.Interface()}.NumElems()
			c += Any{I: vv.MapIndex(kv).Interface()}.NumElems()
		}
		return c

	default:
		return 1
	}
}

// MarshalRESP implements the Marshaler method
func (a Any) MarshalRESP(w io.Writer) error {
	marshalBulk := func(b []byte) error {
		bs := BulkStringBytes{B: b, MarshalNotNil: a.MarshalBulkString}
		return bs.MarshalRESP(w)
	}

	switch at := a.I.(type) {
	case []byte:
		return marshalBulk(at)
	case string:
		if at == "" {
			// special case, we never want string to be nil, but appending empty
			// string to a nil []byte would still be a nil bulk string
			return BulkStringBytes{MarshalNotNil: true}.MarshalRESP(w)
		}
		scratch := getBytes()
		defer putBytes(scratch)
		*scratch = append(*scratch, at...)
		return marshalBulk(*scratch)
	case bool:
		b := bools[0]
		if at {
			b = bools[1]
		}
		return marshalBulk(b)
	case float32:
		scratch := getBytes()
		defer putBytes(scratch)
		*scratch = strconv.AppendFloat(*scratch, float64(at), 'f', -1, 32)
		return marshalBulk(*scratch)
	case float64:
		scratch := getBytes()
		defer putBytes(scratch)
		*scratch = strconv.AppendFloat(*scratch, at, 'f', -1, 64)
		return marshalBulk(*scratch)
	case nil:
		return marshalBulk(nil)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		at64 := anyIntToInt64(at)
		if a.MarshalBulkString {
			scratch := getBytes()
			defer putBytes(scratch)
			*scratch = strconv.AppendInt(*scratch, at64, 10)
			return marshalBulk(*scratch)
		}
		return Int{I: at64}.MarshalRESP(w)
	case error:
		if a.MarshalBulkString {
			scratch := getBytes()
			defer putBytes(scratch)
			*scratch = append(*scratch, at.Error()...)
			return marshalBulk(*scratch)
		}
		return Error{E: at}.MarshalRESP(w)
	case LenReader:
		return BulkReader{LR: at}.MarshalRESP(w)
	case encoding.TextMarshaler:
		b, err := at.MarshalText()
		if err != nil {
			return err
		}
		return marshalBulk(b)
	case encoding.BinaryMarshaler:
		b, err := at.MarshalBinary()
		if err != nil {
			return err
		}
		return marshalBulk(b)
	}

	// now we use.... reflection! duhduhduuuuh....
	vv := reflect.ValueOf(a.I)

	// if it's a pointer we de-reference and try the pointed to value directly
	if vv.Kind() == reflect.Ptr {
		return a.cp(reflect.Indirect(vv).Interface()).MarshalRESP(w)
	}

	// some helper functions
	var err error
	arrHeader := func(l int) {
		if a.MarshalNoArrayHeaders || err != nil {
			return
		}
		err = (ArrayHeader{N: l}.MarshalRESP(w))
	}
	arrVal := func(v interface{}) {
		if err != nil {
			return
		}
		err = a.cp(v).MarshalRESP(w)
	}

	switch vv.Kind() {
	case reflect.Slice, reflect.Array:
		if vv.IsNil() && !a.MarshalNoArrayHeaders {
			err = multiWrite(w, nilArray)
			break
		}

		l := vv.Len()
		arrHeader(l)
		for i := 0; i < l; i++ {
			arrVal(vv.Index(i).Interface())
		}

	case reflect.Map:
		if vv.IsNil() && !a.MarshalNoArrayHeaders {
			err = multiWrite(w, nilArray)
			break
		}
		kkv := vv.MapKeys()
		arrHeader(len(kkv) * 2)
		for _, kv := range kkv {
			arrVal(kv.Interface())
			arrVal(vv.MapIndex(kv).Interface())
		}

	default:
		return fmt.Errorf("could not marshal value of type %T", a.I)
	}

	return err
}

func saneDefault(prefix byte) interface{} {
	// we don't handle errPrefix because that always returns an error and
	// doesn't touch I
	switch prefix {
	case arrayPrefix[0]:
		ii := make([]interface{}, 8)
		return &ii
	case bulkStrPrefix[0]:
		bb := make([]byte, 16)
		return &bb
	case simpleStrPrefix[0]:
		return new(string)
	case intPrefix[0]:
		return new(int64)
	}
	panic("should never get here")
}

// We use pools for these even though they only get used within
// Any.UnmarshalRESP because of how often they get used. Any return from redis
// which has a simple string or bulk string (the vast majority of them) is going
// to go through one of these.
var (
	// RawMessage.UnmarshalInto also uses this
	byteReaderPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewReader(nil)
		},
	}
)

// UnmarshalRESP implements the Unmarshaler method
func (a Any) UnmarshalRESP(br *bufio.Reader) error {
	// if I is itself an Unmarshaler just hit that directly
	if u, ok := a.I.(Unmarshaler); ok {
		return u.UnmarshalRESP(br)
	}

	b, err := br.Peek(1)
	if err != nil {
		return err
	}
	prefix := b[0]

	// This is a super special case that _must_ be handled before we actually
	// read from the reader. If an *interface{} is given we instead unmarshal
	// into a default (created based on the type of th message), then set the
	// *interface{} to that
	if ai, ok := a.I.(*interface{}); ok {
		innerA := Any{I: saneDefault(prefix)}
		if err := innerA.UnmarshalRESP(br); err != nil {
			return err
		}
		*ai = reflect.ValueOf(innerA.I).Elem().Interface()
		return nil
	}

	br.Discard(1)
	b, err = bufferedBytesDelim(br)
	if err != nil {
		return err
	}

	switch prefix {
	case errPrefix[0]:
		return Error{E: errors.New(string(b))}
	case arrayPrefix[0]:
		l, err := parseInt(b)
		if err != nil {
			return err
		} else if l == -1 {
			return a.unmarshalNil()
		}
		return a.unmarshalArray(br, l)
	case bulkStrPrefix[0]:
		l, err := parseInt(b) // fuck DRY
		if err != nil {
			return err
		} else if l == -1 {
			return a.unmarshalNil()
		}
		if err := a.unmarshalSingle(br, int(l)); err != nil && err != io.EOF {
			return err
		}
		_, err = br.Discard(2)
		return err
	case simpleStrPrefix[0], intPrefix[0]:
		reader := byteReaderPool.Get().(*bytes.Reader)
		defer byteReaderPool.Put(reader)
		reader.Reset(b)
		return a.unmarshalSingle(reader, reader.Len())
	default:
		return fmt.Errorf("unknown type prefix %q", b[0])
	}
}

func (a Any) unmarshalSingle(body io.Reader, n int) error {
	var (
		err error
		i   int64
		ui  uint64
	)

	switch ai := a.I.(type) {
	case nil:
		// just read it and do nothing
		_, err = io.CopyN(ioutil.Discard, body, int64(n))
	case *string:
		scratch := getBytes()
		*scratch, err = readNAppend(body, *scratch, n)
		*ai = string(*scratch)
		putBytes(scratch)
	case *[]byte:
		*ai, err = readNAppend(body, (*ai)[:0], n)
	case *bool:
		ui, err = readUint(body, n)
		*ai = ui > 0
	case *int:
		i, err = readInt(body, n)
		*ai = int(i)
	case *int8:
		i, err = readInt(body, n)
		*ai = int8(i)
	case *int16:
		i, err = readInt(body, n)
		*ai = int16(i)
	case *int32:
		i, err = readInt(body, n)
		*ai = int32(i)
	case *int64:
		i, err = readInt(body, n)
		*ai = int64(i)
	case *uint:
		ui, err = readUint(body, n)
		*ai = uint(ui)
	case *uint8:
		ui, err = readUint(body, n)
		*ai = uint8(ui)
	case *uint16:
		ui, err = readUint(body, n)
		*ai = uint16(ui)
	case *uint32:
		ui, err = readUint(body, n)
		*ai = uint32(ui)
	case *uint64:
		ui, err = readUint(body, n)
		*ai = uint64(ui)
	case *float32:
		var f float64
		f, err = readFloat(body, 32, n)
		*ai = float32(f)
	case *float64:
		*ai, err = readFloat(body, 64, n)
	case io.Writer:
		_, err = io.CopyN(ai, body, int64(n))
	case encoding.TextUnmarshaler:
		scratch := getBytes()
		if *scratch, err = readNAppend(body, *scratch, n); err != nil {
			break
		}
		err = ai.UnmarshalText(*scratch)
		putBytes(scratch)
	case encoding.BinaryUnmarshaler:
		scratch := getBytes()
		if *scratch, err = readNAppend(body, *scratch, n); err != nil {
			break
		}
		err = ai.UnmarshalBinary(*scratch)
		putBytes(scratch)
	default:
		return fmt.Errorf("can't unmarshal into %T", a.I)
	}

	return err
}

func (a Any) unmarshalNil() error {
	vv := reflect.ValueOf(a.I)
	if vv.Kind() != reflect.Ptr || !vv.Elem().CanSet() {
		// If the type in I can't be set then just ignore it. This is kind of
		// weird but it's what encoding/json does in the same circumstance
		return nil
	}

	vve := vv.Elem()
	vve.Set(reflect.Zero(vve.Type()))
	return nil
}

func (a Any) unmarshalArray(br *bufio.Reader, l int64) error {
	if a.I == nil {
		return a.discardArray(br, l)
	}

	size := int(l)
	v := reflect.ValueOf(a.I)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("can't unmarshal into %T", a.I)
	}
	v = reflect.Indirect(v)

	switch v.Kind() {
	case reflect.Slice:
		if size > v.Cap() || v.IsNil() {
			newV := reflect.MakeSlice(v.Type(), size, size)
			// we copy only because there might be some preset values in there
			// already that we're intended to decode into,
			// e.g.  []interface{}{int8(0), ""}
			reflect.Copy(newV, v)
			v.Set(newV)
		} else if size != v.Len() {
			v.SetLen(size)
		}

		for i := 0; i < size; i++ {
			ai := Any{I: v.Index(i).Addr().Interface()}
			if err := ai.UnmarshalRESP(br); err != nil {
				return err
			}
		}
		return nil

	case reflect.Map:
		if size%2 != 0 {
			return errors.New("cannot decode redis array with odd number of elements into map")
		} else if v.IsNil() {
			v.Set(reflect.MakeMapWithSize(v.Type(), size/2))
		}

		for i := 0; i < size; i += 2 {
			kv := reflect.New(v.Type().Key())
			if err := (Any{I: kv.Interface()}).UnmarshalRESP(br); err != nil {
				return err
			}

			vv := reflect.New(v.Type().Elem())
			if err := (Any{I: vv.Interface()}).UnmarshalRESP(br); err != nil {
				return err
			}

			v.SetMapIndex(kv.Elem(), vv.Elem())
		}
		return nil

	default:
		return fmt.Errorf("cannot decode redis array into %v", v.Type())
	}
}

func (a Any) discardArray(br *bufio.Reader, l int64) error {
	for i := 0; i < int(l); i++ {
		if err := (Any{}).UnmarshalRESP(br); err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// RawMessage is a Marshaler/Unmarshaler which will capture the exact raw bytes
// of a RESP message. When Marshaling the exact bytes of the RawMessage will be
// written as-is. When Unmarshaling the bytes of a single RESP message will be
// read into the RawMessage's bytes.
type RawMessage []byte

// MarshalRESP implements the Marshaler method
func (rm RawMessage) MarshalRESP(w io.Writer) error {
	_, err := w.Write(rm)
	return err
}

// UnmarshalRESP implements the Unmarshaler method
func (rm *RawMessage) UnmarshalRESP(br *bufio.Reader) error {
	*rm = (*rm)[:0]
	return rm.unmarshal(br)
}

func (rm *RawMessage) unmarshal(br *bufio.Reader) error {
	b, err := br.ReadSlice('\n')
	if err != nil {
		return err
	}
	*rm = append(*rm, b...)

	if len(b) < 3 {
		return errors.New("malformed data read")
	}
	body := b[1 : len(b)-2]

	switch b[0] {
	case arrayPrefix[0]:
		l, err := parseInt(body)
		if err != nil {
			return err
		} else if l == -1 {
			return nil
		}
		for i := 0; i < int(l); i++ {
			if err := rm.unmarshal(br); err != nil {
				return err
			}
		}
		return nil
	case bulkStrPrefix[0]:
		l, err := parseInt(body) // fuck DRY
		if err != nil {
			return err
		} else if l == -1 {
			return nil
		}
		*rm, err = readNAppend(br, *rm, int(l+2))
		return err
	case errPrefix[0], simpleStrPrefix[0], intPrefix[0]:
		return nil
	default:
		return fmt.Errorf("unknown type prefix %q", b[0])
	}
}

// UnmarshalInto is a shortcut for wrapping this RawMessage in a *bufio.Reader
// and passing that into the given Unmarshaler's UnmarshalRESP method. Any error
// from calling UnmarshalRESP is returned, and the RawMessage is unaffected in
// all cases.
func (rm RawMessage) UnmarshalInto(u Unmarshaler) error {
	r := byteReaderPool.Get().(*bytes.Reader)
	defer byteReaderPool.Put(r)
	r.Reset(rm)
	return u.UnmarshalRESP(bufio.NewReader(r))
}

// IsNil returns true if the contents of RawMessage are one of the nil values.
func (rm RawMessage) IsNil() bool {
	return bytes.Equal(rm, nilBulkString) || bytes.Equal(rm, nilArray)
}
