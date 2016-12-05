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

// Marshaler is the interface implemented by types that can marshal themselves
// into valid RESP.
//
// Pool is always optional, nil may be passed in instead.
type Marshaler interface {
	MarshalRESP(*Pool, io.Writer) error
}

// Unmarshaler is the interface implemented by types that can unmarshal a RESP
// description of themselves.
//
// Note that, unlike Marshaler, Unmarshaler _must_ take in a *bufio.Reader.
//
// Pool is always optional, nil may be passed in instead.
type Unmarshaler interface {
	UnmarshalRESP(*Pool, *bufio.Reader) error
}

////////////////////////////////////////////////////////////////////////////////

func expand(b []byte, to int) []byte {
	if b == nil || cap(b) < to {
		return make([]byte, to)
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
	b, err := br.ReadSlice('\r')
	if err != nil {
		return nil, err
	}

	// there's a trailing \n we have to read
	_, err = br.ReadByte()
	return b[:len(b)-1], err
}

// reads an integer out of the buffer, followed by a delim. It parses the
// integer, or returns an error
func bufferedIntDelim(br *bufio.Reader) (int64, error) {
	b, err := bufferedBytesDelim(br)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(b), 10, 64)
}

func readAllAppend(r io.Reader, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	// TODO a side effect of this is that the given b will be re-allocated if
	// it's less than bytes.MinRead. Since this b could be all the way from the
	// user we can't guarantee it within the library. Would be nice to not have
	// that weird edge-case
	_, err := buf.ReadFrom(r)
	return buf.Bytes(), err
}

func multiWrite(w io.Writer, bb ...[]byte) error {
	for _, b := range bb {
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// Pool is used between RESP types so that they may share resources during
// reading and writing, primarily to avoid memory allocations. `new(resp.Pool)`
// is the proper way to initialize a Pool. It is optional in all places which
// it appears (pass nil instead).
type Pool struct {
	scratch []byte
	// Since we use the normal scratch when writing bulk strings it's more
	// convenient to just keep the actual bulk string value in a different slice
	bulkStrScratch []byte
}

// used to retrieve or initialize a Pool, depending on whether it's been
// initialized already. It will reset the scratches if it has already been
// initialized. This is the only method on Pool which allows for a nil Pool.
func (p *Pool) get() *Pool {
	if p == nil {
		p = new(Pool)
	}
	p.scratch = p.scratch[:0]
	p.bulkStrScratch = p.bulkStrScratch[:0]
	return p
}

func (p *Pool) readInt(r io.Reader) (int64, error) {
	var err error
	if p.scratch, err = readAllAppend(r, p.scratch[:0]); err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(p.scratch), 10, 64)
}

func (p *Pool) readUint(r io.Reader) (uint64, error) {
	var err error
	if p.scratch, err = readAllAppend(r, p.scratch[:0]); err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(p.scratch), 10, 64)
}

func (p *Pool) readFloat(r io.Reader, precision int) (float64, error) {
	var err error
	if p.scratch, err = readAllAppend(r, p.scratch[:0]); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(p.scratch), precision)
}

////////////////////////////////////////////////////////////////////////////////

// SimpleString represents the simple string type in the RESP protocol. An S
// value of nil is equivalent to empty string.
type SimpleString struct {
	S []byte
}

// MarshalRESP implements the Marshaler method
func (ss SimpleString) MarshalRESP(p *Pool, w io.Writer) error {
	return multiWrite(w, simpleStrPrefix, ss.S, delim)
}

// UnmarshalRESP implements the Unmarshaler method
func (ss *SimpleString) UnmarshalRESP(p *Pool, br *bufio.Reader) error {
	if err := bufferedPrefix(br, simpleStrPrefix); err != nil {
		return err
	}
	b, err := bufferedBytesDelim(br)
	if err != nil {
		return err
	}
	ss.S = expand(ss.S, len(b))
	copy(ss.S, b)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

// Error represents an error type in the RESP protocol. Note that this only
// represents an actual error message being read/written on the stream, it is
// separate from network or parsing errors. An E value of nil is equivalent to
// an empty error string
//
// Note that the non-pointer form of Error implements the error and Marshaler
// interfaces, and the pointer form implements the Unmarshaler interface.
type Error struct {
	E error
}

func (e Error) Error() string {
	return e.E.Error()
}

// MarshalRESP implements the Marshaler method
func (e Error) MarshalRESP(p *Pool, w io.Writer) error {
	p = p.get()
	if e.E == nil {
		return multiWrite(w, errPrefix, delim)
	}
	return multiWrite(w, errPrefix, append(p.scratch, e.E.Error()...), delim)
}

// UnmarshalRESP implements the Unmarshaler method
func (e *Error) UnmarshalRESP(p *Pool, br *bufio.Reader) error {
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
func (i Int) MarshalRESP(p *Pool, w io.Writer) error {
	p = p.get()
	return multiWrite(w, intPrefix, strconv.AppendInt(p.scratch, int64(i.I), 10), delim)
}

// UnmarshalRESP implements the Unmarshaler method
func (i *Int) UnmarshalRESP(p *Pool, br *bufio.Reader) error {
	if err := bufferedPrefix(br, intPrefix); err != nil {
		return err
	}
	n, err := bufferedIntDelim(br)
	i.I = n
	return err
}

////////////////////////////////////////////////////////////////////////////////

// BulkString represents the bulk string type in the RESP protocol. A B value of
// nil indicates the nil bulk string message, versus a B value of []byte{} which
// indicates a bulk string of length 0.
type BulkString struct {
	B []byte

	// If true then this won't marshal the nil RESP value when B is nil, it will
	// marshal as an empty string instead
	MarshalNotNil bool
}

// MarshalRESP implements the Marshaler method
func (b BulkString) MarshalRESP(p *Pool, w io.Writer) error {
	p = p.get()
	if b.B == nil && !b.MarshalNotNil {
		return multiWrite(w, nilBulkString)
	}
	return multiWrite(w,
		bulkStrPrefix,
		strconv.AppendInt(p.scratch, int64(len(b.B)), 10),
		delim,
		b.B,
		delim,
	)
}

// UnmarshalRESP implements the Unmarshaler method
func (b *BulkString) UnmarshalRESP(p *Pool, br *bufio.Reader) error {
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
	}

	if _, err := io.ReadFull(br, b.B); err != nil {
		return err
	} else if _, err := bufferedBytesDelim(br); err != nil {
		return err
	}
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
func (b BulkReader) MarshalRESP(p *Pool, w io.Writer) error {
	p = p.get()
	if b.LR == nil {
		return multiWrite(w, nilBulkString)
	}
	l := b.LR.Len()
	if err := multiWrite(w,
		bulkStrPrefix,
		strconv.AppendInt(p.scratch, l, 10),
		delim,
	); err != nil {
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
func (ah ArrayHeader) MarshalRESP(p *Pool, w io.Writer) error {
	p = p.get()
	return multiWrite(w, arrayPrefix, strconv.AppendInt(p.scratch, int64(ah.N), 10), delim)
}

// UnmarshalRESP implements the Unmarshaler method
func (ah *ArrayHeader) UnmarshalRESP(p *Pool, br *bufio.Reader) error {
	if err := bufferedPrefix(br, arrayPrefix); err != nil {
		return err
	}
	n, err := bufferedIntDelim(br)
	ah.N = int(n)
	return err
}

////////////////////////////////////////////////////////////////////////////////

// Any represents any primitive go type, such as integers, floats, strings,
// bools, etc... It also includes encoding.Text(Un)Marshalers and
// encoding.(Un)BinaryMarshalers. It will _not_ handle resp.(Un)Marshalers.
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
func (a Any) MarshalRESP(p *Pool, w io.Writer) error {
	p = p.get()

	marshalBulk := func(b []byte) error {
		bs := BulkString{B: b, MarshalNotNil: a.MarshalBulkString}
		return bs.MarshalRESP(p, w)
	}

	switch at := a.I.(type) {
	case []byte:
		return marshalBulk(at)
	case string:
		if at == "" {
			// special case, we never want string to be nil, but appending empty
			// string to a nil p.bulkStrScratch would still be a nil bulk string
			return BulkString{MarshalNotNil: true}.MarshalRESP(p, w)
		}
		p.bulkStrScratch = append(p.bulkStrScratch, at...)
		return marshalBulk(p.bulkStrScratch)
	case bool:
		b := bools[0]
		if at {
			b = bools[1]
		}
		return marshalBulk(b)
	case float32:
		p.bulkStrScratch = strconv.AppendFloat(p.bulkStrScratch, float64(at), 'f', -1, 32)
		return marshalBulk(p.bulkStrScratch)
	case float64:
		p.bulkStrScratch = strconv.AppendFloat(p.bulkStrScratch, at, 'f', -1, 64)
		return marshalBulk(p.bulkStrScratch)
	case nil:
		return marshalBulk(nil)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		at64 := anyIntToInt64(at)
		if a.MarshalBulkString {
			p.bulkStrScratch = strconv.AppendInt(p.bulkStrScratch, at64, 10)
			return marshalBulk(p.bulkStrScratch)
		}
		return Int{I: at64}.MarshalRESP(p, w)
	case error:
		if a.MarshalBulkString {
			p.bulkStrScratch = append(p.bulkStrScratch, at.Error()...)
			return marshalBulk(p.bulkStrScratch)
		}
		return Error{E: at}.MarshalRESP(p, w)
	case LenReader:
		return BulkReader{LR: at}.MarshalRESP(p, w)
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
		return a.cp(reflect.Indirect(vv).Interface()).MarshalRESP(p, w)
	}

	// some helper functions
	var err error
	arrHeader := func(l int) {
		if a.MarshalNoArrayHeaders || err != nil {
			return
		}
		err = (ArrayHeader{N: l}.MarshalRESP(p, w))
	}
	arrVal := func(v interface{}) {
		if err != nil {
			return
		}
		err = a.cp(v).MarshalRESP(p, w)
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

// UnmarshalRESP implements the Unmarshaler method
func (a *Any) UnmarshalRESP(p *Pool, br *bufio.Reader) error {
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
		if err := innerA.UnmarshalRESP(p, br); err != nil {
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
		l, err := strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			return err
		} else if l == -1 {
			return a.unmarshalNil()
		}
		return a.unmarshalArray(p, br, l)
	case bulkStrPrefix[0]:
		l, err := strconv.ParseInt(string(b), 10, 64) // fuck DRY
		if err != nil {
			return err
		} else if l == -1 {
			return a.unmarshalNil()
		}
		return a.unmarshalSingle(p, newLimitedReaderPlus(br, l))
	case simpleStrPrefix[0], intPrefix[0]:
		return a.unmarshalSingle(p, bytes.NewBuffer(b))
	default:
		return fmt.Errorf("unknown type prefix %q", b[0])
	}
}

func (a *Any) unmarshalSingle(p *Pool, body io.Reader) error {
	var (
		err error
		i   int64
		ui  uint64
	)
	p = p.get()

	switch ai := a.I.(type) {
	case nil:
		// just read it and do nothing
		_, err = io.Copy(ioutil.Discard, body)
	case *string:
		p.scratch, err = readAllAppend(body, p.scratch)
		*ai = string(p.scratch)
	case *[]byte:
		*ai, err = readAllAppend(body, (*ai)[:0])
	case *bool:
		ui, err = p.readUint(body)
		*ai = (ui > 0)
	case *int:
		i, err = p.readInt(body)
		*ai = int(i)
	case *int8:
		i, err = p.readInt(body)
		*ai = int8(i)
	case *int16:
		i, err = p.readInt(body)
		*ai = int16(i)
	case *int32:
		i, err = p.readInt(body)
		*ai = int32(i)
	case *int64:
		i, err = p.readInt(body)
		*ai = int64(i)
	case *uint:
		ui, err = p.readUint(body)
		*ai = uint(ui)
	case *uint8:
		ui, err = p.readUint(body)
		*ai = uint8(ui)
	case *uint16:
		ui, err = p.readUint(body)
		*ai = uint16(ui)
	case *uint32:
		ui, err = p.readUint(body)
		*ai = uint32(ui)
	case *uint64:
		ui, err = p.readUint(body)
		*ai = uint64(ui)
	case *float32:
		var f float64
		f, err = p.readFloat(body, 32)
		*ai = float32(f)
	case *float64:
		*ai, err = p.readFloat(body, 64)
	case io.Writer:
		_, err = io.Copy(ai, body)
	case encoding.TextUnmarshaler:
		if p.scratch, err = readAllAppend(body, p.scratch); err != nil {
			break
		}
		err = ai.UnmarshalText(p.scratch)
	case encoding.BinaryUnmarshaler:
		if p.scratch, err = readAllAppend(body, p.scratch); err != nil {
			break
		}
		err = ai.UnmarshalBinary(p.scratch)
	default:
		return fmt.Errorf("can't unmarshal into %T", a.I)
	}

	return err
}

func (a *Any) unmarshalNil() error {
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

func (a *Any) unmarshalArray(p *Pool, br *bufio.Reader, l int64) error {
	if a.I == nil {
		return a.discardArray(p, br, l)
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
			if err := ai.UnmarshalRESP(p, br); err != nil {
				return err
			}
		}
		return nil

	case reflect.Map:
		if size%2 != 0 {
			return errors.New("cannot decode redis array with odd number of elements into map")
		} else if v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		}

		for i := 0; i < size; i += 2 {
			kv := reflect.New(v.Type().Key())
			ak := Any{I: kv.Interface()}
			if err := ak.UnmarshalRESP(p, br); err != nil {
				return err
			}

			vv := reflect.New(v.Type().Elem())
			av := Any{I: vv.Interface()}
			if err := av.UnmarshalRESP(p, br); err != nil {
				return err
			}

			v.SetMapIndex(kv.Elem(), vv.Elem())
		}
		return nil

	default:
		return fmt.Errorf("cannot decode redis array into %v", v.Type())
	}
}

func (a *Any) discardArray(p *Pool, br *bufio.Reader, l int64) error {
	for i := 0; i < int(l); i++ {
		da := Any{}
		if err := da.UnmarshalRESP(p, br); err != nil {
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
func (rm RawMessage) MarshalRESP(p *Pool, w io.Writer) error {
	_, err := w.Write(rm)
	return err
}

// UnmarshalRESP implements the Unmarshaler method
func (rm *RawMessage) UnmarshalRESP(p *Pool, br *bufio.Reader) error {
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
		l, err := strconv.ParseInt(string(body), 10, 64)
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
		l, err := strconv.ParseInt(string(body), 10, 64) // fuck DRY
		if err != nil {
			return err
		} else if l == -1 {
			return nil
		}
		*rm, err = readAllAppend(io.LimitReader(br, l+2), *rm)
		return err
	case errPrefix[0], simpleStrPrefix[0], intPrefix[0]:
		return nil
	default:
		return fmt.Errorf("unknown type prefix %q", b[0])
	}
}
