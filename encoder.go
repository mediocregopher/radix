package radix

import (
	"bufio"
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

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

// Encoder wraps an io.Writer and encodes Resp data onto it
type Encoder interface {
	// Encode writes the given value to the underlying io.Writer, first encoding
	// it into a Resp message.
	// TODO more docs on how that happens
	Encode(interface{}) error
}

type encoder struct {
	w       *bufio.Writer
	bodyBuf *bytes.Buffer
	cmdBuf  []interface{} // TODO this needs some kind of ceiling
	scratch []byte
}

// NewEncoder initializes an encoder instance which will write to the given
// io.Writer. The io.Writer should not be used outside of the encoder after this
func NewEncoder(w io.Writer) Encoder {
	return &encoder{
		w:       bufio.NewWriter(w),
		bodyBuf: bytes.NewBuffer(make([]byte, 0, 1024)),
		cmdBuf:  make([]interface{}, 0, 128),
		scratch: make([]byte, 0, 1024),
	}
}

func (e *encoder) Encode(v interface{}) error {
	var err error
	defer func() {
		if ferr := e.w.Flush(); ferr != nil && err == nil {
			err = ferr
		}
	}()

	return e.encode(v, false)
}

func (e *encoder) encode(v interface{}, forceBulkStr bool) error {
	return walkForeach(v, func(n walkNode) error {
		if n.vOk {
			return e.write(n.v, forceBulkStr)
		}
		return e.writeArrayHeader(n.l)
	})
}

var bools = [][]byte{
	{'0'},
	{'1'},
}

func (e *encoder) toBytes(v interface{}) (LenReader, error) {
	e.bodyBuf.Reset()
	switch vt := v.(type) {
	case []byte:
		e.bodyBuf.Write(vt)
		return e.bodyBuf, nil
	case string:
		e.bodyBuf.WriteString(vt)
		return e.bodyBuf, nil
	case bool:
		b := bools[0]
		if vt {
			b = bools[1]
		}
		e.bodyBuf.Write(b)
		return e.bodyBuf, nil
	case nil:
		return e.bodyBuf, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		i := anyIntToInt64(vt)
		b := strconv.AppendInt(e.scratch[:0], i, 10)
		e.bodyBuf.Write(b)
		return e.bodyBuf, nil
	case float32:
		b := strconv.AppendFloat(e.scratch[:0], float64(vt), 'f', -1, 32)
		e.bodyBuf.Write(b)
		return e.bodyBuf, nil
	case float64:
		b := strconv.AppendFloat(e.scratch[:0], vt, 'f', -1, 64)
		e.bodyBuf.Write(b)
		return e.bodyBuf, nil
	case error:
		e.bodyBuf.WriteString(vt.Error())
		return e.bodyBuf, nil
	case Resp:
		switch {
		case vt.SimpleStr != nil:
			return e.toBytes(vt.SimpleStr)
		case vt.BulkStr != nil:
			return e.toBytes(vt.BulkStr)
		case vt.Err != nil:
			return e.toBytes(vt.Err)
		case vt.Arr != nil:
			return nil, errors.New("can't convert array Resp to []byte")
		case vt.BulkStrNil, vt.ArrNil:
			return e.bodyBuf, nil
		default:
			return e.toBytes(vt.Int)
		}
	case LenReader:
		return vt, nil
	case Marshaler:
		nv, err := vt.Marshal()
		if err != nil {
			return nil, err
		}
		return e.toBytes(nv)
	case encoding.TextMarshaler:
		b, err := vt.MarshalText()
		e.bodyBuf.Write(b)
		return e.bodyBuf, err
	case encoding.BinaryMarshaler:
		b, err := vt.MarshalBinary()
		e.bodyBuf.Write(b)
		return e.bodyBuf, err
	}

	if vv := reflect.ValueOf(v); vv.Kind() == reflect.Ptr {
		return e.toBytes(vv.Elem().Interface())
	}

	return nil, fmt.Errorf("cannot convert %T to []byte", v)
}

// write writes whatever arbitrary data it's given as a resp. It does not handle
// any of the types which would be turned into arrays, those must be handled
// through walk
func (e *encoder) write(v interface{}, forceBulkStr bool) error {
	var err error
	if forceBulkStr {
		if v, err = e.toBytes(v); err != nil {
			return err
		}
	}

	switch vt := v.(type) {
	case LenReader:
		return e.writeLenReader(vt)
	case []byte, string, bool, float32, float64, encoding.TextMarshaler, encoding.BinaryMarshaler:
		// these are covered by toBytes anyway
		lr, err := e.toBytes(v)
		if err != nil {
			return err
		}
		return e.writeLenReader(lr)
	case nil:
		return e.writeBulkNil()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return e.writeInt(anyIntToInt64(vt))
	case error:
		// if we're writing an error we just assume that they want it as an
		// error type on the wire
		return e.writeAppErr(AppErr{Err: vt})
	case RawCmd:
		return e.writeCmd(vt)
	case Resp:
		return e.writeResp(vt)
	case Marshaler:
		nv, err := vt.Marshal()
		if err != nil {
			return err
		}
		return e.encode(nv, forceBulkStr)
	}

	if vv := reflect.ValueOf(v); vv.Kind() == reflect.Ptr {
		return e.write(vv.Elem().Interface(), forceBulkStr)
	}

	return fmt.Errorf("cannot encode %T as a redis type", v)
}

func (e *encoder) writeCmd(c RawCmd) error {
	e.cmdBuf = append(e.cmdBuf[:0], c.Cmd)
	for _, key := range c.Keys {
		e.cmdBuf = append(e.cmdBuf, key)
	}

	walkForeach(c.Args, func(n walkNode) error {
		if n.vOk {
			e.cmdBuf = append(e.cmdBuf, n.v)
		}
		return nil
	})

	// write the array header, then write every single non-array element as a
	// string
	if err := e.writeArrayHeader(len(e.cmdBuf)); err != nil {
		return err
	}
	for _, v := range e.cmdBuf {
		if err := e.write(v, true); err != nil {
			return err
		}
	}
	return nil
}

func (e *encoder) writeResp(r Resp) error {
	switch {
	case r.SimpleStr != nil:
		return e.writeSimpleStr(string(r.SimpleStr))
	case r.BulkStr != nil:
		e.bodyBuf.Reset()
		e.bodyBuf.Write(r.BulkStr)
		return e.writeLenReader(e.bodyBuf)
	case r.Err != nil:
		return e.writeAppErr(AppErr{Err: r.Err})
	case r.Arr != nil:
		if err := e.writeArrayHeader(len(r.Arr)); err != nil {
			return err
		}
		for i := range r.Arr {
			if err := e.writeResp(r.Arr[i]); err != nil {
				return err
			}
		}
		return nil
	case r.BulkStrNil:
		return e.writeBulkNil()
	case r.ArrNil:
		return e.writeArrayNil()
	default:
		return e.writeInt(r.Int)
	}
}

func (e *encoder) writeLenReader(lr LenReader) error {
	var err error
	err = e.writeBytes(err, bulkStrPrefix)
	err = e.writeBytes(err, strconv.AppendInt(e.scratch[:0], int64(lr.Len()), 10))
	err = e.writeBytes(err, delim)
	if err != nil {
		return err
	}

	_, err = io.Copy(e.w, lr)
	err = e.writeBytes(err, delim)
	return err
}

func (e *encoder) writeInt(i int64) error {
	var err error
	err = e.writeBytes(err, intPrefix)
	err = e.writeBytes(err, strconv.AppendInt(e.scratch[:0], i, 10))
	err = e.writeBytes(err, delim)
	return err
}

func (e *encoder) writeSimpleStr(s string) error {
	var err error
	err = e.writeBytes(err, simpleStrPrefix)
	err = e.writeBytes(err, append(e.scratch[:0], s...))
	err = e.writeBytes(err, delim)
	return err
}

func (e *encoder) writeAppErr(ae AppErr) error {
	var err error
	err = e.writeBytes(err, errPrefix)
	err = e.writeBytes(err, append(e.scratch[:0], ae.Error()...))
	err = e.writeBytes(err, delim)
	return err
}

func (e *encoder) writeArrayHeader(l int) error {
	var err error
	err = e.writeBytes(err, arrayPrefix)
	err = e.writeBytes(err, strconv.AppendInt(e.scratch[:0], int64(l), 10))
	err = e.writeBytes(err, delim)
	return err
}

func (e *encoder) writeBulkNil() error {
	return e.writeBytes(nil, nilBulkStr)
}

func (e *encoder) writeArrayNil() error {
	return e.writeBytes(nil, nilArray)
}

func (e *encoder) writeBytes(prevErr error, b []byte) error {
	if prevErr != nil {
		return prevErr
	}
	_, err := e.w.Write(b)
	return err
}
