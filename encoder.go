package radix

import (
	"bufio"
	"encoding"
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
type Encoder struct {
	w       *bufio.Writer
	scratch []byte
}

// NewEncoder initializes an Encoder instance which will write to the given
// io.Writer. The io.Writer should not be used outside of the Encoder after this
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w:       bufio.NewWriter(w),
		scratch: make([]byte, 1024),
	}
}

// Encode writes the given value to the underlying io.Writer, first encoding it
// into a Resp message.
// TODO more docs on how that happens
func (e *Encoder) Encode(v interface{}) error {
	var err error
	defer func() {
		if ferr := e.w.Flush(); ferr != nil && err == nil {
			err = ferr
		}
	}()

	return e.encode(v, false)
}

func (e *Encoder) encode(v interface{}, forceBulkStr bool) error {
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

// write writes whatever arbitrary data it's given as a resp. It does not handle
// any of the types which would be turned into arrays, those must be handled
// through walk
func (e *Encoder) write(v interface{}, forceBulkStr bool) error {
	switch vt := v.(type) {
	case []byte:
		return e.writeBulkStrBytes(vt)
	case string:
		return e.writeBulkStrBytes([]byte(vt))
	case bool:
		if vt {
			return e.writeBulkStrBytes(bools[1])
		}
		return e.writeBulkStrBytes(bools[0])
	case nil:
		if forceBulkStr {
			return e.writeBulkStrBytes(nil)
		}
		return e.writeBulkNil()
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		i := anyIntToInt64(vt)
		if forceBulkStr {
			b := strconv.AppendInt(e.scratch[:0], i, 10)
			return e.writeBulkStrBytes(b)
		}
		return e.writeInt(i)
	case float32:
		return e.writeFloat(float64(vt), 32)
	case float64:
		return e.writeFloat(vt, 64)
	case error:
		if forceBulkStr {
			return e.writeBulkStrBytes([]byte(vt.Error()))
		}
		// if we're writing an error we just assume that they want it as an
		// error type on the wire
		return e.writeAppErr(AppErr{Err: vt})
	case Cmd:
		return e.writeCmd(vt)
	case Resp:
		return e.writeResp(vt)
	case LenReader:
		return e.writeBulkStr(vt)
	case Marshaler:
		nv, err := vt.Marshal()
		if err != nil {
			return err
		}
		return e.encode(nv, forceBulkStr)
	case encoding.TextMarshaler:
		b, err := vt.MarshalText()
		if err != nil {
			return err
		}
		return e.writeBulkStrBytes(b)
	case encoding.BinaryMarshaler:
		b, err := vt.MarshalBinary()
		if err != nil {
			return err
		}
		return e.writeBulkStrBytes(b)
	}

	if vv := reflect.ValueOf(v); vv.Kind() == reflect.Ptr {
		return e.write(vv.Elem().Interface(), forceBulkStr)
	}

	return fmt.Errorf("cannot encode %T as a redis type", v)
}

func (e *Encoder) writeCmd(c Cmd) error {
	vv := make([]interface{}, 1, len(c.Args)+1) // use c.Args as a guess
	vv[0] = c.Cmd

	walkForeach(c.Args, func(n walkNode) error {
		if n.vOk {
			vv = append(vv, n.v)
		}
		return nil
	})

	// write the array header, then write every single non-array element as a
	// string
	if err := e.writeArrayHeader(len(vv)); err != nil {
		return err
	}
	for _, v := range vv {
		if err := e.write(v, true); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) writeResp(r Resp) error {
	switch {
	case r.SimpleStr != nil:
		return e.writeSimpleStr(string(r.SimpleStr))
	case r.BulkStr != nil:
		return e.writeBulkStrBytes(r.BulkStr)
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

func (e *Encoder) writeBulkStrBytes(b []byte) error {
	// implemented separately from writeBulkStrBytes to save an allocation of a
	// bytes.Buffer
	var err error
	err = e.writeBytes(err, bulkStrPrefix)
	err = e.writeBytes(err, strconv.AppendInt(e.scratch[:0], int64(len(b)), 10))
	err = e.writeBytes(err, delim)
	err = e.writeBytes(err, b)
	err = e.writeBytes(err, delim)
	return err
}

func (e *Encoder) writeBulkStr(lr LenReader) error {
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

func (e *Encoder) writeInt(i int64) error {
	var err error
	err = e.writeBytes(err, intPrefix)
	err = e.writeBytes(err, strconv.AppendInt(e.scratch[:0], i, 10))
	err = e.writeBytes(err, delim)
	return err
}

func (e *Encoder) writeFloat(f float64, bits int) error {
	// writeBulkStrBytes also uses scratch, gotta make sure we don't overlap by
	// accident, so temporarily overwrite scratch
	ogScratch := e.scratch
	b := strconv.AppendFloat(e.scratch[:0], f, 'f', -1, bits)
	e.scratch = e.scratch[len(b):]
	err := e.writeBulkStrBytes(b)
	e.scratch = ogScratch
	return err
}

func (e *Encoder) writeSimpleStr(s string) error {
	var err error
	err = e.writeBytes(err, simpleStrPrefix)
	err = e.writeBytes(err, []byte(s))
	err = e.writeBytes(err, delim)
	return err
}

func (e *Encoder) writeAppErr(ae AppErr) error {
	var err error
	err = e.writeBytes(err, errPrefix)
	err = e.writeBytes(err, []byte(ae.Error()))
	err = e.writeBytes(err, delim)
	return err
}

func (e *Encoder) writeArrayHeader(l int) error {
	var err error
	err = e.writeBytes(err, arrayPrefix)
	err = e.writeBytes(err, strconv.AppendInt(e.scratch[:0], int64(l), 10))
	err = e.writeBytes(err, delim)
	return err
}

func (e *Encoder) writeBulkNil() error {
	return e.writeBytes(nil, nilBulkStr)
}

func (e *Encoder) writeArrayNil() error {
	return e.writeBytes(nil, nilArray)
}

func (e *Encoder) writeBytes(prevErr error, b []byte) error {
	if prevErr != nil {
		return prevErr
	}
	_, err := e.w.Write(b)
	return err
}
