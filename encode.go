package radix

import (
	"bufio"
	"bytes"
	"encoding"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

// LenReader adds an additional method to io.Reader, returning how many bytes
// are left till be read until an io.EOF is reached.
type LenReader interface {
	io.Reader
	Len() int
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

	err = e.walk(v, func(v interface{}) error {
		return e.write(v, false)
	}, func(l int) error {
		return e.writeArrayHeader(l)
	})

	return err
}

// TODO it might make more sense to have walk also deal explicitly with the
// types enumerated in write, so it doesn't have to do random one-off checks for
// []byte and Marshalers and stuff

// fn is called on all "single" elements. arrFn is called with the length of
// all arrays found, before the individual elements of the array are sent to fn.
// either can be nil. called in depth-first order.
func (e *Encoder) walk(v interface{}, fn func(interface{}) error, arrFn func(int) error) (err error) {
	doFn := func(v interface{}) {
		if fn != nil && err == nil {
			err = fn(v)
		}
	}

	doArrFn := func(l int) {
		if arrFn != nil && err == nil {
			err = arrFn(l)
		}
	}

	doWalk := func(v interface{}) {
		if err == nil {
			err = e.walk(v, fn, arrFn)
		}
	}

	if _, ok := v.([]byte); ok {
		// make sure we never walk a byte slice, that's just a single element
		doFn(v)
		return

		// We check these two specifically because they could be implemented by
		// a []byte, which would match as a Slice down below and things would
		// get weird. This is pretty hacky to have this here too though
	} else if tm, ok := v.(encoding.TextMarshaler); ok {
		doFn(tm)
		return
	} else if bm, ok := v.(encoding.BinaryMarshaler); ok {
		doFn(bm)
		return

	} else if ii, ok := v.([]interface{}); ok {
		// this is a very common case, so we handle it without getting
		// reflection involved.
		doArrFn(len(ii))
		for _, i := range ii {
			doWalk(i)
		}
		return

	} else if c, ok := v.(Cmd); ok {
		// this is kind of an unlikely case, I'm pretty sure it would only
		// happen if a Cmd was embedded in another Cmd somehow. But might as
		// well handle it
		doArrFn(1 + len(c.Args))
		doFn(c.Cmd)
		for _, arg := range c.Args {
			doWalk(arg)
		}
		return
	}

	vv := reflect.ValueOf(v)
	switch vv.Kind() {
	case reflect.Slice, reflect.Array:
		l := vv.Len()
		doArrFn(l)
		for i := 0; i < l; i++ {
			doWalk(vv.Index(i).Interface())
		}

	case reflect.Map:
		doArrFn(vv.Len() * 2)
		for _, k := range vv.MapKeys() {
			doWalk(k.Interface())
			doWalk(vv.MapIndex(k).Interface())
		}

	default:
		// for all else just assume the element is a single element
		doFn(v)
	}

	return
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
		// TODO predefine these
		if vt {
			return e.writeBulkStrBytes([]byte("1"))
		}
		return e.writeBulkStrBytes([]byte("0"))
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
		return e.writeAppErr(AppErr{error: vt})
	case Cmd:
		return e.writeCmd(vt)
	case LenReader:
		return e.writeBulkStr(vt)
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
	// first we need to figure out the size of this thing. The one is for the
	// Cmd field
	total := 1
	if err := e.walk(c.Args, nil, func(l int) error {
		total += l
		return nil
	}); err != nil {
		return err
	}

	// write the array header, then write every single non-array element as a
	// string
	if err := e.writeArrayHeader(total); err != nil {
		return err
	}
	return e.walk(c.Args, func(v interface{}) error {
		return e.write(v, true)
	}, nil)
}

func (e *Encoder) writeBulkStrBytes(b []byte) error {
	return e.writeBulkStr(bytes.NewBuffer(b))
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
