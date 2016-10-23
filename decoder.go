package radix

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

// A special limited reader which will read an extra two bytes after the limit
// has been reached

type limitedReaderPlus struct {
	eof     bool
	lr      *io.LimitedReader
	discard io.Writer
}

func newLimitedReaderPlus(r io.Reader, limit int64, discard io.Writer) *limitedReaderPlus {
	return &limitedReaderPlus{
		discard: discard,
		lr: &io.LimitedReader{
			R: r,
			N: limit,
		},
	}
}

func (lrp *limitedReaderPlus) Read(b []byte) (int, error) {
	if lrp.eof {
		return 0, io.EOF
	}

	i, err := lrp.lr.Read(b)
	if err == io.EOF {
		lrp.eof = true
		_, err = io.CopyN(lrp.discard, lrp.lr.R, 2)
		return i, err
	}
	return i, err
}

// Decoder wraps an io.Reader and decodes Resp data off of it.
type Decoder struct {

	// The size of the internal read buffer used for miscellaneous things. If
	// the buffer grows larger it will be re-allocated at this size.
	//
	// This should rarely if ever need to be touched. If you're not sure if it
	// needs to be changed assume it doesn't.
	//
	// Defaults to 1024
	BufSize int

	r       *bufio.Reader
	scratch []byte
	discard *bufio.Writer // used to just outright discard data
}

// NewDecoder initialized a Decoder instance which will read from the given
// io.Reader. The io.Reader should not be used after this
func NewDecoder(r io.Reader) *Decoder {
	bufSize := 1024
	return &Decoder{
		r:       bufio.NewReader(r),
		scratch: make([]byte, bufSize),
		// wrap in bufio so we get ReaderFrom, eliminates an allocation later
		discard: bufio.NewWriter(ioutil.Discard),
		BufSize: bufSize,
	}
}

var typePrefixMap = map[byte]riType{
	simpleStrPrefix[0]: riSimpleStr,
	errPrefix[0]:       riAppErr,
	intPrefix[0]:       riInt,
	bulkStrPrefix[0]:   riBulkStr,
	arrayPrefix[0]:     riArray,
}

var (
	emptyInterfaceT      = reflect.TypeOf([]interface{}(nil)).Elem()
	emptyInterfaceSliceT = reflect.SliceOf(emptyInterfaceT)
	stringT              = reflect.TypeOf("")
	intT                 = reflect.TypeOf(int64(0))
)

// Decode reads a single message off of the underlying io.Reader and unmarshals
// it into the given receiver, which should be a pointer or reference type.
func (d *Decoder) Decode(v interface{}) error {
	// the slice returned here should not be stored or modified, it's internal
	// to the bufio.Reader
	b, err := d.r.ReadSlice(delimEnd)
	if err != nil {
		return err
	}
	body := b[1 : len(b)-delimLen]

	typ, ok := typePrefixMap[b[0]]
	if !ok {
		return fmt.Errorf("Unknown type prefix %q", string(b))
	}

	var size int64
	switch typ {
	case riSimpleStr, riInt:
		return d.scanInto(v, bytes.NewReader(body), typ)

	case riBulkStr:
		if size, err = strconv.ParseInt(string(body), 10, 64); err != nil {
			return err
		}
		return d.scanInto(v, newLimitedReaderPlus(d.r, size, d.discard), typ)

	case riAppErr:
		return AppErr{errors.New(string(body))}

	case riArray:
		if size, err = strconv.ParseInt(string(body), 10, 64); err != nil {
			return err
		}
		return d.scanArrayInto(reflect.ValueOf(v), int(size))
	}

	// shouldn't ever get here
	panic(fmt.Sprintf("weird type: %#v", typ))
}

func (d *Decoder) scanInto(dst interface{}, r io.Reader, typ riType) error {
	var (
		err error
		i   int64
		ui  uint64
	)

	switch dstt := dst.(type) {
	case nil:
		// discard everything
	case io.Writer:
		_, err = io.Copy(dstt, r)
	case *string:
		d.scratch, err = readAllAppend(r, d.scratch[:0])
		*dstt = string(d.scratch)
	case *[]byte:
		*dstt, err = readAllAppend(r, (*dstt)[:0])
	case *bool:
		if ui, err = d.readUint(r); err != nil {
			err = fmt.Errorf("could not parse as bool: %s", err)
			break
		}
		if ui == 1 {
			*dstt = true
		} else if ui == 0 {
			*dstt = false
		} else {
			err = fmt.Errorf("invalid bool value: %d", ui)
		}

	case *int:
		i, err = d.readInt(r)
		*dstt = int(i)
	case *int8:
		i, err = d.readInt(r)
		*dstt = int8(i)
	case *int16:
		i, err = d.readInt(r)
		*dstt = int16(i)
	case *int32:
		i, err = d.readInt(r)
		*dstt = int32(i)
	case *int64:
		*dstt, err = d.readInt(r)
	case *uint:
		ui, err = d.readUint(r)
		*dstt = uint(ui)
	case *uint8:
		ui, err = d.readUint(r)
		*dstt = uint8(ui)
	case *uint16:
		ui, err = d.readUint(r)
		*dstt = uint16(ui)
	case *uint32:
		ui, err = d.readUint(r)
		*dstt = uint32(ui)
	case *uint64:
		*dstt, err = d.readUint(r)
	case *float32:
		var f float64
		f, err = d.readFloat(r, 32)
		*dstt = float32(f)
	case *float64:
		*dstt, err = d.readFloat(r, 64)
	case encoding.TextUnmarshaler:
		if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
			break
		}
		err = dstt.UnmarshalText(d.scratch)
	case encoding.BinaryUnmarshaler:
		if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
			break
		}
		err = dstt.UnmarshalBinary(d.scratch)
	case *interface{}: // this case is more or less black magic
		v := reflect.Indirect(reflect.ValueOf(dstt))
		if !v.CanSet() {
			err = fmt.Errorf("cannot decode into type %T", dstt)
			break
		}
		var rcvT reflect.Type
		if v.IsNil() {
			switch typ {
			case riSimpleStr, riBulkStr:
				rcvT = stringT
			case riInt:
				rcvT = intT
			default:
				// errors don't get scanned, arrays get scanned in a different
				// method
				panic(fmt.Sprintf("weird typ value: %#v", typ))
			}
		} else {
			rcvT = v.Elem().Type()
		}
		rcv := reflect.New(rcvT)
		err = d.scanInto(rcv.Interface(), r, typ)
		v.Set(rcv.Elem())
	default:
		err = fmt.Errorf("cannot decode into type %T", dstt)
	}

	// no matter what we *must* finish reading the io.Reader. The io.Reader must
	// allow for Read to be called on it after an io.EOF is hit.
	io.Copy(d.discard, r)

	if cap(d.scratch) > d.BufSize {
		d.scratch = make([]byte, d.BufSize)
	}

	return err
}

//func dv(v reflect.Value) string {
//	return fmt.Sprintf("type:%q val:%v canSet:%v canAddr:%v", v.Type().String(), v, v.CanSet(), v.CanAddr())
//}

func (d *Decoder) scanArrayInto(v reflect.Value, size int) error {
	// set up some logic so we can make sure we discard all remaining elements
	// in an array in the event of a decoding error part-way through. If there's
	// a network error we're kind of screwed no matter what
	var decoded int
	dDecode := func(v interface{}) error {
		decoded++
		return d.Decode(v)
	}
	defer func() {
		for i := decoded; i < size; i++ {
			d.Decode(nil)
		}
	}()

	v = reflect.Indirect(v)
	if !v.IsValid() {
		// not valid means a straight up nil was passed in, so we return
		// immediately, the defer will discard everything
		return nil

	} else if !v.CanSet() {
		// this will also use the defer to discard everything
		return fmt.Errorf("cannot decode redis array into %v, can't set", v.Type())

	} else if v.Type() == emptyInterfaceT {
		if v.IsNil() {
			vslice := reflect.MakeSlice(emptyInterfaceSliceT, size, size)
			v.Set(vslice)
			// we can't do SetLen or SetCap from this point on, but vslice is the
			// exact length and capacity needed already so it shouldn't matter too
			// much
			v = vslice
		} else {
			// v.Elem isn't settable, but we can't use v because it's an empty
			// interface. So make a new pointer to v.Elem (which we can modify)
			// and use that.
			vcp := reflect.New(v.Elem().Type())
			vcp.Elem().Set(v.Elem())
			decoded = size // the scanArrayInto call will have taken care of this
			if err := d.scanArrayInto(vcp, size); err != nil {
				return err
			}
			v.Set(vcp.Elem())
			return nil
		}
	}

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
			vindex := v.Index(i).Addr().Interface()
			if err := dDecode(vindex); err != nil {
				return err
			}
		}

	case reflect.Map:
		if size%2 != 0 {
			return fmt.Errorf("cannot decode redis array with odd number of elements into map")
		} else if v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		}

		for i := 0; i < size; i += 2 {
			kv := reflect.New(v.Type().Key())
			if err := dDecode(kv.Interface()); err != nil {
				return err
			}

			vv := v.MapIndex(kv.Elem())
			if !vv.IsValid() || vv.Kind() != reflect.Interface {
				vv = reflect.New(v.Type().Elem())
			} else {
				vvcp := reflect.New(vv.Elem().Type())
				vvcp.Elem().Set(vv.Elem())
				vv = vvcp
			}
			if err := dDecode(vv.Interface()); err != nil {
				return err
			}
			v.SetMapIndex(kv.Elem(), vv.Elem())
		}

	default:
		return fmt.Errorf("cannot decode redis array into %v", v.Type())
	}
	return nil
}

func (d *Decoder) readInt(r io.Reader) (int64, error) {
	var err error
	if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(d.scratch), 10, 64)
}

func (d *Decoder) readUint(r io.Reader) (uint64, error) {
	var err error
	if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(d.scratch), 10, 64)
}

func (d *Decoder) readFloat(r io.Reader, precision int) (float64, error) {
	var err error
	if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(d.scratch), precision)
}

func readAllAppend(r io.Reader, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	// TODO a side effect of this is that the given b will be re-allocated if
	// it's less than bytes.MinRead. Since this b could be all the way from the
	// user we can't guarantee it within the library. Would b enice to not have
	// that weird edge-case
	_, err := buf.ReadFrom(r)
	return buf.Bytes(), err
}
