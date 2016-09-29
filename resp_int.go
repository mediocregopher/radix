package radix

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

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

type riType int

const (
	riSimpleStr riType = 1 << iota
	riBulkStr
	riAppErr // An error returned by redis, e.g. WRONGTYPE
	riInt
	riArray

	// special types
	riStr = riSimpleStr | riBulkStr
)

type respInt struct {
	riType

	// Exactly one of these must be set
	arrayHeaderSize int
	body            []byte
	isNil           bool
}

////////////////////////////////////////////////////////////////////////////////

type respIntBuf []respInt

func (rib *respIntBuf) write(ri respInt) {
	*rib = append(*rib, ri)
}

func (rib *respIntBuf) pop() respInt {
	ri := (*rib)[0]
	*rib = (*rib)[1:]
	return ri
}

func (rib *respIntBuf) reset() {
	*rib = (*rib)[:0]
}

////////////////////////////////////////////////////////////////////////////////
// reading

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

func (rib *respIntBuf) srcResp(r Resp) {
	rib.write(r.respInt)
	for i := range r.arr {
		rib.srcResp(r.arr[i])
	}
}

func (rib *respIntBuf) srcReader(r *bufio.Reader) error {
	var ri respInt

	// the slice returned here should not be stored or modified, it's internal
	// to the bufio.Reader
	b, err := r.ReadSlice(delimEnd)
	if err != nil {
		return err
	}

	switch b[0] {
	case simpleStrPrefix[0]:
		ri.riType = riSimpleStr
	case errPrefix[0]:
		ri.riType = riAppErr
	case intPrefix[0]:
		ri.riType = riInt
	case bulkStrPrefix[0]:
		ri.riType = riBulkStr
	case arrayPrefix[0]:
		ri.riType = riArray
	}

	var size int
	if ri.riType == riBulkStr || ri.riType == riArray {
		size, err = strconv.Atoi(string(b[1 : len(b)-delimLen]))
		if err != nil {
			return fmt.Errorf("error parsing size header: %s", err)
		}

		if size == -1 {
			ri.isNil = true
			rib.write(ri)
			return nil
		}
	}

	if ri.riType == riArray {
		ri.arrayHeaderSize = size
		rib.write(ri)
		for i := 0; i < size; i++ {
			if err := rib.srcReader(r); err != nil {
				return err
			}
		}
		return nil
	}

	if ri.riType == riBulkStr {
		ri.body = make([]byte, size)
		if _, err := io.ReadFull(r, ri.body); err != nil {
			return err
		} else if _, err := r.ReadSlice(delimEnd); err != nil {
			return err
		}
		rib.write(ri)
		return nil
	}

	b = b[1 : len(b)-delimLen]
	ri.body = make([]byte, len(b))
	copy(ri.body, b)
	rib.write(ri)
	return nil
}

func (rib *respIntBuf) srcAny(m interface{}) {
	switch mt := m.(type) {
	case []byte:
		rib.write(respInt{riType: riBulkStr, body: mt})
	case string:
		rib.write(respInt{riType: riBulkStr, body: []byte(mt)})
	case bool:
		if mt {
			rib.write(respInt{riType: riInt, body: []byte("1")})
		}
		rib.write(respInt{riType: riInt, body: []byte("0")})
	case nil:
		rib.write(respInt{riType: riBulkStr, isNil: true})
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		i := anyIntToInt64(mt)
		rib.write(respInt{riType: riInt, body: []byte(strconv.FormatInt(i, 10))})
	case float32:
		ft := strconv.FormatFloat(float64(mt), 'f', -1, 32)
		rib.write(respInt{riType: riBulkStr, body: []byte(ft)})
	case float64:
		ft := strconv.FormatFloat(mt, 'f', -1, 64)
		rib.write(respInt{riType: riBulkStr, body: []byte(ft)})
	case error:
		rib.write(respInt{riType: riAppErr, body: []byte(mt.Error())})

	// We duplicate the below code here a bit, since this is the common case and
	// it'd be better to not get the reflect package involved here
	case []interface{}:
		rib.write(respInt{riType: riArray, arrayHeaderSize: len(mt)})
		for i := range mt {
			rib.srcAny(mt[i])
		}

	case *Resp:
		rib.srcResp(*mt)
	case Resp:
		rib.srcResp(mt)

	case []Resp:
		rib.write(respInt{riType: riArray, arrayHeaderSize: len(mt)})
		for i := range mt {
			rib.srcResp(mt[i])
		}

	default:
		// Fallback to reflect-based.
		switch reflect.TypeOf(m).Kind() {
		case reflect.Slice:
			rm := reflect.ValueOf(mt)
			l := rm.Len()
			rib.write(respInt{riType: riArray, arrayHeaderSize: l})
			for i := 0; i < l; i++ {
				rib.srcAny(rm.Index(i).Interface())
			}

		case reflect.Map:
			rm := reflect.ValueOf(mt)
			rib.write(respInt{riType: riArray, arrayHeaderSize: rm.Len() * 2})
			for _, k := range rm.MapKeys() {
				rib.srcAny(k.Interface())
				rib.srcAny(rm.MapIndex(k).Interface())
			}

		default:
			rib.write(respInt{riType: riBulkStr, body: []byte(fmt.Sprint(m))})
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// writing

func (rib *respIntBuf) dstWriter(w *bufio.Writer) error {
	ri := rib.pop()

	var err error
	writeBytes := func(b []byte) {
		if err != nil || len(b) == 0 {
			return
		}
		_, err = w.Write(b)
	}

	writeInt := func(i int) {
		if err != nil {
			return
		}
		_, err = fmt.Fprintf(w, "%d", i)
	}

	switch ri.riType {
	case riSimpleStr:
		writeBytes(simpleStrPrefix)
		writeBytes(ri.body)
		writeBytes(delim)

	case riBulkStr:
		if ri.isNil {
			writeBytes(nilBulkStr)
			break
		}

		writeBytes(bulkStrPrefix)
		writeInt(len(ri.body))
		writeBytes(delim)
		writeBytes(ri.body)
		writeBytes(delim)

	case riAppErr:
		writeBytes(errPrefix)
		writeBytes(ri.body)
		writeBytes(delim)

	case riInt:
		writeBytes(intPrefix)
		writeBytes(ri.body)
		writeBytes(delim)

	case riArray:
		if ri.isNil {
			writeBytes(nilArray)
			break
		}

		writeBytes(arrayPrefix)
		writeInt(ri.arrayHeaderSize)
		writeBytes(delim)
		for i := 0; i < ri.arrayHeaderSize; i++ {
			if err != nil {
				break
			}
			err = rib.dstWriter(w)
		}
	}

	if err != nil {
		return err
	}

	return w.Flush()
}

func (rib *respIntBuf) dstResp() Resp {
	r := Resp{respInt: rib.pop()}
	if r.riType == riAppErr {
		r.Err = errors.New(string(r.body))

	} else if r.riType == riArray {
		r.arr = make([]Resp, r.arrayHeaderSize)
		for i := range r.arr {
			r.arr[i] = rib.dstResp()
		}
	}

	return r
}

////////////////////////////////////////////////////////////////////////////////
// util

func cpFlattened(dst, src respIntBuf) {
	var total int
	for _, ri := range src {
		if ri.riType == riArray {
			continue
		}
		total++
	}

	dst.write(respInt{riType: riArray, arrayHeaderSize: total})
	for _, ri := range src {
		if ri.riType == riArray {
			continue
		}
		dst.write(ri)
	}
}

func mapToStrs(rip respIntBuf) {
	for i, ri := range rip {
		// all types are simply held in body, and don't need any extra logic
		// done to them except to change the riType field, except array which is
		// a problem child
		if ri.riType == riArray {
			panic("can't translate resp array to resp string")
		}
		ri.riType = riBulkStr
		rip[i] = ri
	}
}
