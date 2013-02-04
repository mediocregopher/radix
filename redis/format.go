package redis

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
)

var delim []byte = []byte{'\r', '\n'}

type request struct {
	cmd  string
	args []interface{}
}

// formatArg formats the given argument to a Redis-styled argument byte slice.
func formatArg(v interface{}) []byte {
	var b, bs []byte

	switch vt := v.(type) {
	case []byte:
		bs = vt
	case string:
		bs = []byte(vt)
	case bool:
		if vt {
			bs = []byte{'1'}
		} else {
			bs = []byte{'0'}
		}
	case nil:
		// empty byte slice
	case int:
		bs = []byte(strconv.Itoa(vt))
	case int8:
		bs = []byte(strconv.FormatInt(int64(vt), 10))
	case int16:
		bs = []byte(strconv.FormatInt(int64(vt), 10))
	case int32:
		bs = []byte(strconv.FormatInt(int64(vt), 10))
	case int64:
		bs = []byte(strconv.FormatInt(vt, 10))
	case uint:
		bs = []byte(strconv.FormatUint(uint64(vt), 10))
	case uint8:
		bs = []byte(strconv.FormatUint(uint64(vt), 10))
	case uint16:
		bs = []byte(strconv.FormatUint(uint64(vt), 10))
	case uint32:
		bs = []byte(strconv.FormatUint(uint64(vt), 10))
	case uint64:
		bs = []byte(strconv.FormatUint(vt, 10))
	default:
		// Fallback to reflect-based.
		switch reflect.TypeOf(vt).Kind() {
		case reflect.Slice:
			rv := reflect.ValueOf(vt)
			for i := 0; i < rv.Len(); i++ {
				bs = append(bs, formatArg(rv.Index(i).Interface())...)
			}

			return bs
		case reflect.Map:
			rv := reflect.ValueOf(vt)
			keys := rv.MapKeys()
			for _, k := range keys {
				bs = append(bs, formatArg(k.Interface())...)
				bs = append(bs, formatArg(rv.MapIndex(k).Interface())...)
			}

			return bs
		default:
			var buf bytes.Buffer

			fmt.Fprint(&buf, v)
			bs = buf.Bytes()
		}
	}

	b = append(b, '$')
	b = append(b, []byte(strconv.Itoa(len(bs)))...)
	b = append(b, delim...)
	b = append(b, bs...)
	b = append(b, delim...)
	return b
}

// createRequest creates a request string from the given requests.
func createRequest(requests ...*request) []byte {
	var total []byte

	for _, req := range requests {
		var s []byte

		// Calculate number of arguments.
		argsLen := 1
		for _, arg := range req.args {
			kind := reflect.TypeOf(arg).Kind()
			switch kind {
			case reflect.Slice:
				argsLen += reflect.ValueOf(arg).Len()
			case reflect.Map:
				argsLen += reflect.ValueOf(arg).Len() * 2
			default:
				argsLen++
			}
		}

		// number of arguments
		s = append(s, '*')
		s = append(s, []byte(strconv.Itoa(argsLen))...)
		s = append(s, delim...)

		// command
		s = append(s, '$')
		s = append(s, []byte(strconv.Itoa(len(req.cmd)))...)
		s = append(s, delim...)
		s = append(s, []byte(req.cmd)...)
		s = append(s, delim...)

		// arguments
		for _, arg := range req.args {
			s = append(s, formatArg(arg)...)
		}

		total = append(total, s...)
	}

	return total
}
