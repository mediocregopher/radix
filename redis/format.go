package redis

import (
	"fmt"
	"reflect"
	"strconv"
	"bytes"
)

const (
    dollar byte = 36
    colon byte = 58
    minus byte = 45
    plus byte = 43
    star byte = 42
)

var delim []byte = []byte{13, 10}

// formatArg formats the given argument to a Redis-styled argument byte slice.
func formatArg(v interface{}) []byte {
	var b, bs []byte
	recursion := false // recursive return values are appended directly

	switch vt := v.(type) {
	case []byte:
		bs = vt
	case string:
		bs = []byte(vt)
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
	case bool:
		if vt {
			bs = []byte{49}
		} else {
			bs = []byte{48}
		}
	case nil:
		// empty byte slice
	default:
		// Fallback to reflect-based.
		switch reflect.TypeOf(vt).Kind() {
		case reflect.Slice:
			recursion = true
			rv := reflect.ValueOf(vt)
			for i := 0; i < rv.Len(); i++ {
				bs = append(bs, formatArg(rv.Index(i).Interface())...)
			}
		case reflect.Map:
			recursion = true
			rv := reflect.ValueOf(vt)
			keys := rv.MapKeys()
			for _, k := range keys {
				bs = append(bs, formatArg(k.Interface())...)
				bs = append(bs, formatArg(rv.MapIndex(k).Interface())...)
			}
		default:
            var buf bytes.Buffer

            fmt.Fprint(&buf, v)
            bs = buf.Bytes()
		}
	}

	if recursion {
		b = append(b, bs...)
	} else {
		b = append(b, dollar)
		b = append(b, []byte(strconv.Itoa(len(bs)))...)
		b = append(b, delim...)
		b = append(b, bs...)
		b = append(b, delim...)
	}

	return b
}


// createRequest creates a Redis request for the given call and its arguments.
func createRequest(call call) []byte {
	var req []byte

	// Calculate number of arguments.
	argsLen := 1
	for _, arg := range call.args {
		switch arg.(type) {
		case []byte:
			argsLen++
		default:
			// Fallback to reflect-based.
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
	}

	// number of arguments
	req = append(req, star)
	req = append(req, []byte(strconv.Itoa(argsLen))...)
	req = append(req, delim...)

	// command
	req = append(req, dollar)
	req = append(req, []byte(strconv.Itoa(len(call.cmd)))...)
	req = append(req, delim...)
	req = append(req, []byte(call.cmd)...)
	req = append(req, delim...)

	// arguments
	for _, arg := range call.args {
		req = append(req, formatArg(arg)...)
	}

	return req
}

/*
// formatArg formats an argument value into a Redis styled byte string.
func formatArg(v interface{}) (bs []byte) {
	switch vt := v.(type) {
	case string:
		bs = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len([]byte(vt)), []byte(vt)))
	case []byte:
		bs = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(vt), vt))
	case int:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.Itoa(vt))), vt))
	case int8:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.FormatInt(int64(vt), 10))), vt))
	case int16:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.FormatInt(int64(vt), 10))), vt))
	case int32:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.FormatInt(int64(vt), 10))), vt))
	case int64:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.FormatInt(vt, 10))), vt))
	case uint:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.FormatUint(uint64(vt), 10))), vt))
	case uint8:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.FormatUint(uint64(vt), 10))), vt))
	case uint16:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.FormatUint(uint64(vt), 10))), vt))
	case uint32:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.FormatUint(uint64(vt), 10))), vt))
	case uint64:
		bs = []byte(fmt.Sprintf("$%d\r\n%d\r\n", len([]byte(strconv.FormatUint(vt, 10))), vt))
	case bool:
		if vt {
			bs = []byte("$1\r\n1\r\n")
		} else {
			bs = []byte("$1\r\n0\r\n")
		}
	default:
		// Fallback to reflect-based.
		switch reflect.TypeOf(vt).Kind() {
		case reflect.Slice:
			rv := reflect.ValueOf(vt)
			for i := 0; i < rv.Len(); i++ {
				bs = append(bs, formatArg(rv.Index(i).Interface())...)
			}
		case reflect.Map:
			rv := reflect.ValueOf(vt)
			keys := rv.MapKeys()
			for _, k := range keys {
				bs = append(bs, formatArg(k.Interface())...)
				bs = append(bs, formatArg(rv.MapIndex(k).Interface())...)
			}
		default:
			vs := fmt.Sprintf("%v", vt)
			bs = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(vs), vs))
		}
	}

	return bs
}

// createRequest creates a Redis request for the given call and its arguments.
func createRequest(call call) []byte {
	var req []byte

	// Calculate number of arguments.
	argsLen := 1
	for _, arg := range call.args {
		switch arg.(type) {
		case []byte:
			argsLen++
		default:
			// Fallback to reflect-based.
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
	}

	// number of arguments
	req = append(req, []byte(fmt.Sprintf("*%d\r\n", argsLen))...)

	// command
	req = append(req, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(call.cmd), call.cmd))...)

	// arguments
	for _, arg := range call.args {
		req = append(req, formatArg(arg)...)
	}

	return req
}
*/