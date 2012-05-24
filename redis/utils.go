package redis

import (
	"fmt"
	"reflect"
	"strconv"
)

//* Useful helpers

// argToRedis formats an argument value into a Redis styled byte string.
func argToRedis(v interface{}) (bs []byte) {
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
				bs = append(bs, argToRedis(rv.Index(i).Interface())...)
			}
		case reflect.Map:
			rv := reflect.ValueOf(vt)
			keys := rv.MapKeys()
			for _, k := range keys {
				bs = append(bs, argToRedis(k)...)
				bs = append(bs, argToRedis(rv.MapIndex(k).Interface())...)
			}
		default:
			vs := fmt.Sprintf("%v", vt)
			bs = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(vs), vs))
		}
	}

	return bs
}

// createRequest creates a Redis request for the given command and its arguments.
func createRequest(cmd command) []byte {
	var req []byte

	// Calculate number of arguments.
	argsLen := 1
	for _, arg := range cmd.args {
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

	// number of arguments.
	req = append(req, []byte(fmt.Sprintf("*%d\r\n", argsLen))...)

	// command name
	req = append(req, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.cmd), cmd.cmd))...)

	// arguments
	for _, arg := range cmd.args {
		req = append(req, argToRedis(arg)...)
	}

	return req
}
