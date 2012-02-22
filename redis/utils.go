package redis

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

type command struct {
	cmd  string
	args []interface{}
}

//* Useful helpers

// argToRedis formats an argument value into a Redis styled byte slice argument.
func argToRedis(v interface{}) []byte {
	var bs []byte

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

// Return error with properly formatted message.
func newError(msg string) error {
	return errors.New("redis: " + msg)
}