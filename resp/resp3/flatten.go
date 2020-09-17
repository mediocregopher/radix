package resp3

import (
	"bytes"
	"encoding"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"reflect"
	"sort"
	"strconv"

	"github.com/mediocregopher/radix/v3/internal/bytesutil"
	"github.com/mediocregopher/radix/v3/resp"
)

var boolStrs = [][]byte{
	{'0'},
	{'1'},
}

// cleanFloatStr is needed because go likes to marshal infinity values as
// "+Inf" and "-Inf", but we need "inf" and "-inf".
func cleanFloatStr(b []byte) []byte {
	b = bytes.ToLower(b)
	if b[0] == '+' { // "+inf"
		b = b[1:]
	}
	return b
}

// Flatten accepts any type accepted by Any.MarshalRESP, except a
// resp.Marshaler, and converts it into a flattened array of byte slices. For
// example:
//
//	Flatten(5) -> {"5"}
//	Flatten(nil) -> {""}
//	Flatten([]string{"a","b"}) -> {"a", "b"}
//	Flatten(map[string]int{"a":5,"b":10}) -> {"a","5","b","10"}
//	Flatten([]map[int]float64{{1:2, 3:4},{5:6},{}}) -> {"1","2","3","4","5","6"})
//
func Flatten(i interface{}) ([][]byte, error) {
	f := flattener{
		out: make([][]byte, 0, 8),
	}
	err := f.flatten(i)
	return f.out, err
}

type flattener struct {
	out           [][]byte
	deterministic bool
}

// emit always returns nil for convenience
func (f *flattener) emit(b []byte) error {
	f.out = append(f.out, b)
	return nil
}

func (f *flattener) flatten(i interface{}) error {
	switch i := i.(type) {
	case []byte:
		return f.emit(i)
	case string:
		return f.emit([]byte(i))
	case []rune:
		return f.emit([]byte(string(i)))
	case bool:
		if i {
			return f.emit(boolStrs[1])
		}
		return f.emit(boolStrs[0])
	case float32:
		b := strconv.AppendFloat([]byte(nil), float64(i), 'f', -1, 32)
		return f.emit(cleanFloatStr(b))
	case float64:
		b := strconv.AppendFloat([]byte(nil), i, 'f', -1, 64)
		return f.emit(cleanFloatStr(b))
	case *big.Float:
		return f.flatten(*i)
	case big.Float:
		b := i.Append([]byte(nil), 'f', -1)
		return f.emit(cleanFloatStr(b))
	case nil:
		return f.emit([]byte(nil))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		i64 := bytesutil.AnyIntToInt64(i)
		b := strconv.AppendInt([]byte(nil), i64, 10)
		return f.emit(b)
	case *big.Int:
		return f.flatten(*i)
	case big.Int:
		b := i.Append([]byte(nil), 10)
		return f.emit(b)
	case error:
		b := append([]byte(nil), i.Error()...)
		return f.emit(b)
	case resp.LenReader:
		b := bytesutil.Expand([]byte(nil), i.Len())
		if _, err := io.ReadFull(i, b); err != nil {
			return err
		}
		return f.emit(b)
	case io.Reader:
		// TODO use bytes pool here?
		b, err := ioutil.ReadAll(i)
		if err != nil {
			return err
		}
		return f.emit(b)
	case encoding.TextMarshaler:
		b, err := i.MarshalText()
		if err != nil {
			return err
		}
		return f.emit(b)
	case encoding.BinaryMarshaler:
		b, err := i.MarshalBinary()
		if err != nil {
			return err
		}
		return f.emit(b)
	}

	vv := reflect.ValueOf(i)
	if vv.Kind() != reflect.Ptr {
		// ok
	} else if vv.IsNil() {
		switch vv.Type().Elem().Kind() {
		case reflect.Slice, reflect.Array, reflect.Map, reflect.Struct:
			// if an agg type is nil then don't emit anything
			return nil
		default:
			// otherwise emit empty string
			return f.emit([]byte(nil))
		}
	} else {
		return f.flatten(vv.Elem().Interface())
	}

	switch vv.Kind() {
	case reflect.Slice, reflect.Array:
		l := vv.Len()
		for i := 0; i < l; i++ {
			if err := f.flatten(vv.Index(i).Interface()); err != nil {
				return err
			}
		}
		return nil

	case reflect.Map:
		kkv := vv.MapKeys()
		if f.deterministic {
			// This is hacky af but basically works
			sort.Slice(kkv, func(i, j int) bool {
				return fmt.Sprint(kkv[i].Interface()) < fmt.Sprint(kkv[j].Interface())
			})
		}

		for _, kv := range kkv {
			if err := f.flatten(kv.Interface()); err != nil {
				return err
			} else if err := f.flatten(vv.MapIndex(kv).Interface()); err != nil {
				return err
			}
		}
		return nil

	case reflect.Struct:
		tt := vv.Type()
		l := vv.NumField()
		for i := 0; i < l; i++ {
			ft, fv := tt.Field(i), vv.Field(i)
			tag := ft.Tag.Get("redis")
			if ft.Anonymous {
				if fv = reflect.Indirect(fv); !fv.IsValid() { // fv is nil
					continue
				} else if err := f.flatten(fv.Interface()); err != nil {
					return err
				}
				continue
			} else if ft.PkgPath != "" || tag == "-" {
				continue // unexported
			}

			keyName := ft.Name
			if tag != "" {
				keyName = tag
			}
			f.emit([]byte(keyName))

			if err := f.flatten(fv.Interface()); err != nil {
				return err
			}
		}
		return nil
	}

	return fmt.Errorf("could not flatten value of type %T", i)
}
