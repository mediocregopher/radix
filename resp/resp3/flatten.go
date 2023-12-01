package resp3

import (
	"encoding"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/mediocregopher/radix/v4/internal/bytesutil"
	"github.com/mediocregopher/radix/v4/resp"
)

// cleanFloatStr is needed because go likes to marshal infinity values as
// "+Inf" and "-Inf", but we need "inf" and "-inf".
func cleanFloatStr(str string) string {
	str = strings.ToLower(str)
	if str[0] == '+' { // "+inf"
		str = str[1:]
	}
	return str
}

// Source: https://cs.opensource.google/go/go/+/refs/tags/go1.19.2:src/encoding/json/encode.go;drc=d0b0b10b5cbb28d53403c2bd6af343581327e946;l=339
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	}
	return false
}

// Flatten accepts any type accepted by Marshal, except a resp.Marshaler, and
// converts it into a flattened array of strings. For example:
//
//	Flatten(5) -> {"5"}
//	Flatten(nil) -> {""}
//	Flatten([]string{"a","b"}) -> {"a", "b"}
//	Flatten(map[string]int{"a":5,"b":10}) -> {"a","5","b","10"}
//	Flatten([]map[int]float64{{1:2, 3:4},{5:6},{}}) -> {"1","2","3","4","5","6"})
func Flatten(i interface{}, o *resp.Opts) ([]string, error) {
	f := flattener{
		opts: o,
		out:  make([]string, 0, 8),
	}
	err := f.flatten(i)
	return f.out, err
}

type flattener struct {
	// Opts is not really used and is mostly here for future compatibility. It
	// is secretly allowed to be nil, but we don't tell the users that.
	opts *resp.Opts
	out  []string
}

// emit always returns nil for convenience.
func (f *flattener) emit(str string) error {
	f.out = append(f.out, str)
	return nil
}

func (f *flattener) flatten(i interface{}) error {
	switch i := i.(type) {
	case []byte:
		return f.emit(string(i))
	case string:
		return f.emit(i)
	case []rune:
		return f.emit(string(i))
	case bool:
		if i {
			return f.emit("1")
		}
		return f.emit("0")
	case float32:
		s := strconv.FormatFloat(float64(i), 'f', -1, 32)
		return f.emit(cleanFloatStr(s))
	case float64:
		s := strconv.FormatFloat(i, 'f', -1, 64)
		return f.emit(cleanFloatStr(s))
	case *big.Float:
		return f.flatten(*i)
	case big.Float:
		return f.emit(cleanFloatStr(i.Text('f', -1)))
	case nil:
		return f.emit("")
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		i64 := bytesutil.AnyIntToInt64(i)
		return f.emit(strconv.FormatInt(i64, 10))
	case *big.Int:
		return f.flatten(*i)
	case big.Int:
		return f.emit(i.Text(10))
	case error:
		return f.emit(i.Error())
	case resp.LenReader:
		b, err := ioutil.ReadAll(i)
		if err != nil {
			return err
		}
		return f.emit(string(b))
	case io.Reader:
		b, err := ioutil.ReadAll(i)
		if err != nil {
			return err
		}
		return f.emit(string(b))
	case encoding.TextMarshaler:
		b, err := i.MarshalText()
		if err != nil {
			return err
		}
		return f.emit(string(b))
	case encoding.BinaryMarshaler:
		b, err := i.MarshalBinary()
		if err != nil {
			return err
		}
		return f.emit(string(b))
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
			return f.emit("")
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
		if f.opts != nil && f.opts.Deterministic {
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
			tag, tagOpts := parseTag(ft.Tag.Get("redis"))
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

			isEmpty := isEmptyValue(fv)
			if isEmpty && tagOpts.Contains("omitempty") {
				continue
			}

			keyName := ft.Name
			if tag != "" {
				keyName = tag
			}
			_ = f.emit(keyName)

			if isEmpty {
				// Return "", setting empty value
				return f.emit("")
			}

			if err := f.flatten(fv.Interface()); err != nil {
				return err
			}
		}
		return nil
	}

	return fmt.Errorf("could not flatten value of type %T", i)
}
