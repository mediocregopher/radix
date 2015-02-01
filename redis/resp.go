package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
)

var (
	delim    = []byte{'\r', '\n'}
	delimEnd = delim[len(delim)-1]
)

// RespType is a field on every Resp which indicates the type of the data it
// contains
type RespType int

// Different RespTypes. You can check if a message is of one or more types using
// the IsType method on Resp
const (
	SimpleStr RespType = 1 << iota
	BulkStr
	IOErr  // An error which prevented reading/writing, e.g. connection close
	AppErr // An error returned by redis, e.g. WRONGTYPE
	Int
	Array
	Nil

	// Str combines both SimpleStr and BulkStr, which are considered strings to
	// the Str() method.  This is what you want to give to IsType when
	// determining if a response is a string
	Str = SimpleStr | BulkStr

	// Err combines both IOErr and AppErr, which both indicate that the Err
	// field on their Resp is filled. To determine if a Resp is an error you'll
	// most often want to simply check if the Err field on it is nil
	Err = IOErr | AppErr
)

const (
	simpleStrPrefix = '+'
	errPrefix       = '-'
	intPrefix       = ':'
	bulkStrPrefix   = '$'
	arrayPrefix     = '*'
)

// Parse errors
var (
	errBadType  = errors.New("wrong type")
	errParse    = errors.New("parse error")
	errNotStr   = errors.New("could not convert to string")
	errNotInt   = errors.New("could not convert to int")
	errNotArray = errors.New("could not convert to array")
)

// Resp represents a single response or message being sent to/from a redis
// server. Each Resp has a type (see RespType and IsType) and a value. Values
// can be retrieved using any of the casting methods on this type (e.g. Str)
type Resp struct {
	typ RespType
	val interface{}
	raw []byte

	// Err indicates that this Resp signals some kind of error, either on the
	// connection level or the application level. Use IsType if you need to
	// determine which, otherwise you can simply check if this is nil
	Err error
}

// NewResp takes the given value and interprets it into a resp encoded byte
// stream
func NewResp(v interface{}) *Resp {
	return format(v, false)
}

// NewRespSimple is like NewResp except it encodes its string as a resp
// SimpleStr type, whereas NewResp will encode all strings as BulkStr
func NewRespSimple(s string) *Resp {
	b := append(make([]byte, 0, len(s)+3), '+')
	b = append(b, []byte(s)...)
	b = append(b, '\r', '\n')
	return &Resp{
		typ: SimpleStr,
		val: []byte(s),
		raw: b,
	}
}

// NewRespFlattenedStrings is like NewResp except it looks through the given
// value and converts any types (except slices/maps) into strings, and flatten any
// embedded slices/maps into a single slice. This is useful because commands to
// a redis server must be given as an array of bulk strings. If the argument
// isn't already in a slice/map it will be wrapped so that it is written as a
// Array of size one
func NewRespFlattenedStrings(v interface{}) *Resp {
	fv := flatten(v)
	return format(fv, true)
}

// newRespIOErr is a convenience method for making Resps to wrap io errors
func newRespIOErr(err error) *Resp {
	r := NewResp(err)
	r.typ = IOErr
	return r
}

// RespReader is a wrapper around an io.Reader which will read Resp messages off
// of the io.Reader
type RespReader struct {
	r *bufio.Reader
}

// NewRespReader creates and returns a new RespReader which will read from the
// given io.Reader. Once passed in the io.Reader shouldn't be read from by any
// other processes
func NewRespReader(r io.Reader) *RespReader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &RespReader{br}
}

// ReadResp attempts to read a message object from the given io.Reader, parse
// it, and return a Resp representing it
func (rr *RespReader) Read() *Resp {
	res, err := bufioReadResp(rr.r)
	if err != nil {
		res = formatErr(err)
		res.typ = IOErr
	}
	return res
}

func bufioReadResp(r *bufio.Reader) (*Resp, error) {
	b, err := r.Peek(1)
	if err != nil {
		return nil, err
	}
	switch b[0] {
	case simpleStrPrefix:
		return readSimpleStr(r)
	case errPrefix:
		return readError(r)
	case intPrefix:
		return readInt(r)
	case bulkStrPrefix:
		return readBulkStr(r)
	case arrayPrefix:
		return readArray(r)
	default:
		return nil, errBadType
	}
}

func readSimpleStr(r *bufio.Reader) (*Resp, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	return &Resp{typ: SimpleStr, val: b[1 : len(b)-2], raw: b}, nil
}

func readError(r *bufio.Reader) (*Resp, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	err = errors.New(string(b[1 : len(b)-2]))
	return &Resp{typ: AppErr, val: err, raw: b, Err: err}, nil
}

func readInt(r *bufio.Reader) (*Resp, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	i, err := strconv.ParseInt(string(b[1:len(b)-2]), 10, 64)
	if err != nil {
		return nil, errParse
	}
	return &Resp{typ: Int, val: i, raw: b}, nil
}

func readBulkStr(r *bufio.Reader) (*Resp, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseInt(string(b[1:len(b)-2]), 10, 64)
	if err != nil {
		return nil, errParse
	}
	if size < 0 {
		return &Resp{typ: Nil, raw: b}, nil
	}
	total := make([]byte, size)
	b2 := total
	var n int
	for len(b2) > 0 {
		n, err = r.Read(b2)
		if err != nil {
			return nil, err
		}
		b2 = b2[n:]
	}

	// There's a hanging \r\n there, gotta read past it
	trail := make([]byte, 2)
	for i := 0; i < 2; i++ {
		c, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		trail[i] = c
	}

	blens := len(b) + len(total)
	raw := make([]byte, 0, blens+2)
	raw = append(raw, b...)
	raw = append(raw, total...)
	raw = append(raw, trail...)
	return &Resp{typ: BulkStr, val: total, raw: raw}, nil
}

func readArray(r *bufio.Reader) (*Resp, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseInt(string(b[1:len(b)-2]), 10, 64)
	if err != nil {
		return nil, errParse
	}
	if size < 0 {
		return &Resp{typ: Nil, raw: b}, nil
	}

	arr := make([]*Resp, size)
	for i := range arr {
		m, err := bufioReadResp(r)
		if err != nil {
			return nil, err
		}
		arr[i] = m
		b = append(b, m.raw...)
	}
	return &Resp{typ: Array, val: arr, raw: b}, nil
}

// IsType returns whether or or not the reply is of a given type
//
//	isStr := r.IsType(redis.Str)
//
// Multiple types can be checked at the same time by or'ing the desired types
//
//	isStrOrInt := r.IsType(redis.Str | redis.Int)
//
func (r *Resp) IsType(t RespType) bool {
	return r.typ&t > 0
}

// WriteTo writes the resp encoded form of the Resp to the given writer,
// implementing the WriterTo interface
func (r *Resp) WriteTo(w io.Writer) (int64, error) {
	i, err := w.Write(r.raw)
	return int64(i), err
}

// Bytes returns a byte slice representing the value of the Resp. Only valid for
// a Resp of type Str. If r.Err != nil that will be returned.
func (r *Resp) Bytes() ([]byte, error) {
	if r.Err != nil {
		return nil, r.Err
	} else if !r.IsType(Str) {
		return nil, errBadType
	}

	if b, ok := r.val.([]byte); ok {
		return b, nil
	}
	return nil, errNotStr
}

// Str is a wrapper around Bytes which returns the result as a string instead of
// a byte slice
func (r *Resp) Str() (string, error) {
	b, err := r.Bytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Int returns an int64 representing the value of the Resp. Only valid for a
// Resp of type Int. If r.Err != nil that will be returned
func (r *Resp) Int() (int64, error) {
	if r.Err != nil {
		return 0, r.Err
	}
	if i, ok := r.val.(int64); ok {
		return i, nil
	}
	return 0, errNotInt
}

// Array returns the Resp slice encompassed by this Resp. Only valid for a Resp
// of type Array. If r.Err != nil that will be returned
func (r *Resp) Array() ([]*Resp, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	if a, ok := r.val.([]*Resp); ok {
		return a, nil
	}
	return nil, errNotArray
}

// List is a wrapper around Array which returns the result as a list of strings,
// calling Str() on each Resp which Array returns. Any errors encountered are
// immediately returned
func (r *Resp) List() ([]string, error) {
	m, err := r.Array()
	if err != nil {
		return nil, err
	}
	l := make([]string, len(m))
	for i := range m {
		s, err := m[i].Str()
		if err != nil {
			return nil, err
		}
		l[i] = s
	}
	return l, nil
}

// ListBytes is a wrapper around Array which returns the result as a list of
// byte slices, calling Bytes() on each Resp which Array returns. Any errors
// encountered are immediately returned
func (r *Resp) ListBytes() ([][]byte, error) {
	m, err := r.Array()
	if err != nil {
		return nil, err
	}
	l := make([][]byte, len(m))
	for i := range m {
		b, err := m[i].Bytes()
		if err != nil {
			return nil, err
		}
		l[i] = b
	}
	return l, nil
}

// Map is a wrapper around Array which returns the result as a map of strings,
// calling Str() on alternating key/values for the map. All value fields of type
// Nil will be treated as empty strings, keys must all be of type Str
func (r *Resp) Map() (map[string]string, error) {
	l, err := r.Array()
	if err != nil {
		return nil, err
	}
	if len(l)%2 != 0 {
		return nil, errors.New("reply has odd number of elements")
	}

	m := map[string]string{}
	for {
		if len(l) == 0 {
			return m, nil
		}
		k, v := l[0], l[1]
		l = l[2:]

		ks, err := k.Str()
		if err != nil {
			return nil, err
		}

		var vs string
		if v.IsType(Nil) {
			vs = ""
		} else if vs, err = v.Str(); err != nil {
			return nil, err
		}
		m[ks] = vs
	}
}

// String returns a string representation of the Resp. This method is for
// debugging, use Str() for reading a Str reply
func (r *Resp) String() string {
	var inner string
	switch r.typ {
	case AppErr:
		inner = fmt.Sprintf("AppErr %s", r.Err)
	case IOErr:
		inner = fmt.Sprintf("IOErr %s", r.Err)
	case BulkStr, SimpleStr:
		inner = fmt.Sprintf("Str %s", string(r.val.([]byte)))
	case Int:
		inner = fmt.Sprintf("Int %d", r.val.(int64))
	case Nil:
		inner = fmt.Sprintf("Nil")
	case Array:
		kids := r.val.([]*Resp)
		kidsStr := make([]string, len(kids))
		for i := range kids {
			kidsStr[i] = kids[i].String()
		}
		inner = strings.Join(kidsStr, " ")
	}
	return fmt.Sprintf("Resp(%s)", inner)
}

var typeOfBytes = reflect.TypeOf([]byte(nil))

func flatten(m interface{}) []interface{} {
	t := reflect.TypeOf(m)

	// If it's a byte-slice we don't want to flatten
	if t == typeOfBytes {
		return []interface{}{m}
	}

	switch t.Kind() {
	case reflect.Slice:
		rm := reflect.ValueOf(m)
		l := rm.Len()
		ret := make([]interface{}, 0, l)
		for i := 0; i < l; i++ {
			ret = append(ret, flatten(rm.Index(i).Interface())...)
		}
		return ret

	case reflect.Map:
		rm := reflect.ValueOf(m)
		l := rm.Len() * 2
		keys := rm.MapKeys()
		ret := make([]interface{}, 0, l)
		for _, k := range keys {
			kv := k.Interface()
			vv := rm.MapIndex(k).Interface()
			ret = append(ret, flatten(kv)...)
			ret = append(ret, flatten(vv)...)
		}
		return ret

	default:
		return []interface{}{m}
	}
}

func format(m interface{}, forceString bool) *Resp {
	switch mt := m.(type) {
	case []byte:
		return formatStr(mt)
	case string:
		return formatStr([]byte(mt))
	case bool:
		if mt {
			return formatStr([]byte("1"))
		}
		return formatStr([]byte("0"))
	case nil:
		if forceString {
			return formatStr([]byte{})
		}
		return formatNil()
	case int:
		return formatInt(int64(mt), forceString)
	case int8:
		return formatInt(int64(mt), forceString)
	case int16:
		return formatInt(int64(mt), forceString)
	case int32:
		return formatInt(int64(mt), forceString)
	case int64:
		return formatInt(mt, forceString)
	case uint:
		return formatInt(int64(mt), forceString)
	case uint8:
		return formatInt(int64(mt), forceString)
	case uint16:
		return formatInt(int64(mt), forceString)
	case uint32:
		return formatInt(int64(mt), forceString)
	case uint64:
		return formatInt(int64(mt), forceString)
	case float32:
		ft := strconv.FormatFloat(float64(mt), 'f', -1, 32)
		return formatStr([]byte(ft))
	case float64:
		ft := strconv.FormatFloat(mt, 'f', -1, 64)
		return formatStr([]byte(ft))
	case error:
		if forceString {
			return formatStr([]byte(mt.Error()))
		}
		return formatErr(mt)

	// We duplicate the below code here a bit, since this is the common case and
	// it'd be better to not get the reflect package involved here
	case []interface{}:
		l := len(mt)
		rl := make([]*Resp, 0, l)
		b := make([]byte, 0, l*1024)
		b = append(b, '*')
		b = append(b, []byte(strconv.Itoa(l))...)
		b = append(b, []byte("\r\n")...)
		for i := 0; i < l; i++ {
			r := format(mt[i], forceString)
			b = append(b, r.raw...)
			rl = append(rl, r)
		}
		return &Resp{typ: Array, val: rl, raw: b}

	case *Resp:
		return mt

	default:
		// Fallback to reflect-based.
		switch reflect.TypeOf(m).Kind() {
		case reflect.Slice:
			rm := reflect.ValueOf(mt)
			l := rm.Len()
			rl := make([]*Resp, 0, l)
			b := make([]byte, 0, l*1024)
			b = append(b, '*')
			b = append(b, []byte(strconv.Itoa(l))...)
			b = append(b, []byte("\r\n")...)
			for i := 0; i < l; i++ {
				vv := rm.Index(i).Interface()
				r := format(vv, forceString)
				rl = append(rl, r)
				b = append(b, r.raw...)
			}
			return &Resp{typ: Array, val: rl, raw: b}

		case reflect.Map:
			rm := reflect.ValueOf(mt)
			l := rm.Len() * 2
			rl := make([]*Resp, 0, l)
			b := make([]byte, 0, l*1024)
			b = append(b, '*')
			b = append(b, []byte(strconv.Itoa(l))...)
			b = append(b, []byte("\r\n")...)
			keys := rm.MapKeys()
			for _, k := range keys {
				kv := k.Interface()
				vv := rm.MapIndex(k).Interface()

				kr := format(kv, forceString)
				rl = append(rl, kr)
				b = append(b, kr.raw...)

				vr := format(vv, forceString)
				rl = append(rl, vr)
				b = append(b, vr.raw...)
			}
			return &Resp{typ: Array, val: rl, raw: b}

		default:
			return formatStr([]byte(fmt.Sprint(m)))
		}
	}
}

func formatStr(b []byte) *Resp {
	l := strconv.Itoa(len(b))
	bs := make([]byte, 0, len(l)+len(b)+5)
	bs = append(bs, bulkStrPrefix)
	bs = append(bs, []byte(l)...)
	bs = append(bs, delim...)
	bs = append(bs, b...)
	bs = append(bs, delim...)
	return &Resp{typ: BulkStr, val: b, raw: bs}
}

func formatErr(ierr error) *Resp {
	ierrstr := []byte(ierr.Error())
	bs := make([]byte, 0, len(ierrstr)+3)
	bs = append(bs, errPrefix)
	bs = append(bs, ierrstr...)
	bs = append(bs, delim...)
	return &Resp{typ: AppErr, val: ierr, raw: bs, Err: ierr}
}

func formatInt(i int64, forceString bool) *Resp {
	istr := strconv.FormatInt(i, 10)
	if forceString {
		return formatStr([]byte(istr))
	}
	bs := make([]byte, 0, len(istr)+3)
	bs = append(bs, intPrefix)
	bs = append(bs, istr...)
	bs = append(bs, delim...)
	return &Resp{typ: Int, val: i, raw: bs}
}

var nilFormatted = []byte("$-1\r\n")

func formatNil() *Resp {
	return &Resp{typ: Nil, val: nil, raw: nilFormatted}
}

// IsTimeout is a helper function for determining if an IOErr Resp was caused by
// a network timeout
func IsTimeout(r *Resp) bool {
	if r.IsType(IOErr) {
		t, ok := r.Err.(*net.OpError)
		return ok && t.Timeout()
	}
	return false
}
