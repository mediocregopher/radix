package redis

import (
	"strconv"
	"strings"
)

//* Value

type Value struct {
	string *string
	int    *int64
}

// String returns the reply value as string.
// It panics if, if there was no string reply.
func (v Value) String() string {
	return *v.string
}

// Bytes returns the reply value as byte slice.
// It panics, if there was no string reply.
func (v Value) Bytes() []byte {
	return []byte(*v.string)
}

// Int64 returns the reply value as int64.
// It panics,  if there was no integer reply.
func (v Value) Int64() int64 {
	return *v.int
}

// Bool return the value as bool.
func (v Value) Bool() bool {
	if b, err := strconv.ParseBool(v.String()); err == nil {
		return b
	}

	return false
}

// Int returns the value as int.
func (v Value) Int() int {
	if i, err := strconv.Atoi(v.String()); err == nil {
		return i
	}

	return 0
}


// Uint64 returns the value as uint64.
func (v Value) Uint64() uint64 {
	if i, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
		return i
	}

	return 0
}

// Float64 returns the value as float64.
func (v Value) Float64() float64 {
	if f, err := strconv.ParseFloat(v.String(), 64); err == nil {
		return f
	}

	return 0.0
}



// StringSlice returns the value as slice of strings when seperated by CRLF.
func (v Value) StringSlice() []string {
	return strings.Split(v.String(), "\r\n")
}

// StringMap returns the value as a map of strings when seperated by CRLF and colons between key and value.
func (v Value) StringMap() map[string]string {
	tmp := v.StringSlice()
	m := make(map[string]string, len(tmp))

	for _, s := range tmp {
		kv := strings.Split(s, ":")

		if len(kv) > 1 {
			m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}

	return m
}

// Unpack removes the braces of a list value.
func (v Value) Unpack() Value {
	if len(v) > 2 && v[0] == '[' && v[len(v)-1] == ']' {
		return Value(v[1 : len(v)-1])
	}

	return v
}

//* Special values

// KeyValue combines a key and a value for blocked lists.
type KeyValue struct {
	Key   string
	Value Value
}
