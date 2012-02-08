package redis

import (
	"errors"
	"strconv"
	"strings"
)

//* Value

type Value []byte

// String returns the value as string (alternative to type conversion).
func (v Value) String() string {
	return string([]byte(v))
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

// Int64 returns the value as int64.
func (v Value) Int64() int64 {
	if i, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
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

// Bytes returns the value as byte slice.
func (v Value) Bytes() []byte {
	return []byte(v)
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

//* ResultSet

// ResultSet is the returned struct of commands.
type ResultSet struct {
	values     []Value
	resultSets []*ResultSet
	error      error
}

// OK returns true if the result set has nil error, otherwise false.
func (rs *ResultSet) OK() bool {
	if rs.error != nil {
		return false
	}

	return true
}

// Multi returns true if the result set contains
// multiple result sets.
func (rs *ResultSet) Multi() bool {
	return rs.resultSets != nil
}

// Len returns the number of returned values.
func (rs *ResultSet) Len() int {
	if rs.values == nil {
		return 0
	}

	return len(rs.values)
}

// At returns a wanted value by its index.
func (rs *ResultSet) At(i int) Value {
	if i < 0 || i >= len(rs.values) {
		return nil
	}

	return rs.values[i]
}

// Value returns the first value.
func (rs *ResultSet) Value() Value {
	return rs.At(0)
}

//UnpackedValue returns the first value unpacked.
func (rs *ResultSet) UnpackedValue() Value {
	return rs.At(0).Unpack()
}

// Values returns all values as slice.
func (rs *ResultSet) Values() []Value {
	if rs.values == nil {
		return nil
	}

	vs := make([]Value, len(rs.values))
	copy(vs, rs.values)
	return vs
}

// Ints returns all values as a slice of integers.
func (rs *ResultSet) Ints() []int {
	if rs.values == nil {
		return nil
	}

	ints := make([]int, len(rs.values))
	for i, v := range rs.values {
		ints[i] = v.Int()
	}

	return ints
}

// Strings returns all values as a slice of strings.
func (rs *ResultSet) Strings() []string {
	if rs.values == nil {
		return nil
	}

	strings := make([]string, len(rs.values))
	for i, v := range rs.values {
		strings[i] = string(v)
	}

	return strings
}

// UnpackedValues returns all values unpacked as slice.
func (rs *ResultSet) UnpackedValues() []Value {
	vs := rs.Values()

	for i, v := range vs {
		vs[i] = v.Unpack()
	}

	return vs
}

// Bool returns the first value as bool.
func (rs *ResultSet) Bool() bool {
	return rs.At(0).Bool()
}

// Int returns the first value as int.
func (rs *ResultSet) Int() int {
	return rs.At(0).Int()
}

// String returns the first value as string.
func (rs *ResultSet) String() string {
	return rs.At(0).String()
}

// Bytes returns the first value as byte slice.
func (rs *ResultSet) Bytes() []byte {
	return rs.Value().Bytes()
}

// KeyValue return the first value as key and the second as value.
func (rs *ResultSet) KeyValue() *KeyValue {
	return &KeyValue{
		Key:   rs.At(0).String(),
		Value: rs.At(1),
	}
}

// Apply iterates over the result values and
// applies the passed function for each one.
func (rs *ResultSet) Apply(f func(int, Value)) {
	for idx, v := range rs.values {
		f(idx, v)
	}
}

// Values iterates over the result values and
// applies the passed function for each one. The result
// is a slice of values returned by the functions.
func (rs *ResultSet) ApplySlice(f func(Value) interface{}) []interface{} {
	result := make([]interface{}, len(rs.values))

	for i, v := range rs.values {
		result[i] = f(v)
	}

	return result
}

// Hash returns the values of the result set as hash.
func (rs *ResultSet) Hash() Hash {
	var key string

	result := make(Hash)
	isVal := false

	for _, v := range rs.values {
		if isVal {
			// Write every second value.
			result[key] = v
			isVal = false
		} else {
			// First value is always a key.
			key = v.String()
			isVal = true
		}
	}

	return result
}

// ResultSetLen returns the number of result sets
// inside the result set.
func (rs *ResultSet) ResultSetLen() int {
	if rs.resultSets == nil {
		return 0
	}

	return len(rs.resultSets)
}

// ResultSetAt returns a result set by its index.
func (rs *ResultSet) ResultSetAt(i int) *ResultSet {
	if i < 0 || i >= len(rs.resultSets) {
		rs := &ResultSet{}
		rs.error = errors.New("illegal result set index")
		return rs
	}

	return rs.resultSets[i]
}

// MultiApply iterates over the result sets and
// applies the passed function for each one.
func (rs *ResultSet) MultiApply(f func(*ResultSet)) {
	for _, rs := range rs.resultSets {
		f(rs)
	}
}

// MultiApplySlice iterates over the result sets and
// performs the passed function for each one. The result
// is a slice of values returned by the functions.
func (rs *ResultSet) MultiApplySlice(f func(*ResultSet) interface{}) []interface{} {
	result := make([]interface{}, len(rs.resultSets))

	for idx, rs := range rs.resultSets {
		result[idx] = f(rs)
	}

	return result
}

// Error returns the error if the operation creating
// the result set failed or nil.
func (rs *ResultSet) Error() error {
	return rs.error
}

//* Future

// Future just waits for a result set
// returned somewhere in the future.
type Future struct {
	rsChan chan *ResultSet
}

// newFuture creates the new future.
func newFuture() *Future {
	return &Future{make(chan *ResultSet, 1)}
}

// setResultSet sets the result set.
func (f *Future) setResultSet(rs *ResultSet) {
	f.rsChan <- rs
}

// ResultSet returns the result set in the moment it is available.
func (f *Future) ResultSet() (rs *ResultSet) {
	rs = <-f.rsChan
	f.rsChan <- rs
	return
}
