package redis

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
		panic("redis: value index out of range")
	}

	return rs.values[i]
}

// Value returns the first value.
func (rs *ResultSet) Value() Value {
	if len(rs.values) == 0 {
		return nil
	}

	return rs.At(0)
}

//UnpackedValue returns the first value unpacked.
func (rs *ResultSet) UnpackedValue() Value {
	return rs.Value().Unpack()
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
	return rs.Value().Bool()
}

// Int returns the first value as int.
func (rs *ResultSet) Int() int {
	return rs.Value().Int()
}

// String returns the first value as string.
func (rs *ResultSet) String() string {
	return rs.Value().String()
}

// Bytes returns the first value as byte slice.
func (rs *ResultSet) Bytes() []byte {
	return rs.Value().Bytes()
}

// KeyValue return the first value as key and the second as value.
// Will panic if there are less that two values in the result set.
func (rs *ResultSet) KeyValue() *KeyValue {
	if rs.Len() < 2 {
		panic("redis: ResultSet does not have enough values for KeyValue")
	}

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
		panic("redis: result set index out of range")
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
type Future chan *ResultSet

// newFuture creates the new future.
func newFuture() Future {
	return make(chan *ResultSet, 1)
}

// setResultSet sets the result set.
func (f Future) setResultSet(rs *ResultSet) {
	f <- rs
}

// ResultSet blocks until the associated result set can be returned.
func (f Future) ResultSet() (rs *ResultSet) {
	rs = <-f
	f <- rs
	return
}
