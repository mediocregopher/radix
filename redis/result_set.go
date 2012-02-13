package redis

//* Reply

type ReplyType uint

const (
	ReplyStatus ReplyType = iota
	ReplyError
	ReplyInteger
	ReplyNil
	ReplyString
	ReplyMulti
)

// Reply is the returned struct of commands.
type Reply struct {
	t ReplyType
	val Value
	elems []Reply
	err error
}

// Type returns the type of the reply.
func (r *Reply) Type() ReplyType {
	return r.t
}

// OK returns true if the reply had no error, otherwise false.
func (r *Reply) OK() bool {
	if r.err != nil {
		return false
	}

	return true
}

// Multi returns true if the result set contains
// multiple result sets.
func (r *Reply) Multi() bool {
	return r.resultSets != nil
}

// Len returns the number of returned values.
func (r *Reply) Len() int {
	return len(r.values)
}

// At returns a wanted value by its index.
func (r *Reply) At(i int) Value {
	if i < 0 || i >= len(r.values) {
		panic("redis: value index out of range")
	}

	return r.values[i]
}

// Value returns the first value of the reply.
// It panics, if there are no values in the reply.
func (r *Reply) Value() Value {
	if len(r.values) == 0 {
		panic("redis: no values in the reply")
	}

	return r.values[0]
}

//UnpackedValue returns the first value unpacked.
func (r *Reply) UnpackedValue() Value {
	return r.Value().Unpack()
}

// Values returns all values as slice.
func (r *Reply) Values() []Value {
	if r.values == nil {
		return nil
	}

	vs := make([]Value, len(r.values))
	copy(vs, r.values)
	return vs
}

// Ints returns all values as a slice of integer.
func (r *Reply) Ints() []int {
	if r.values == nil {
		return nil
	}

	ints := make([]int, len(r.values))
	for i, v := range r.values {
		ints[i] = v.Int()
	}

	return ints
}

// Strings returns all values as a slice of strings.
func (r *Reply) Strings() []string {
	if r.values == nil {
		return nil
	}

	strings := make([]string, len(r.values))
	for i, v := range r.values {
		strings[i] = *v.string
	}

	return strings
}

// UnpackedValues returns all values unpacked as slice.
func (r *Reply) UnpackedValues() []Value {
	vs := r.Values()

	for i, v := range vs {
		vs[i] = v.Unpack()
	}

	return vs
}

// Bool returns the first value as bool.
func (r *Reply) Bool() bool {
	return r.Value().Bool()
}

// Int returns the first value as int.
func (r *Reply) Int() int {
	return r.Value().Int()
}

// String returns the first value as string.
func (r *Reply) String() string {
	return r.Value().String()
}

// Bytes returns the first value as byte slice.
func (r *Reply) Bytes() []byte {
	return r.Value().Bytes()
}

// KeyValue return the first value as key and the second as value.
// Will panic if there are less that two values in the result set.
func (r *Reply) KeyValue() *KeyValue {
	if r.Len() < 2 {
		panic("redis: Reply does not have enough values for KeyValue")
	}

	return &KeyValue{
		Key:   r.At(0).String(),
		Value: r.At(1),
	}
}

// Apply iterates over the result values and
// applies the passed function for each one.
func (r *Reply) Apply(f func(int, Value)) {
	for idx, v := range r.values {
		f(idx, v)
	}
}

// Values iterates over the result values and
// applies the passed function for each one. The result
// is a slice of values returned by the functions.
func (r *Reply) ApplySlice(f func(Value) interface{}) []interface{} {
	result := make([]interface{}, len(r.values))

	for i, v := range r.values {
		result[i] = f(v)
	}

	return result
}

// Hash returns the values of the result set as hash.
func (r *Reply) Hash() Hash {
	var key string

	result := make(Hash)
	isVal := false

	for _, v := range r.values {
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

// ReplyLen returns the number of result sets
// inside the result set.
func (r *Reply) ReplyLen() int {
	if r.resultSets == nil {
		return 0
	}

	return len(r.resultSets)
}

// ReplyAt returns a result set by its index.
func (r *Reply) ReplyAt(i int) *Reply {
	if i < 0 || i >= len(r.resultSets) {
		panic("redis: result set index out of range")
	}

	return r.resultSets[i]
}

// MultiApply iterates over the result sets and
// applies the passed function for each one.
func (r *Reply) MultiApply(f func(*Reply)) {
	for _, rs := range r.resultSets {
		f(rs)
	}
}

// MultiApplySlice iterates over the result sets and
// performs the passed function for each one. The result
// is a slice of values returned by the functions.
func (r *Reply) MultiApplySlice(f func(*Reply) interface{}) []interface{} {
	result := make([]interface{}, len(r.resultSets))

	for idx, rs := range r.resultSets {
		result[idx] = f(rs)
	}

	return result
}

// Error returns the error if the operation creating
// the result set failed or nil.
func (r *Reply) Error() error {
	return r.err
}

//* Future

// Future just waits for a result set
// returned somewhere in the future.
type Future chan *Reply

// newFuture creates the new future.
func newFuture() Future {
	return make(chan *Reply, 1)
}

// setReply sets the result set.
func (f Future) setReply(r *Reply) {
	f <- rs
}

// Reply blocks until the associated result set can be returned.
func (f Future) Reply() (r *Reply) {
	rs = <-f
	f <- rs
	return
}
