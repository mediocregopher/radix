package radix

import (
	"encoding"
	"reflect"
)

type walkNode struct {
	v interface{}
	// because sometimes v is actually nil, and sometimes l is actually 0
	vOk bool
	l   int
}

type walker struct {
	fn walkFn
}

func newWalker(v interface{}) *walker {
	return &walker{fn: newWalkFn(v)}
}

func (w *walker) next() (walkNode, bool) {
	var n walkNode
	if w.fn == nil {
		return n, false
	}
	n, w.fn = w.fn()
	return n, true
}

// walkForeach will only return an error if the callback, the process of walking
// itself does not produce errors
func walkForeach(v interface{}, do func(n walkNode) error) error {
	fn := newWalkFn(v)
	var n walkNode
	for {
		n, fn = fn()
		if err := do(n); err != nil {
			return err
		} else if fn == nil {
			return nil
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// walker and walkForeach are really just a convenience wrappers for walkFn,
// which is a FSM a la Rob Pike's lexer talk

type walkFn func() (walkNode, walkFn)

func newWalkFn(v interface{}) walkFn {
	return func() (walkNode, walkFn) {

		// Because []byte is a slice, and any of the other interfaces might be
		// implemented by a slice, we want to check them explicitly first, so
		// they don't get caught in the reflect.Slice check below
		switch v.(type) {
		case []byte, LenReader, Marshaler,
			encoding.TextMarshaler, encoding.BinaryMarshaler:
			return walkNode{v: v, vOk: true}, nil
		}

		vv := reflect.ValueOf(v)
		switch vv.Kind() {
		case reflect.Slice, reflect.Array:
			return walkNode{l: vv.Len()}, sliceWalkFn(vv, 0)
		case reflect.Map:
			return walkNode{l: vv.Len() * 2}, mapWalkFn(vv, vv.MapKeys())
		default:
			// for all else just assume the element is a single element
			return walkNode{v: v, vOk: true}, nil
		}
	}
}

func concatWalkFns(a, b walkFn) walkFn {
	if a == nil {
		return b
	}
	return func() (walkNode, walkFn) {
		n, next := a()
		return n, concatWalkFns(next, b)
	}
}

func sliceWalkFn(vv reflect.Value, i int) walkFn {
	if i == vv.Len() {
		return nil
	}
	return func() (walkNode, walkFn) {
		v := vv.Index(i).Interface()
		n, next := newWalkFn(v)()
		return n, concatWalkFns(next, sliceWalkFn(vv, i+1))
	}
}

func mapWalkFn(vv reflect.Value, keys []reflect.Value) walkFn {
	if len(keys) == 0 {
		return nil
	}
	return func() (walkNode, walkFn) {
		k := keys[0].Interface()
		n, next := newWalkFn(k)()

		mapValWalkFn := func() (walkNode, walkFn) {
			v := vv.MapIndex(keys[0]).Interface()
			n, next := newWalkFn(v)()
			return n, concatWalkFns(next, mapWalkFn(vv, keys[1:]))
		}

		return n, concatWalkFns(next, mapValWalkFn)
	}
}
