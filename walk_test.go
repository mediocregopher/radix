package radix

import (
	"errors"
	. "testing"

	"github.com/stretchr/testify/assert"
)

func TestWalk(t *T) {
	alen := func(l int) func(w *walker) {
		return func(w *walker) {
			n, ok := w.next()
			assert.True(t, ok)
			assert.False(t, n.vOk)
			assert.Equal(t, l, n.l)
		}
	}

	val := func(v interface{}) func(w *walker) {
		return func(w *walker) {
			n, ok := w.next()
			assert.True(t, ok)
			assert.True(t, n.vOk)
			assert.Equal(t, v, n.v)
		}
	}

	done := func(w *walker) {
		_, ok := w.next()
		assert.False(t, ok)
	}

	type wt struct {
		in   interface{}
		pipe []func(w *walker)
	}

	walkTest := func(in interface{}, pipe ...func(w *walker)) wt {
		return wt{in: in, pipe: pipe}
	}

	tests := []wt{
		// normal values
		walkTest([]byte("ohey"), val([]byte("ohey")), done),
		walkTest("ohey", val("ohey"), done),
		walkTest(true, val(true), done),
		walkTest(nil, val(nil), done),
		walkTest(textCPMarshaler("ohey"), val(textCPMarshaler("ohey")), done),
		walkTest(errors.New(":("), val(errors.New(":(")), done),

		// simple arrays
		walkTest([]string(nil), alen(0), done),
		walkTest([]string{}, alen(0), done),
		walkTest([]string{"a", "b"}, alen(2), val("a"), val("b"), done),

		// complex arrays
		walkTest([]interface{}(nil), alen(0), done),
		walkTest([]interface{}{}, alen(0), done),
		walkTest([]interface{}{"a", 1}, alen(2), val("a"), val(1), done),

		// embedded arrays
		walkTest([]interface{}{[]string{}}, alen(1), alen(0), done),
		walkTest([]interface{}{[]string{}, []string{"a"}},
			alen(2), alen(0), alen(1), val("a"), done),
		walkTest([]interface{}{[]string{"a"}, []string{}},
			alen(2), alen(1), val("a"), alen(0), done),
		walkTest([]interface{}{[]string{"a"}, []string{"b"}},
			alen(2), alen(1), val("a"), alen(1), val("b"), done),

		// maps
		walkTest(map[string]int(nil), alen(0), done),
		walkTest(map[string]int{}, alen(0), done),
		walkTest(map[string]int{"one": 1}, alen(2), val("one"), val(1), done),
		walkTest(map[string][]int{"one": []int(nil)},
			alen(2), val("one"), alen(0), done),
		walkTest(map[string][]int{"one": []int{}},
			alen(2), val("one"), alen(0), done),
		walkTest(map[string][]int{"one": []int{1}},
			alen(2), val("one"), alen(1), val(1), done),
		walkTest(map[[2]int][]int{}, alen(0), done),
		walkTest(map[[2]int][]int{[2]int{1, 2}: []int{3}},
			alen(2), alen(2), val(1), val(2), alen(1), val(3), done),
	}

	for _, test := range tests {
		w := newWalker(test.in)
		for i := range test.pipe {
			test.pipe[i](w)
		}
	}
}
