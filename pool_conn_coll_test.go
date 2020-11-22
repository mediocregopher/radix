package radix

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPoolConnColl(t *testing.T) {
	poolConnNames := map[*poolConn]string{}
	newPoolConn := func(name string) *poolConn {
		conn := new(poolConn)
		poolConnNames[conn] = name
		return conn
	}

	getPoolConnName := func(conn *poolConn) string {
		if conn == nil {
			return "<nil>"
		}
		return poolConnNames[conn]
	}

	a, b, c := newPoolConn("a"), newPoolConn("b"), newPoolConn("c")

	assertConnEqual := func(t *testing.T, exp, got *poolConn) {
		if exp != got {
			t.Fatalf("expected conn %s, got %s", getPoolConnName(exp), getPoolConnName(got))
		}
	}

	type subTest struct {
		name    string
		op      func(*testing.T, *poolConnColl)
		expColl *poolConnColl
	}

	type test struct {
		name     string
		initColl func() *poolConnColl
		subTests []subTest
	}

	tests := []test{
		{
			name: "cap 1 empty",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{nil}, first: 0, l: 0}
			},
			subTests: []subTest{
				{
					name:    "pushFront",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.pushFront(a) },
					expColl: &poolConnColl{s: []*poolConn{a}, first: 0, l: 1},
				},
				{
					name: "popBack",
					op: func(t *testing.T, coll *poolConnColl) {
						assertConnEqual(t, nil, coll.popBack())
					},
					expColl: &poolConnColl{s: []*poolConn{nil}, first: 0, l: 0},
				},
				{
					name:    "remove dne",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil}, first: 0, l: 0},
				},
			},
		},

		{
			name: "cap 1",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{a}, first: 0, l: 1}
			},
			subTests: []subTest{
				{
					name: "popBack",
					op: func(t *testing.T, coll *poolConnColl) {
						assertConnEqual(t, a, coll.popBack())
					},
					expColl: &poolConnColl{s: []*poolConn{nil}, first: 0, l: 0},
				},
				{
					name:    "remove",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil}, first: 0, l: 0},
				},
				{
					name:    "remove dne",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(b) },
					expColl: &poolConnColl{s: []*poolConn{a}, first: 0, l: 1},
				},
			},
		},

		////////////////////////////////////////////////////////////////////////

		{
			name: "cap 2 empty A",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{nil, nil}, first: 0, l: 0}
			},
			subTests: []subTest{
				{
					name:    "pushFront",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.pushFront(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, a}, first: 1, l: 1},
				},
				{
					name: "popBack",
					op: func(t *testing.T, coll *poolConnColl) {
						assertConnEqual(t, nil, coll.popBack())
					},
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 0, l: 0},
				},
				{
					name:    "remove dne",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 0, l: 0},
				},
			},
		},
		{
			name: "cap 2 empty B",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{nil, nil}, first: 1, l: 0}
			},
			subTests: []subTest{
				{
					name:    "pushFront",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.pushFront(a) },
					expColl: &poolConnColl{s: []*poolConn{a, nil}, first: 0, l: 1},
				},
				{
					name: "popBack",
					op: func(t *testing.T, coll *poolConnColl) {
						assertConnEqual(t, nil, coll.popBack())
					},
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 1, l: 0},
				},
				{
					name:    "remove dne",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 1, l: 0},
				},
			},
		},

		////////////////////////////////////////////////////////////////////////

		{
			name: "cap 2 A",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{a, nil}, first: 0, l: 1}
			},
			subTests: []subTest{
				{
					name:    "pushFront",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.pushFront(b) },
					expColl: &poolConnColl{s: []*poolConn{a, b}, first: 1, l: 2},
				},
				{
					name: "popBack",
					op: func(t *testing.T, coll *poolConnColl) {
						assertConnEqual(t, a, coll.popBack())
					},
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 0, l: 0},
				},
				{
					name:    "remove",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 0, l: 0},
				},
				{
					name:    "remove dne",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(b) },
					expColl: &poolConnColl{s: []*poolConn{a, nil}, first: 0, l: 1},
				},
			},
		},
		{
			name: "cap 2 B",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{nil, a}, first: 1, l: 1}
			},
			subTests: []subTest{
				{
					name:    "pushFront",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.pushFront(b) },
					expColl: &poolConnColl{s: []*poolConn{b, a}, first: 0, l: 2},
				},
				{
					name: "popBack",
					op: func(t *testing.T, coll *poolConnColl) {
						assertConnEqual(t, a, coll.popBack())
					},
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 1, l: 0},
				},
				{
					name:    "remove",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 1, l: 0},
				},
				{
					name:    "remove dne",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(b) },
					expColl: &poolConnColl{s: []*poolConn{nil, a}, first: 1, l: 1},
				},
			},
		},
		{
			name: "cap 2 C",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{a, b}, first: 0, l: 2}
			},
			subTests: []subTest{
				{
					name: "popBack",
					op: func(t *testing.T, coll *poolConnColl) {
						assertConnEqual(t, b, coll.popBack())
					},
					expColl: &poolConnColl{s: []*poolConn{a, nil}, first: 0, l: 1},
				},
				{
					name:    "remove",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, b}, first: 1, l: 1},
				},
				{
					name:    "remove dne",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(c) },
					expColl: &poolConnColl{s: []*poolConn{a, b}, first: 0, l: 2},
				},
			},
		},
		{
			name: "cap 2 D",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{a, b}, first: 1, l: 2}
			},
			subTests: []subTest{
				{
					name: "popBack",
					op: func(t *testing.T, coll *poolConnColl) {
						assertConnEqual(t, a, coll.popBack())
					},
					expColl: &poolConnColl{s: []*poolConn{nil, b}, first: 1, l: 1},
				},
				{
					name:    "remove",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, b}, first: 1, l: 1},
				},
				{
					name:    "remove dne",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(c) },
					expColl: &poolConnColl{s: []*poolConn{a, b}, first: 1, l: 2},
				},
			},
		},

		////////////////////////////////////////////////////////////////////////

		{
			name: "cap 3 A",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{a, b, c}, first: 0, l: 3}
			},
			subTests: []subTest{
				{
					name:    "remove a",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, b, c}, first: 1, l: 2},
				},
				{
					name:    "remove b",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(b) },
					expColl: &poolConnColl{s: []*poolConn{nil, a, c}, first: 1, l: 2},
				},
				{
					name:    "remove c",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(c) },
					expColl: &poolConnColl{s: []*poolConn{nil, a, b}, first: 1, l: 2},
				},
			},
		},
		{
			name: "cap 3 B",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{a, b, c}, first: 1, l: 3}
			},
			subTests: []subTest{
				{
					name:    "remove a",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, b, c}, first: 1, l: 2},
				},
				{
					name:    "remove b",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(b) },
					expColl: &poolConnColl{s: []*poolConn{nil, a, c}, first: 1, l: 2},
				},
				{
					name:    "remove c",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(c) },
					expColl: &poolConnColl{s: []*poolConn{nil, a, b}, first: 1, l: 2},
				},
			},
		},
		{
			name: "cap 3 C",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{a, b, c}, first: 2, l: 3}
			},
			subTests: []subTest{
				{
					name:    "remove a",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{b, nil, c}, first: 2, l: 2},
				},
				{
					name:    "remove b",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(b) },
					expColl: &poolConnColl{s: []*poolConn{a, nil, c}, first: 2, l: 2},
				},
				{
					name:    "remove c",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(c) },
					expColl: &poolConnColl{s: []*poolConn{nil, a, b}, first: 2, l: 2},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, subTest := range test.subTests {
				t.Run(subTest.name, func(t *testing.T) {
					coll := test.initColl()
					subTest.op(t, coll)
					assert.Equal(t, subTest.expColl, coll)
				})
			}
		})
	}
}
