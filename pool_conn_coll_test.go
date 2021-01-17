package radix

import (
	"container/list"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPoolConnCollChaotic(t *testing.T) {
	t.Skip("useful for finding bugs, but not as part of the normal set of tests")

	const capacity = 4
	pcc := newPoolConnColl(capacity)
	l := list.New()

	assertState := func() bool {
		exp := make([]*poolConn, 0, l.Len())
		for el := l.Front(); el != nil; el = el.Next() {
			exp = append(exp, el.Value.(*poolConn))
		}

		got := make([]*poolConn, pcc.len())
		for i := range got {
			got[i] = pcc.get(i)
		}

		return assert.Equal(t, exp, got)
	}

	assertState() // sanity check

	seed := time.Now().UnixNano()
	rand := rand.New(rand.NewSource(seed))
	t.Logf("seed:%d", seed)

	for i := 0; i < 10000; i++ {
		switch rand.Intn(3) {
		case 0:
			if l.Len() == capacity {
				i--
				continue
			}
			pc := new(poolConn)
			t.Logf("pushing %p", pc)
			l.PushFront(pc)
			pcc.pushFront(pc)

		case 1:
			if l.Len() == 0 {
				i--
				continue
			}
			expPC := l.Remove(l.Back())
			t.Logf("popping %p from back", expPC)
			gotPC := pcc.popBack()
			assert.Equal(t, expPC, gotPC)

		case 2:
			if l.Len() == 0 {
				i--
				continue
			}
			idx := rand.Intn(pcc.len())
			pc := pcc.get(idx)
			t.Logf("removing %p (index:%d)", pc, idx)

			var found bool
			for el := l.Front(); el != nil; el = el.Next() {
				if el.Value.(*poolConn) == pc {
					found = true
					l.Remove(el)
					break
				}
			}
			if !found {
				t.Fatalf("could not find poolConn %p in list", pc)
			}

			pcc.remove(pc)
		}

		if !assertState() {
			return
		}
	}
}

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
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 1, l: 0},
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
					expColl: &poolConnColl{s: []*poolConn{nil, nil}, first: 0, l: 0},
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
					name:    "remove a",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, b}, first: 1, l: 1},
				},
				{
					name:    "remove b",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(b) },
					expColl: &poolConnColl{s: []*poolConn{nil, a}, first: 1, l: 1},
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
					expColl: &poolConnColl{s: []*poolConn{a, nil, c}, first: 2, l: 2},
				},
				{
					name:    "remove c",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(c) },
					expColl: &poolConnColl{s: []*poolConn{a, nil, b}, first: 2, l: 2},
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
					expColl: &poolConnColl{s: []*poolConn{a, b, nil}, first: 0, l: 2},
				},
			},
		},

		{
			name: "issue 254",
			initColl: func() *poolConnColl {
				return &poolConnColl{s: []*poolConn{nil, b, a}, first: 1, l: 2}
			},
			subTests: []subTest{
				{
					name:    "remove a",
					op:      func(_ *testing.T, coll *poolConnColl) { coll.remove(a) },
					expColl: &poolConnColl{s: []*poolConn{nil, nil, b}, first: 2, l: 1},
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
