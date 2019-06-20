package bytesutil

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"io"
	. "testing"

	"github.com/mediocregopher/mediocre-go-lib/mrand"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadNAppend(t *T) {
	buf := []byte("hello")
	buf, err := ReadNAppend(bytes.NewReader([]byte(" world!")), buf, len(" world"))
	require.Nil(t, err)
	assert.Len(t, buf, len("hello world"))
	assert.Equal(t, buf, []byte("hello world"))
}

type discarder struct {
	didDiscard bool
	*bufio.Reader
}

func (d *discarder) Discard(n int) (int, error) {
	d.didDiscard = true
	return d.Reader.Discard(n)
}

func TestReadNDiscard(t *T) {
	type testT struct {
		n         int
		discarder bool
	}

	assert := func(test testT) {
		buf := bytes.NewBuffer(make([]byte, 0, test.n))
		if _, err := io.CopyN(buf, rand.Reader, int64(test.n)); err != nil {
			t.Fatal(err)
		}

		var r io.Reader = buf
		var d *discarder
		if test.discarder {
			d = &discarder{Reader: bufio.NewReader(r)}
			r = d
		}

		if err := ReadNDiscard(r, test.n); err != nil {
			t.Fatalf("error calling readNDiscard: %s (%#v)", err, test)

		} else if test.discarder && test.n > 0 && !d.didDiscard {
			t.Fatalf("Discard not called on discarder (%#v)", test)

		} else if test.discarder && test.n == 0 && d.didDiscard {
			t.Fatalf("Unnecessary Discard call (%#v)", test)

		} else if buf.Len() > 0 {
			t.Fatalf("%d bytes not discarded (%#v)", buf.Len(), test)
		}
	}

	// randomly generate test cases
	for i := 0; i < 1000; i++ {
		test := testT{
			n:         mrand.Intn(16384),
			discarder: mrand.Intn(2) == 0,
		}
		assert(test)
	}

	// edge cases
	assert(testT{n: 0, discarder: true})
}
