package radix

import (
	"bytes"
	. "testing"
)

type benchTest struct {
	i interface{} // go representation
	s string      // resp representation
}

func loopBenchTests() chan benchTest {
	ch := make(chan benchTest)
	go func() {
		for {
			for _, dt := range decodeTests {
				ch <- benchTest{
					i: dt.out,
					s: dt.in,
				}
			}
			for _, et := range encodeTests {
				ch <- benchTest{
					i: et.in,
					s: et.out,
				}
			}
		}
	}()
	return ch
}

func BenchmarkRead(b *B) {
	ch := loopBenchTests()
	buf := new(bytes.Buffer)
	r := NewDecoder(buf)

	var into interface{}
	for i := 0; i < b.N; i++ {
		buf.WriteString((<-ch).s)
		if err := r.Decode(&into); err != nil {
			panic(err)
		}
		into = nil
	}
}

func BenchmarkWrite(b *B) {
	ch := loopBenchTests()
	buf := new(bytes.Buffer)
	w := NewEncoder(buf)

	for i := 0; i < b.N; i++ {
		if err := w.Encode((<-ch).i); err != nil {
			panic(err)
		}
		buf.Reset()
	}
}
