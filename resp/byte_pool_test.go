package resp

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

func BenchmarkBytePool(b *testing.B) {
	do := func(b *testing.B, o *Opts) {
		b.Helper()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := o.GetBytes()
			o.PutBytes(buf)
		}
	}

	b.Run("sync.Pool", func(b *testing.B) {
		syncPool := sync.Pool{
			New: func() interface{} {
				b := make([]byte, 0, 32)
				return &b
			},
		}
		opts := NewOpts()
		opts.GetBytes = func() *[]byte {
			return syncPool.Get().(*[]byte)
		}
		opts.PutBytes = func(b *[]byte) {
			*b = (*b)[:0]
			syncPool.Put(b)
		}
		do(b, opts)
	})

	for i := 1; i < 15; i++ {
		threshold := int(math.Pow10(i))
		b.Run(fmt.Sprintf("bytePool%d", threshold), func(b *testing.B) {
			bp := newBytePool(threshold)
			opts := NewOpts()
			opts.GetBytes = bp.get
			opts.PutBytes = bp.put
			do(b, opts)
		})
	}
}
