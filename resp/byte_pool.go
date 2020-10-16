package resp

// bytePool is a non-thread-safe pool of []byte instances which can help absorb
// allocations.
//
// bytePool uses an internal threshold and a counter to free []bytes on a
// regular basis. Everytime a []byte is put back in the pool the counter is
// incremented by len([]byte). If the counter then exceeds the threshold that
// []byte is freed rather than being put back.
type bytePool struct {
	used, threshold int
	pool            []*[]byte
}

func newBytePool(threshold int) *bytePool {
	return &bytePool{threshold: threshold}
}

func (bp *bytePool) get() *[]byte {
	if len(bp.pool) == 0 {
		b := make([]byte, 0, 32)
		return &b
	}
	b := bp.pool[len(bp.pool)-1]
	bp.pool = bp.pool[:len(bp.pool)-1]
	return b
}

func (bp *bytePool) put(b *[]byte) {
	if bp.used += cap(*b); bp.used > bp.threshold {
		bp.used = 0
		return
	}

	*b = (*b)[:0]
	bp.pool = append(bp.pool, b)
}
