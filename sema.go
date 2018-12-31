package radix

type semaphore struct {
	ch chan struct{}
}

func newSemaphore(size int) semaphore {
	sema := semaphore{ch: make(chan struct{}, size)}
	for i := 0; i < cap(sema.ch); i++ {
		sema.ch <- struct{}{}
	}
	return sema
}

func (s semaphore) acquire() {
	<-s.ch
}

func (s semaphore) acquireAll() {
	for i := 0; i < cap(s.ch); i++ {
		<-s.ch
	}
}

func (s semaphore) release() {
	select {
	case s.ch <- struct{}{}:
	default:
		panic("release called on full semaphore")
	}
}
