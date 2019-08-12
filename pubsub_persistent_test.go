package radix

import (
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	errors "golang.org/x/xerrors"
)

func TestPersistentPubSub(t *T) {
	closeCh := make(chan chan bool)
	p := PersistentPubSub("", "", func(_, _ string) (Conn, error) {
		c := dial()
		go func() {
			closeRetCh := <-closeCh
			c.Close()
			closeRetCh <- true
		}()
		return c, nil
	})

	pubCh := make(chan int)
	go func() {
		for i := 0; i < 1000; i++ {
			pubCh <- i
			if i%100 == 0 {
				time.Sleep(100 * time.Millisecond)
				closeRetCh := make(chan bool)
				closeCh <- closeRetCh
				<-closeRetCh
				assert.Nil(t, p.Ping())
			}
		}
		close(pubCh)
	}()

	testSubscribe(t, p, pubCh)
}

func TestPersistentPubSubAbortAfter(t *T) {
	var errNope = errors.New("nope")
	var attempts int
	connFn := func(_, _ string) (Conn, error) {
		attempts++
		if attempts%3 != 0 {
			return nil, errNope
		}
		return dial(), nil
	}

	_, err := PersistentPubSubWithOpts("", "",
		PersistentPubSubConnFunc(connFn),
		PersistentPubSubAbortAfter(2))
	assert.Equal(t, errNope, err)

	attempts = 0
	p, err := PersistentPubSubWithOpts("", "",
		PersistentPubSubConnFunc(connFn),
		PersistentPubSubAbortAfter(3))
	assert.NoError(t, err)
	assert.NoError(t, p.Ping())
	p.Close()
}
