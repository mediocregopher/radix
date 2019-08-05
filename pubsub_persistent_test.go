package radix

import (
	. "testing"
	"time"

	"github.com/stretchr/testify/assert"
	errors "golang.org/x/xerrors"
)

func TestPersistentPubSub(t *T) {
	closeCh := make(chan chan bool)
	p, err := PersistentPubSub("", "",
		PersistentPubSubConnFunc(func(_, _ string) (Conn, error) {
			c := dial()
			go func() {
				closeRetCh := <-closeCh
				c.Close()
				closeRetCh <- true
			}()
			return c, nil
		}))
	assert.NoError(t, err)

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

func TestPersistenPubSubAbortAfter(t *T) {
	var (
		attempts         int
		expectedAttempts = 5
	)

	p, err := PersistentPubSub("", "",
		PersistentPubSubAbortAfter(expectedAttempts),
		PersistentPubSubConnFunc(func(_, _ string) (Conn, error) {
			attempts++
			return nil, errors.New("bad connection")
		}))

	assert.Error(t, err)
	assert.Nil(t, p)
	assert.Equal(t, expectedAttempts, attempts)
}
