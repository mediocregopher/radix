package radix

import (
	"sync"
	"time"
)

// timer wraps time.Timer to make is easier to re-use
type timer struct {
	*time.Timer
}

func (t *timer) Reset(d time.Duration) {
	if t.Timer == nil {
		t.Timer = time.NewTimer(d)
		return
	}

	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Timer.Reset(d)
}

// global pool of *time.Timer's.
// TODO this probably isn't needed.
var timerPool sync.Pool

// get returns a timer that completes after the given duration.
func getTimer(d time.Duration) *time.Timer {
	t, _ := timerPool.Get().(*time.Timer)
	tt := timer{t}
	tt.Reset(d)
	return tt.Timer
}

// putTimer pools the given timer. putTimer stops the timer and handles any left over data in the channel.
func putTimer(t *time.Timer) {
	timerPool.Put(t)
}
