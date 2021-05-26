package radix

import (
	"context"
	"fmt"
	"time"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/mediocregopher/radix/v4/resp/resp3"
)

type connWriter struct {
	wCh    <-chan connMarshalerUnmarshaler
	rCh    chan<- connMarshalerUnmarshaler
	doneCh <-chan struct{}

	bw   resp.BufferedWriter
	conn interface{ SetWriteDeadline(time.Time) error }
	opts *resp.Opts

	flushInterval time.Duration
	flushTicker   interface {
		Stop()
		Reset(time.Duration)
	}
	flushTickerCh <-chan time.Time

	// populated during run()
	connErr           error
	flushBuf          []connMarshalerUnmarshaler
	flushTickerPaused bool

	// only used for tests, will be written to in the event loop
	eventLoopCh chan struct{}
}

func (c *conn) writer(ctx context.Context, flushInterval time.Duration) {
	cw := &connWriter{
		wCh:           c.wCh,
		rCh:           c.rCh,
		doneCh:        ctx.Done(),
		bw:            c.bw,
		conn:          c.conn,
		opts:          c.wOpts,
		flushInterval: flushInterval,
	}

	if cw.flushInterval > 0 {
		ticker := time.NewTicker(cw.flushInterval)
		cw.flushTicker = ticker
		cw.flushTickerCh = ticker.C
	}

	cw.run()
}

func (cw *connWriter) pauseTicker() {
	if cw.flushTicker == nil || cw.flushTickerPaused {
		return
	}
	cw.flushTicker.Stop()
	cw.flushTickerPaused = true
}

func (cw *connWriter) resumeTicker() {
	if cw.flushTicker == nil || !cw.flushTickerPaused {
		return
	}
	cw.flushTicker.Reset(cw.flushInterval)
	cw.flushTickerPaused = false
}

// forwardToReader returns false if doneCh is closed
func (cw *connWriter) forwardToReader(mu connMarshalerUnmarshaler) bool {
	select {
	case <-cw.doneCh:
		return false
	case cw.rCh <- mu:
		return true
	}
}

// write returns true if the write was successful
func (cw *connWriter) write(mu connMarshalerUnmarshaler) error {
	if err := mu.ctx.Err(); err != nil {
		return err
	} else if deadline, ok := mu.ctx.Deadline(); ok {
		if err := cw.conn.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("setting write deadline to %v: %w", deadline, err)
		}
	} else if err := cw.conn.SetWriteDeadline(time.Time{}); err != nil {
		return fmt.Errorf("unsetting write deadline: %w", err)
	}

	return resp3.Marshal(cw.bw, mu.marshal, cw.opts)
}

// setConnErr marks the connWriter as failed and forwards the error to all
// actions in flushBuf.
func (cw *connWriter) setConnErr(err error, flushBuf []connMarshalerUnmarshaler) {
	for _, mu := range flushBuf {
		mu.errCh <- err
	}
	cw.connErr = err
}

// flush returns false if doneCh is closed
func (cw *connWriter) flush() bool {
	if len(cw.flushBuf) == 0 {
		cw.pauseTicker()
		return true
	}

	flushBuf := cw.flushBuf[:0]
	for i, mu := range cw.flushBuf {
		if err := cw.write(mu); err != nil {
			// Connection can now be in an inconsistent state; we
			// don't know how much was written to the buffer.
			cw.setConnErr(err, cw.flushBuf[i:])
			break
		} else {
			flushBuf = append(flushBuf, mu)
		}
	}
	cw.flushBuf = cw.flushBuf[:0]

	if err := cw.bw.Flush(); err != nil {
		cw.setConnErr(err, flushBuf)
	} else {
		for _, mu := range flushBuf {
			// if there's no unmarshaler then don't forward to the reader
			if mu.unmarshalInto == nil {
				mu.errCh <- nil
			} else if !cw.forwardToReader(mu) {
				return false
			}
		}
	}

	cw.resumeTicker()
	return true
}

func (cw *connWriter) run() {
	cw.pauseTicker()
	for {
		select {
		case <-cw.doneCh:
			return
		case <-cw.flushTickerCh:
			if !cw.flush() {
				return
			}
		case <-cw.eventLoopCh:
			// do nothing, only used for tests
		case mu := <-cw.wCh:
			// Permanent connection error; no action can be taken.
			if cw.connErr != nil {
				mu.errCh <- cw.connErr
				return
			}
			if mu.marshal != nil {
				cw.flushBuf = append(cw.flushBuf, mu)
				if (cw.flushInterval == 0 || cw.flushTickerPaused) && !cw.flush() {
					return
				}

				// if there's no marshaler then flush what's there so far before
				// forwarding, so that order can be preserved
			} else if !cw.flush() || !cw.forwardToReader(mu) {
				return
			}
		}
	}
}
