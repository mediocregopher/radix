package radix

import (
	"net"
	"sync"
	"time"

	"github.com/mediocregopher/radix/v3/resp"
)

type pipeliningConnOpts struct {
	batchSize int
	batchDur  time.Duration
}

// PipeliningConnOpt is an option which can be given to NewPipeliningConn to
// effect how the resulting Conn functions.
type PipeliningConnOpt func(*pipeliningConnOpts)

// PipeliningConnBatchSize describes the max number of EncodeDecode calls which
// will be buffered in the PipeliningConn before it executes all at once. A
// larger batch size will result in fewer round-trips with the server, but more
// of a delay between the round-trips.
//
// A value of 0 indicates that batch size should not be considered when
// determining when to execute the batch.
//
// Defaults to 10.
func PipeliningConnBatchSize(size int) PipeliningConnOpt {
	return func(opts *pipeliningConnOpts) {
		opts.batchSize = size
	}
}

// PipeliningConnBatchDuration describes the max amount of time EncodeDecode
// calls will block before the batch they are a part of is executed. A longer
// batch duration will result in fewer round-trips with the server, but more of
// a delay in EncodeDecode calls.
//
// A value of 0 indicates that the batch duration should not be considered when
// determining when to execute the batch.
//
// Defaults to 150 microseconds.
func PipeliningConnBatchDuration(d time.Duration) PipeliningConnOpt {
	return func(opts *pipeliningConnOpts) {
		opts.batchDur = d
	}
}

type pipeliningConnEncDec struct {
	pipelineMarshalerUnmarshaler
	resCh chan error
}

// pipeliningConn wraps a Conn such that it will buffer concurrent EncodeDecode
// calls until certain thresholds are met (either time or buffer size). At that
// point it will perform all encodes, in order, in a single write, then perform
// all decodes, in order, in a single read.
//
// pipeliningConn's methods are thread-safe.
type pipeliningConn struct {
	conn     Conn
	opts     pipeliningConnOpts
	encDecCh chan pipeliningConnEncDec

	// encDecs corresponds 1:1 with the mm slice in the pipeline
	encDecs  []pipeliningConnEncDec
	pipeline *pipeline

	batchTimer *timer

	wg        sync.WaitGroup
	closeOnce sync.Once

	// this is only used in tests. If it's set it will be used for timerChs
	// instead of an actual timer
	testTimerCh chan time.Time
}

var _ Client = new(pipeliningConn)

// NewPipeliningConn wraps the given Conn such that it will batch concurrent
// EncodeDecode calls together. Once certain thresholds are met (such as time or
// buffer size, see PipeliningConnOpt) all Encodes will be performed in a single
// write, then all Decodes will be performed in a single read.
//
// The Do and EncodeDecode methods of the returned Conn are thread-safe. The
// Conn given here should not be used after this is called.
func NewPipeliningConn(conn Conn, opts ...PipeliningConnOpt) Conn {
	pco := pipeliningConnOpts{
		batchSize: 10,
		batchDur:  150 * time.Microsecond,
	}
	for _, opt := range opts {
		opt(&pco)
	}

	pc := &pipeliningConn{
		conn:       conn,
		opts:       pco,
		encDecCh:   make(chan pipeliningConnEncDec, 16),
		pipeline:   newPipeline(),
		batchTimer: new(timer),
	}

	pc.wg.Add(1)
	go func() {
		defer pc.wg.Done()
		pc.spin()
	}()

	return pc
}

func (pc *pipeliningConn) Close() error {
	pc.closeOnce.Do(func() {
		close(pc.encDecCh)
		pc.wg.Wait()
	})
	return pc.conn.Close()
}

func (pc *pipeliningConn) canFlush(timerCh <-chan time.Time) bool {
	if pc.opts.batchSize > 0 && len(pc.encDecs) >= pc.opts.batchSize {
		return true
	} else if timerCh != nil {
		select {
		case <-timerCh:
			return true
		default:
		}
	}
	return false
}

func (pc *pipeliningConn) flush() {
	if len(pc.encDecs) == 0 {
		return
	}

	// pipeline's Marshal/UnmarshalRESP methods don't return an error, but
	// instead swallow any errors they come across. If EncodeDecode returns an
	// error it means something else the Conn was doing errored (like flushing
	// its write buffer). There's not much to be done except return that error
	// for all pipelineMarshalerUnmarshalers.
	if err := pc.conn.EncodeDecode(pc.pipeline, pc.pipeline); err != nil {
		pc.pipeline.setErr(0, err)
	}

	for i, m := range pc.pipeline.mm {
		pc.encDecs[i].resCh <- m.err
	}

	pc.encDecs = pc.encDecs[:0]
	pc.pipeline.reset()
}

func (pc *pipeliningConn) resetTimer() <-chan time.Time {
	if pc.testTimerCh != nil {
		return pc.testTimerCh
	} else if pc.opts.batchDur > 0 {
		pc.batchTimer.Reset(pc.opts.batchDur)
		return pc.batchTimer.Timer.C
	}
	return nil
}

func (pc *pipeliningConn) spin() {
	timerCh := pc.resetTimer()
	for {
		select {
		case encDec, ok := <-pc.encDecCh:
			if !ok {
				pc.flush()
				return
			}

			pc.encDecs = append(pc.encDecs, encDec)
			pc.pipeline.mm = append(pc.pipeline.mm, encDec.pipelineMarshalerUnmarshaler)

			if pc.canFlush(timerCh) {
				pc.flush()
				timerCh = pc.resetTimer()
			} else if timerCh == nil {
				timerCh = pc.resetTimer()
			}

		case <-timerCh:
			pc.flush()
			// don't start a new timer here, only do that the first time a new
			// encDec comes in, otherwise for really small batchDurs and low
			// activity this will end up in a tight-ish loop.
			timerCh = nil
		}
	}
}

func (pc *pipeliningConn) Do(action Action) error {
	return action.Perform(pc)
}

func (pc *pipeliningConn) EncodeDecode(m resp.Marshaler, u resp.Unmarshaler) error {
	resCh := make(chan error, 1)
	pc.encDecCh <- pipeliningConnEncDec{
		pipelineMarshalerUnmarshaler{Marshaler: m, Unmarshaler: u},
		resCh,
	}
	return <-resCh
}

func (pc *pipeliningConn) NetConn() net.Conn {
	return pc.conn.NetConn()
}
