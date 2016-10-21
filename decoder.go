package radix

import (
	"bufio"
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
)

// A special limited reader which will read an extra two bytes after the limit
// has been reached

type limitedReaderPlus struct {
	eof     bool
	lr      *io.LimitedReader
	discard io.Writer
}

func newLimitedReaderPlus(r io.Reader, limit int64, discard io.Writer) *limitedReaderPlus {
	return &limitedReaderPlus{
		discard: discard,
		lr: &io.LimitedReader{
			R: r,
			N: limit,
		},
	}
}

func (lrp *limitedReaderPlus) Read(b []byte) (int, error) {
	if lrp.eof {
		return 0, io.EOF
	}

	i, err := lrp.lr.Read(b)
	if err == io.EOF {
		lrp.eof = true
		_, err = io.CopyN(lrp.discard, lrp.lr.R, 2)
		return i, err
	}
	return i, err
}

// Decoder wraps an io.Reader and decodes Resp data off of it.
type Decoder struct {

	// The size of the internal read buffer used for miscellaneous things. If
	// the buffer grows larger it will be re-allocated at this size.
	//
	// This should rarely if ever need to be touched. If you're not sure if it
	// needs to be changed assume it doesn't.
	//
	// Defaults to 1024
	BufSize int

	r       *bufio.Reader
	scratch []byte
	discard *bufio.Writer // used to just outright discard data
}

// NewDecoder initialized a Decoder instance which will read from the given
// io.Reader. The io.Reader should not be used after this
func NewDecoder(r io.Reader) *Decoder {
	bufSize := 1024
	return &Decoder{
		r:       bufio.NewReader(r),
		scratch: make([]byte, bufSize),
		// wrap in bufio so we get ReaderFrom, eliminates an allocation later
		discard: bufio.NewWriter(ioutil.Discard),
		BufSize: bufSize,
	}
}

// Decode reads a single message off of the underlying io.Reader and unmarshals
// it into the given receiver, which should be a pointer or reference type.
func (d *Decoder) Decode(v interface{}) error {
	// the slice returned here should not be stored or modified, it's internal
	// to the bufio.Reader
	b, err := d.r.ReadSlice(delimEnd)
	if err != nil {
		return err
	}
	body := b[1 : len(b)-delimLen]

	var size int64
	switch b[0] {
	case simpleStrPrefix[0], intPrefix[0]:
		return d.scanInto(v, bytes.NewReader(body))

	case bulkStrPrefix[0]:
		if size, err = strconv.ParseInt(string(body), 10, 64); err != nil {
			return err
		}
		return d.scanInto(v, newLimitedReaderPlus(d.r, size, d.discard))

	case errPrefix[0]:
		return AppErr{errors.New(string(body))}

	case arrayPrefix[0]:
		panic("TODO")

	default:
		return fmt.Errorf("Unknown type prefix %q", string(b))
	}
}

func (d *Decoder) scanInto(dst interface{}, r io.Reader) error {
	var (
		err error
		i   int64
		ui  uint64
	)

	switch dstt := dst.(type) {
	case nil:
		// discard everything
	case io.Writer:
		_, err = io.Copy(dstt, r)
	case *string:
		d.scratch, err = readAllAppend(r, d.scratch[:0])
		*dstt = string(d.scratch)
	case *[]byte:
		*dstt, err = readAllAppend(r, (*dstt)[:0])
	case *bool:
		if ui, err = d.readUint(r); err != nil {
			err = fmt.Errorf("could not parse as bool: %s", err)
			break
		}
		if ui == 1 {
			*dstt = true
		} else if ui == 0 {
			*dstt = false
		} else {
			err = fmt.Errorf("invalid bool value: %d", ui)
		}

	case *int:
		i, err = d.readInt(r)
		*dstt = int(i)
	case *int8:
		i, err = d.readInt(r)
		*dstt = int8(i)
	case *int16:
		i, err = d.readInt(r)
		*dstt = int16(i)
	case *int32:
		i, err = d.readInt(r)
		*dstt = int32(i)
	case *int64:
		*dstt, err = d.readInt(r)
	case *uint:
		ui, err = d.readUint(r)
		*dstt = uint(ui)
	case *uint8:
		ui, err = d.readUint(r)
		*dstt = uint8(ui)
	case *uint16:
		ui, err = d.readUint(r)
		*dstt = uint16(ui)
	case *uint32:
		ui, err = d.readUint(r)
		*dstt = uint32(ui)
	case *uint64:
		*dstt, err = d.readUint(r)
	case *float32:
		var f float64
		f, err = d.readFloat(r, 32)
		*dstt = float32(f)
	case *float64:
		*dstt, err = d.readFloat(r, 64)
	case encoding.TextUnmarshaler:
		if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
			break
		}
		err = dstt.UnmarshalText(d.scratch)
	case encoding.BinaryUnmarshaler:
		if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
			break
		}
		err = dstt.UnmarshalBinary(d.scratch)
	default:
		err = fmt.Errorf("cannot decode into type %T", dstt)
	}

	// no matter what we *must* finish reading the io.Reader. The io.Reader must
	// allow for Read to be called on it after an io.EOF is hit.
	io.Copy(d.discard, r)

	if cap(d.scratch) > d.BufSize {
		d.scratch = make([]byte, d.BufSize)
	}

	return err
}

func (d *Decoder) readInt(r io.Reader) (int64, error) {
	var err error
	if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(d.scratch), 10, 64)
}

func (d *Decoder) readUint(r io.Reader) (uint64, error) {
	var err error
	if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(d.scratch), 10, 64)
}

func (d *Decoder) readFloat(r io.Reader, precision int) (float64, error) {
	var err error
	if d.scratch, err = readAllAppend(r, d.scratch[:0]); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(d.scratch), precision)
}

func readAllAppend(r io.Reader, b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	// TODO a side effect of this is that the given b will be re-allocated if
	// it's less than bytes.MinRead. Since this b could be all the way from the
	// user we can't guarantee it within the library. Would b enice to not have
	// that weird edge-case
	_, err := buf.ReadFrom(r)
	return buf.Bytes(), err
}
