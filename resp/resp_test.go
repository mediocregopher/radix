package resp

import (
	. "testing"

	"errors"

	"github.com/stretchr/testify/assert"
)

func TestErrDiscarded(t *T) {
	err := errors.New("foo")
	assert.False(t, errors.As(err, new(ErrDiscarded)))
	assert.True(t, errors.As(ErrDiscarded{Err: err}, new(ErrDiscarded)))
	assert.True(t, errors.Is(ErrDiscarded{Err: err}, err))
}
