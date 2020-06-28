package resp

import (
	. "testing"

	"errors"

	"github.com/stretchr/testify/assert"
)

func TestErrConnUsable(t *T) {
	err := errors.New("foo")
	assert.False(t, errors.As(err, new(ErrConnUsable)))
	assert.True(t, errors.As(ErrConnUsable{Err: err}, new(ErrConnUsable)))
	assert.True(t, errors.Is(ErrConnUsable{Err: err}, err))
}
