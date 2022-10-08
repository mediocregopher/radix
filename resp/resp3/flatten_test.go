package resp3

import (
	"testing"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/stretchr/testify/suite"
)

type FlattenTestSuite struct {
	suite.Suite
}

// Test whether by default, an empty slice in a hashmap should be flattened to an empty value.
func (s *FlattenTestSuite) TestEmptyNestedSlice() {
	testInst := struct {
		Other  string
		Nested []string
	}{
		Other: "hello",
	}

	flat, err := Flatten(testInst, resp.NewOpts())
	s.NoError(err)

	s.Equal([]string{"Other", "hello", "Nested", ""}, flat)
}

// Test with omitempty; empty slices should be left off altogether.
func (s *FlattenTestSuite) TestOmitEmptyNestedSlice() {
	testInst := struct {
		Other           string
		NestedOmitEmpty []string `redis:",omitempty"`
	}{
		Other: "hello",
	}

	flat, err := Flatten(testInst, resp.NewOpts())
	s.NoError(err)

	s.Equal([]string{"Other", "hello"}, flat)
}

func TestFlattenTestSuite(t *testing.T) {
	suite.Run(t, new(FlattenTestSuite))
}
