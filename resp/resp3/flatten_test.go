package resp3

import (
	"testing"

	"github.com/mediocregopher/radix/v4/resp"
	"github.com/stretchr/testify/suite"
)

type FlattenTestSuite struct {
	suite.Suite
}

type NestedStruct struct{}

type sliceTestStruct struct {
	Other  string
	Nested []NestedStruct
}

func (s *FlattenTestSuite) TestEmptyNestedSlice() {
	testInst := sliceTestStruct{
		Other: "hello",
	}

	flat, err := Flatten(testInst, resp.NewOpts())
	s.NoError(err)
	s.Equal([]string{"Other", "hello"}, flat)
}

func TestFlattenTestSuite(t *testing.T) {
	suite.Run(t, new(FlattenTestSuite))
}
