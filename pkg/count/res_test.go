package count

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testData = `the,1603
and,766
to,706
a,614
she,517
`

var words = []WordWithFrequency{
	{"the", 1603},
	{"and", 766},
	{"to", 706},
	{"a", 614},
	{"she", 517},
}

func TestWriteResult(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	err := WriteResult(&buf, words)
	assert.NoError(t, err)
	assert.Equal(t, testData, buf.String())
}

func TestReadResult(t *testing.T) {
	t.Parallel()

	r := strings.NewReader(testData)
	res, err := ReadResult(r)
	assert.NoError(t, err)
	assert.Equal(t, words, res)
}
