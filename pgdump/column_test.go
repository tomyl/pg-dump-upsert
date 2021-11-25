package pgdump

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuoteString(t *testing.T) {
	require.Equal(t, "'foobar'", quoteString("foobar"))
	require.Equal(t, "'foo''bar'", quoteString("foo'bar"))
}
