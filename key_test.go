package fskv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardedHashKey(t *testing.T) {
	is := require.New(t)

	var cases = []struct {
		key      string
		pathname string
		filename string
	}{
		{"foo", "dc", "b27518fed9d577"},
		{"bar", "00", "3934191339461a"},
	}

	for _, c := range cases {
		p, f := ShardedHashKey(c.key).Location()

		is.Equal(c.pathname, p)
		is.Equal(c.filename, f)
	}
}

func TestHashKey(t *testing.T) {
	is := require.New(t)

	var cases = []struct {
		key      string
		pathname string
		filename string
	}{
		{"foo", "", "dcb27518fed9d577"},
		{"bar", "", "003934191339461a"},
	}

	for _, c := range cases {
		p, f := HashKey(c.key).Location()

		is.Equal(c.pathname, p)
		is.Equal(c.filename, f)
	}
}
