package fskv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeymapShardedDirectory(t *testing.T) {
	assert := require.New(t)

	var cases = []struct {
		key    string
		result Keymap
	}{
		{"foo", Keymap{"dc", "b27518fed9d577"}},
		{"bar", Keymap{"00", "3934191339461a"}},
	}

	for _, c := range cases {
		km := KeymapShardedDirectory(c.key)
		assert.Equal(
			km.Filename,
			c.result.Filename,
		)

		assert.Equal(
			km.Pathname,
			c.result.Pathname,
		)
	}
}

func TestKeymapToplevel(t *testing.T) {
	assert := require.New(t)

	var cases = []struct {
		key    string
		result Keymap
	}{
		{"foo", Keymap{"", "dcb27518fed9d577"}},
		{"bar", Keymap{"", "003934191339461a"}},
	}

	for _, c := range cases {
		km := KeymapToplevel(c.key)
		assert.Equal(
			km.Filename,
			c.result.Filename,
		)

		assert.Equal(
			km.Pathname,
			c.result.Pathname,
		)
	}
}
