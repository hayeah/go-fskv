package fskv

import (
	"fmt"
	"hash/fnv"
	"io"
)

// Keymap specifies how to map a key to a valid path
type Keymap struct {
	Pathname string
	Filename string
}

// KeyMapper maps a key to a valid path to store a file
type KeyMapper func(key string) Keymap

// KeymapToplevel maps all keys to top level files
func KeymapToplevel(key string) Keymap {
	h := fnv.New64a()
	io.WriteString(h, key)
	digest := h.Sum(nil)

	return Keymap{
		Filename: fmt.Sprintf("%x", digest),
	}
}

// KeymapShardedDirectory maps a key to nested directories to prevent one directory with large number or files
func KeymapShardedDirectory(key string) Keymap {
	h := fnv.New64a()
	io.WriteString(h, key)
	digest := h.Sum(nil)

	return Keymap{
		Pathname: fmt.Sprintf("%x", digest[0:1]),
		Filename: fmt.Sprintf("%x", digest[1:]),
	}
}
