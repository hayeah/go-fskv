package fskv

import (
	"fmt"
	"hash/fnv"
	"io"
)

// Key specifies a location in the file system to store the data
type Key interface {
	Location() (pathname string, filename string)
}

// HashKey hashes the string key as storage path
type HashKey string

// Location returns the storage location
func (s HashKey) Location() (pathname string, filename string) {
	h := fnv.New64a()
	io.WriteString(h, string(s))
	digest := h.Sum(nil)

	pathname = ""
	filename = fmt.Sprintf("%x", digest)
	return
}

// ShardedHashKey maps a string key to nested directories to prevent one directory with large number or files
type ShardedHashKey string

// Location returns the storage location
func (s ShardedHashKey) Location() (pathname string, filename string) {
	h := fnv.New64a()
	io.WriteString(h, string(s))
	digest := h.Sum(nil)

	pathname = fmt.Sprintf("%x", digest[0:1])
	filename = fmt.Sprintf("%x", digest[1:])
	return
}
