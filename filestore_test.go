package fskv

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os/exec"
	"testing"

	"bytes"

	"github.com/stretchr/testify/require"
)

const storedir = "tmp/store"
const testImage = "testdata/270k.jpg"

func TestFileStorePut(t *testing.T) {
	defer cleanup()

	assert := require.New(t)

	fs, err := NewFileStore(storedir, FileStoreOptions{})
	assert.NoError(err)

	var cases = []struct {
		key      string
		filename string
		content  string
	}{
		{"foo", "tmp/store/dc/b27518fed9d577", "abc"},
		{"bar", "tmp/store/00/3934191339461a", "efg"},
	}

	for _, c := range cases {
		key := ShardedHashKey(c.key)
		err = fs.Put(key, []byte(c.content))
		assert.NoError(err)

		data, err := ioutil.ReadFile(c.filename)
		assert.NoError(err)

		assert.EqualValues(string(data), c.content)
	}
}

func TestFileStoreGet(t *testing.T) {
	defer cleanup()

	assert := require.New(t)

	fs, err := NewFileStore(storedir, FileStoreOptions{})
	assert.NoError(err)

	var cases = []struct {
		key     string
		put     bool
		content string
	}{
		{"foo", true, "abc"},
		{"bar", true, "efg"},
		{"qux", false, ""},
	}

	for _, c := range cases {
		key := ShardedHashKey(c.key)
		if c.put {
			err = fs.Put(key, []byte(c.content))
			assert.NoError(err)
		}

		data, err := fs.Get(key)

		if c.put {
			assert.NoError(err)
			assert.Equal(c.content, string(data))
		} else {
			assert.Equal(err, ErrNotFound)
		}

		var w bytes.Buffer
		err = fs.GetInto(key, &w)
		if c.put {
			assert.NoError(err)
			assert.Equal(c.content, string(w.Bytes()))
		} else {
			assert.Equal(err, ErrNotFound)
		}
	}
}

func BenchmarkFileStorePutRandom(b *testing.B) {
	defer cleanup()

	fs, err := NewFileStore(storedir, FileStoreOptions{})
	if err != nil {
		log.Fatal(err)
	}

	file, err := ioutil.ReadFile(testImage)
	if err != nil {
		log.Fatal(err)
	}

	const nkeys = 5000
	keys := make([]ShardedHashKey, nkeys, nkeys)
	for i := range keys {
		keys[i] = ShardedHashKey(randString(10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%nkeys]
		err = fs.Put(key, file)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkFileStoreGetRandom(b *testing.B) {
	defer cleanup()

	fs, err := NewFileStore(storedir, FileStoreOptions{})
	if err != nil {
		log.Fatal(err)
	}

	file, err := ioutil.ReadFile(testImage)
	if err != nil {
		log.Fatal(err)
	}

	const nkeys = 200
	keys := make([]ShardedHashKey, nkeys, nkeys)
	for i := range keys {
		keys[i] = ShardedHashKey(randString(10))
	}

	for _, key := range keys {
		err = fs.Put(key, file)
		if err != nil {
			log.Fatal(err)
		}
	}

	var w bytes.Buffer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Reset()
		key := keys[i%nkeys]
		err := fs.GetInto(key, &w)
		if err != nil {
			log.Fatal(err)
		}
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func cleanup() {
	exec.Command("rm", "-rf", storedir).Run()
}
