// Package fskv uses the filesystem as a key value store
package fskv

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
)

// ErrNotFound is the error returned by getter when a key is not found
var ErrNotFound = errors.New("fskv: key not found")

// FileStoreOptions Config for FileStore
type FileStoreOptions struct {
	KeyMapper KeyMapper

	// FileMode is permission bits for stored files
	FileMode os.FileMode

	// FileMode is permission bits for storage directories
	StorageDirectoryFileMode os.FileMode
}

type storedirChecks map[string]bool

// FileStore stores key values on the file system
type FileStore struct {
	basedir        string
	storedirChecks storedirChecks
	Options        FileStoreOptions
}

// NewFileStore returns a new FileStore
func NewFileStore(basedir string, options FileStoreOptions) (fs *FileStore, err error) {
	if options.FileMode == 0 {
		options.FileMode = 0644
	}

	if options.StorageDirectoryFileMode == 0 {
		options.StorageDirectoryFileMode = 0755
	}

	if options.KeyMapper == nil {
		options.KeyMapper = KeymapShardedDirectory
	}

	basedir = path.Clean(basedir)
	if basedir != "/" {
		basedir += "/"
	}

	err = os.MkdirAll(basedir, options.StorageDirectoryFileMode)
	if err != nil {
		return
	}

	return &FileStore{
		basedir:        basedir,
		storedirChecks: make(storedirChecks),
		Options:        options,
	}, nil
}

// Put creates a file to store value
func (fs *FileStore) Put(key string, value []byte) error {
	filename, err := fs.filename(key)
	if err != nil {
		return err
	}
	// TODO detect directory error and retry
	return ioutil.WriteFile(filename, value, fs.Options.FileMode)
}

func (fs *FileStore) filename(key string) (filename string, err error) {
	km := fs.Options.KeyMapper(key)

	if km.Pathname != "" {
		storedir := path.Join(fs.basedir, km.Pathname)
		err = fs.ensureStoreDirectory(storedir)
		if err != nil {
			return
		}

		filename = path.Join(storedir, km.Filename)
	} else {
		filename = fs.basedir + km.Filename
	}

	return
}

// Get returns the content for a key. Returns NotExist error if key is not found.
func (fs *FileStore) Get(key string) (data []byte, err error) {
	filename, err := fs.filename(key)
	if err != nil {
		return nil, err
	}

	// TODO dedicated error for not found
	data, err = ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return
	}

	return
}

// GetInto reads the content for a key into a writer. Returns NotExist error if key is not found.
func (fs *FileStore) GetInto(key string, w io.Writer) (err error) {
	filename, err := fs.filename(key)
	if err != nil {
		return
	}

	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return
	}
	defer f.Close()

	_, err = io.Copy(w, f)
	return
}

func (fs *FileStore) ensureStoreDirectory(storedir string) error {
	_, ok := fs.storedirChecks[storedir]

	if ok {
		return nil
	}

	// Assumes that storedir already exists
	err := os.MkdirAll(storedir, fs.Options.StorageDirectoryFileMode)
	if err != nil {
		return err
	}

	fs.storedirChecks[storedir] = true
	return nil
}
