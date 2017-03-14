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

// IsNotFound is true if the err is ErrNotFound
func IsNotFound(err error) bool {
	return err == ErrNotFound
}

// FileStoreOptions Config for FileStore
type FileStoreOptions struct {
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
func (fs *FileStore) Put(key Key, value []byte) error {
	pathname, filename := key.Location()

	if pathname != "" {
		storedir := path.Join(fs.basedir, pathname)
		err := fs.ensureStoreDirectory(storedir)
		if err != nil {
			return err
		}
	}

	storefile := path.Join(fs.basedir, pathname, filename)

	// TODO detect directory error and retry
	return ioutil.WriteFile(storefile, value, fs.Options.FileMode)
}

// Get returns the content for a key. Returns NotExist error if key is not found.
func (fs *FileStore) Get(key Key) (data []byte, err error) {
	pathname, filename := key.Location()
	storefile := path.Join(fs.basedir, pathname, filename)

	data, err = ioutil.ReadFile(storefile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return
	}

	return
}

// GetInto reads the content for a key into a writer. Returns NotExist error if key is not found.
func (fs *FileStore) GetInto(key Key, w io.Writer) (err error) {
	pathname, filename := key.Location()
	storefile := path.Join(fs.basedir, pathname, filename)

	f, err := os.Open(storefile)
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
