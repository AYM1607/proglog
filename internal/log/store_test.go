package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write) + lenWidth)
)

func TestStoreAppendRead(t *testing.T) {
	// Create a temprorary file with a random name.
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// Create a new store from the file.
	s, err := newStore(f)
	require.NoError(t, err)

	// Basic operations.
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// A store can be created from an existing non-empty file.
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		// test the returned offsets.
		require.Equal(t, width*i, pos+n)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		// Read the size of the record at the current offset.
		b := make([]byte, lenWidth)
		n, err := s.File.ReadAt(b, off)
		require.NoError(t, err)
		// bytes read same as predifined byte size for length.
		require.Equal(t, lenWidth, n)
		off += int64(n)

		// Read the actual content of the record.
		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		// Read the correct number of bytes.
		require.Equal(t, int(size), n)
		// The content read is correct.
		require.Equal(t, write, b)
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	// Append a single record.
	_, _, err = s.Append(write)
	require.NoError(t, err)

	_, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)

	// A store buffers its writes so the size of the underlying file should change after close.
	require.Greater(t, afterSize, beforeSize)

}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
