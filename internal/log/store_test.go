package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

const rCnt = 3

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	// Test basic operations on the store.
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// Test that the store can be reconstructed from an existing file.
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i <= rCnt; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, width*i, pos+n, "Bytes written to the store file must be the length of the message + `lenWidth` bytes for length")
	}
}

// testRead ensures `rCnt` records were written to the store and the contet matches `write`.
func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i <= rCnt; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read, "Record value should match the written one.")
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i <= rCnt; i++ {
		// Read record size.
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		size := enc.Uint64(b)
		off += int64(n)

		// Read record content.
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, write, b)
		off += int64(n)
	}
}

func TestClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	_, _, err = s.Append(write)
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.Greater(t, afterSize, beforeSize)
}

func openFile(fn string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		fn,
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
