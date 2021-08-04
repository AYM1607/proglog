package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	// Create a temp file.
	f, err := ioutil.TempFile(os.TempDir(), "index_tes")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// Use a configuration that allows enough bytes for the test.
	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err, "Read should return an error on empty index.")

	require.Equal(t, f.Name(), idx.Name(), "Name should return the same name as the underlying file.")

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err, "No error when writing to an index with enough space.")

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err, "No error when reading an existing record.")
		require.Equal(t, want.Pos, pos, "Read pos should be the same as the read one.")
	}

	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err, "Read should error with EOF when reading past the index records.")
	_ = idx.Close()

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err, "No error when creating index from an existing file.")
	off, pos, err := idx.Read(-1)
	require.NoError(t, err, "No error when reading the last record of a non empty index.")
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
