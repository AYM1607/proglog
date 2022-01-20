package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/AYM1607/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

const (
	baseOff uint64 = 16
)

func TestSegment(t *testing.T) {
	dir, err := ioutil.TempDir("", "segmet_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world!")}

	// Index-limited config.
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, baseOff, c)
	require.NoError(t, err)
	require.Equal(t, baseOff, s.nextOffset, "next offset is the base offset for an empty segment")
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, baseOff+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.True(t, s.IsMaxed())
	require.Equal(t, io.EOF, err, "Append fails when the index is full")

	// Store-limited config.
	// This is not really accurate. The Marshalled record with the added bytes
	// for the length will be longer that just the length of the value in bytes.
	// If more fields are added to the record, 2 could cause the store to fill up
	// and this test would fail.
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	// Create from the existing files.
	s, err = newSegment(dir, baseOff, c)
	require.NoError(t, err)
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)

	// Re-create files.
	s, err = newSegment(dir, baseOff, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
