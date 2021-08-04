package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/AYM1607/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxIndexBytes = entWidth * 3
	c.Segment.MaxStoreBytes = 1024

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err, "The segment is created successfully")
	require.Equal(t, uint64(16), s.nextOffset, "Next offset should be equal to base offset for new segments.")

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err, "Record should be appended successfully.")
		require.Equal(t, off, 16+i, "The offsets should be increase by 1 with respect to the base offset.")

		got, err := s.Read(off)
		require.NoError(t, err, "Existing records should be read successfully.")
		require.Equal(t, want.Value, got.Value, "Record's read data should be the same as the written one.")
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err, "Appends should fail if the segment is maxed.")

	require.True(t, s.IsMaxed(), "IsMaxed should return true when the segment's index is maxed.")

	// Create a new segment from the same files with a different configuration.
	c.Segment.MaxIndexBytes = 1024
	// The length of the record's value is not the same as the byte size of the
	// marshalled data which includes other fields. By setting this as the limit though,
	// we assure that the store will be maxed with only 3 records.
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err, "Segment should be created successfully from existing files.")
	require.True(t, s.IsMaxed(), "IsMaxed should return true when the segment's store is maxed.")

	err = s.Remove()
	require.NoError(t, err, "Segment should be able to remove itself.")

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err, "Segment should create the necessary files after removed.")
	require.False(t, s.IsMaxed(), "Segment should not be maxed if new and config's limits are non-zero.")

}
