package log

import (
	"io/ioutil"
	"os"
	"testing"

	api "github.com/AYM1607/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record suceeds": testAppendRead,
		"offset out of range error":        testOutOfRangeErr,
		"init with existing segments":      testInitExisting,
		"reader":                           testReader,
		"truncate":                         testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			// Guarantee that each segment will only have one record.
			c.Segment.MaxIndexBytes = entWidth
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	want := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(want)
	require.NoError(t, err, "Record is appended successfully")
	require.Equal(t, uint64(0), off, "First written record has offset 0")

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, want.Value, read.Value)
	require.Equal(t, off, read.Offset)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(10)
	require.Nil(t, read)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(10), apiErr.Offset)
}

func testInitExisting(t *testing.T, o *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := o.Append(record)
		require.NoError(t, err)
	}
	require.NoError(t, o.Close())

	off, err := o.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = o.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	n, err := NewLog(o.Dir, o.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	want := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(want)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read) // Ignore the bytes used to store the length of the record.
	require.NoError(t, err)
	require.Equal(t, want.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(record)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}
