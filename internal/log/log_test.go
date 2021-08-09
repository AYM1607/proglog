package log

import (
	"io"
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
		"append and rea a record succeeds": testAppendRead,
		"offset out of range error":        testOutOfRangeErr,
		"init with existing segments":      testInitExisting,
		"reader":                           testReader,
		"truncate":                         testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "log-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			// This ensures that each segment can only hold one record.
			c.Segment.MaxIndexBytes = entWidth
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(apnd)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, apnd.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, o *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := o.Append(apnd)
		require.NoError(t, err)
	}
	require.NoError(t, o.Close())

	off, err := o.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = o.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	// Create a new log from the directory and config of the old one.
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
	apnd := &api.Record{
		Value: []byte("hello world"),
	}

	off, err := log.Append(apnd)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	// Store writes the length as a prefix to the binary content so we have to skip it.
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, apnd.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}

	// Because of the configured store limit, each segment should only contain a single record.
	for i := 0; i < 3; i++ {
		_, err := log.Append(apnd)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	_, err = log.Read(0)
	require.Error(t, err)
}
