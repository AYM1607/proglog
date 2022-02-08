package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/AYM1607/proglog/api/v1"
	"github.com/AYM1607/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	lsn, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(lsn.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}

	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(lsn)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		lsn.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	pRes, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)

	cRes, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: pRes.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, want.Value, cRes.Record.Value)
	require.Equal(t, pRes.Offset, cRes.Record.Offset)
}

func testConsumePastBoundary(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	pRes, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("Hello world"),
		},
	})
	require.NoError(t, err)

	cRes, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: pRes.Offset + 1,
	})
	require.Nil(t, cRes)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	got := grpc.Code(err)
	require.Equal(t, want, got)
}

func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	// Produce.
	pStream, err := client.ProduceStream(ctx)
	require.NoError(t, err)
	for offset, record := range records {
		err := pStream.Send(&api.ProduceRequest{
			Record: record,
		})
		require.NoError(t, err)
		res, err := pStream.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(offset), res.Offset)
	}

	// Consume.
	cStream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{Offset: 0},
	)
	require.NoError(t, err)

	for offset, record := range records {
		res, err := cStream.Recv()
		require.NoError(t, err)
		require.Equal(t, &api.Record{
			Value:  record.Value,
			Offset: uint64(offset),
		}, res.Record)
	}
}
