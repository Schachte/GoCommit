package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"io/ioutil"
	"net"
	"testing"

	api_v1 "github.com/schachte/kafkaclone/api/v1"
	"github.com/schachte/kafkaclone/api/v1/logger"
	"github.com/schachte/kafkaclone/internal/config"
	"github.com/schachte/kafkaclone/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type scenarios map[string]func(t *testing.T, client logger.LogServiceClient, config *Config)

func TestServer(t *testing.T) {
	certFileContents, certFileName := config.ConfigFile("../../test_certs/server.pem")
	keyFileContents, keyFileName := config.ConfigFile("../../test_certs/server-key.pem")
	caFileContents, caFileName := config.ConfigFile("../../test_certs/ca.pem")

	tlsConfig := &config.TLSConfig{
		CertFile:      certFileContents,
		CertFileName:  certFileName,
		KeyFile:       keyFileContents,
		KeyFileName:   keyFileName,
		CAFile:        caFileContents,
		CAFileName:    caFileName,
		ServerAddress: "127.0.0.1",
		Server:        true,
	}
	testGrid := NewTestGrid()

	// Different test scenarios we want to invoke
	testGrid.addEntry("produce/consume a message to/from the log succeeds", testProduceConsume)
	testGrid.addEntry("consume past log boundary fails", testConsumePastBoundary)
	testGrid.addEntry("produce/consume stream succeeds", testProduceConsumeStream)

	for scenario, fn := range testGrid {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, *tlsConfig)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func testProduceConsumeStream(
	t *testing.T,
	client logger.LogServiceClient,
	config *Config,
) {
	ctx := context.Background()
	records := []*logger.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	stream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	for offset, record := range records {
		err = stream.Send(&logger.ProduceRequest{
			Record: record,
		})
		require.NoError(t, err)

		res, err := stream.Recv()
		require.NoError(t, err)
		if res.Offset != uint64(offset) {
			t.Fatalf("Got offset: %d, want: %d", res.Offset, offset)
		}
	}

	consumerStream, err := client.ConsumeStream(
		ctx,
		&logger.ConsumeRequest{Offset: 0},
	)
	require.NoError(t, err)

	for i, record := range records {
		res, err := consumerStream.Recv()
		require.NoError(t, err)
		require.Equal(t, res.Record, &logger.Record{
			Value:  record.Value,
			Offset: uint64(i),
		})
	}
}

func testConsumePastBoundary(t *testing.T, client logger.LogServiceClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &logger.ProduceRequest{
		Record: &logger.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &logger.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := grpc.Code(err)
	want := grpc.Code(api_v1.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("Got err: %v, want: %v", got, want)
	}

}

func testProduceConsume(t *testing.T, client logger.LogServiceClient, config *Config) {
	ctx := context.Background()
	want := &logger.Record{
		Value: []byte("hello world"),
	}
	produce, err := client.Produce(
		ctx,
		&logger.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &logger.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func setupTest(t *testing.T, tlsConfig config.TLSConfig) (client logger.LogServiceClient, cfg *Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	certFileContentsClient, certFileNameClient := config.ConfigFile("../../test_certs/server.pem")
	keyFileContentsClient, keyFileNameClient := config.ConfigFile("../../test_certs/server-key.pem")
	caFileContents, caFileName := config.ConfigFile("../../test_certs/ca.pem")

	clientTlsConfig := &config.TLSConfig{
		CertFile:     certFileContentsClient,
		CertFileName: certFileNameClient,
		KeyFile:      keyFileContentsClient,
		KeyFileName:  keyFileNameClient,
		CAFile:       caFileContents,
		CAFileName:   caFileName,
		Server:       false,
	}

	// Override this field for specifying that it's a client
	clientTLSConfig, err := config.SetupTLSConfig(clientTlsConfig)

	clientCreds := credentials.NewTLS(clientTLSConfig)

	cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	serverConfig := &Config{
		TLSConfig: tlsConfig,
		CommitLog: clog,
	}

	copyConfig := tlsConfig
	copyConfig.ServerAddress = l.Addr().String()
	serverTlsConfig, err := config.SetupTLSConfig(&copyConfig)

	if err != nil {
		panic(err)
	}
	server, err := NewGRPCServer(serverConfig, grpc.Creds(credentials.NewTLS(serverTlsConfig)))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	client = logger.NewLogServiceClient(cc)
	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
}

func NewTestGrid() scenarios {
	return make(scenarios)
}

func (s *scenarios) addEntry(key string, val func(*testing.T, logger.LogServiceClient, *Config)) {
	(*s)[key] = val
}

func DeepCopy(src, dist interface{}) (err error) {
	buf := bytes.Buffer{}
	if err = gob.NewEncoder(&buf).Encode(src); err != nil {
		return
	}
	return gob.NewDecoder(&buf).Decode(dist)
}
