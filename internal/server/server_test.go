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
	"github.com/schachte/kafkaclone/internal/authorizer"
	"github.com/schachte/kafkaclone/internal/config"
	"github.com/schachte/kafkaclone/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type scenarios map[string]func(*testing.T, *TestConnections, []logger.LogServiceClient, *Config)

type TestConnections struct {
	RootConnection  *grpc.ClientConn
	DummyConnection *grpc.ClientConn
}

func TestServer(t *testing.T) {
	certFileContents, certFileName := config.ConfigFile("../../test_certs/server.pem")
	keyFileContents, keyFileName := config.ConfigFile("../../test_certs/server-key.pem")
	caFileContents, caFileName := config.ConfigFile("../../test_certs/ca.pem")

	ACLModelFile, err := config.LoadFileFromPath("../../acl/model.conf")
	require.NoError(t, err)

	ACLPolicyFile, err := config.LoadFileFromPath("../../acl/policy.csv")
	require.NoError(t, err)

	tlsConfig := &config.TLSConfig{
		CertFile:      certFileContents,
		CertFileName:  certFileName,
		KeyFile:       keyFileContents,
		KeyFileName:   keyFileName,
		CAFile:        caFileContents,
		CAFileName:    caFileName,
		ServerAddress: "127.0.0.1",
		Server:        true,
		ACLModelFile:  ACLModelFile,
		ACLPolicyFile: ACLPolicyFile,
	}

	testGrid := NewTestGrid()

	// Different test scenarios we want to invoke
	testGrid.addEntry("produce/consume a message to/from the log succeeds", testProduceConsume)
	testGrid.addEntry("consume past log boundary fails", testConsumePastBoundary)
	testGrid.addEntry("produce/consume stream succeeds", testProduceConsumeStream)
	testGrid.addEntry("unauthorized fails", testUnauthorized)

	for scenario, fn := range testGrid {
		t.Run(scenario, func(t *testing.T) {
			clients, conns, config, teardown := setupTest(t, *tlsConfig)
			defer teardown()
			connections := &TestConnections{
				RootConnection:  conns[0],
				DummyConnection: conns[1],
			}
			fn(t, connections, clients, config)
		})
	}
}

func testUnauthorized(t *testing.T, _ *TestConnections, clients []logger.LogServiceClient, config *Config) {
	ctx := context.Background()
	unauthorizedClient := clients[1]
	produce, err := unauthorizedClient.Produce(ctx, &logger.ProduceRequest{
		Record: &logger.Record{
			Value: []byte("Hello world"),
		},
	})
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("Got code: %d, want: %d", gotCode, wantCode)
	}
}

func testProduceConsumeStream(
	t *testing.T,
	conns *TestConnections,
	clients []logger.LogServiceClient,
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

	stream, err := clients[0].ProduceStream(ctx)
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

	consumerStream, err := clients[0].ConsumeStream(
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

func testConsumePastBoundary(t *testing.T, conns *TestConnections, clients []logger.LogServiceClient, config *Config) {
	ctx := context.Background()
	produce, err := clients[0].Produce(ctx, &logger.ProduceRequest{
		Record: &logger.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := clients[0].Consume(ctx, &logger.ConsumeRequest{
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

func testProduceConsume(t *testing.T,
	conns *TestConnections,
	client []logger.LogServiceClient, config *Config) {
	ctx := context.Background()
	want := &logger.Record{
		Value: []byte("hello world"),
	}
	produce, err := client[0].Produce(
		ctx,
		&logger.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	consume, err := client[0].Consume(ctx, &logger.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func setupTest(t *testing.T, tlsConfig config.TLSConfig) (clients []logger.LogServiceClient, conns []*grpc.ClientConn, cfg *Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, logger.LogServiceClient, []grpc.DialOption) {
		certFileContentsClient, certFileNameClient := config.ConfigFile(crtPath)
		keyFileContentsClient, keyFileNameClient := config.ConfigFile(keyPath)
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
		opts := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
		cc, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := logger.NewLogServiceClient(cc)
		return cc, client, opts
	}

	rootCon, rootConClient, _ := newClient("../../test_certs/server.pem", "../../test_certs/server-key.pem")
	nobodyCon, nobodyConClient, _ := newClient("../../test_certs/client.pem", "../../test_certs/client-key.pem")

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := authorizer.New(tlsConfig.ACLModelFile.Name(), tlsConfig.ACLPolicyFile.Name())
	serverConfig := &Config{
		TLSConfig:  tlsConfig,
		CommitLog:  clog,
		Authorizer: authorizer,
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

	return []logger.LogServiceClient{rootConClient, nobodyConClient}, []*grpc.ClientConn{rootCon, nobodyCon}, cfg, func() {
		server.Stop()
		rootCon.Close()
		nobodyCon.Close()
		l.Close()
		clog.Remove()
	}
}

func NewTestGrid() scenarios {
	return make(scenarios)
}

func (s *scenarios) addEntry(key string, val func(*testing.T, *TestConnections, []logger.LogServiceClient, *Config)) {
	(*s)[key] = val
}

func DeepCopy(src, dist interface{}) (err error) {
	buf := bytes.Buffer{}
	if err = gob.NewEncoder(&buf).Encode(src); err != nil {
		return
	}
	return gob.NewDecoder(&buf).Decode(dist)
}
