package server

import (
	"context"

	api_v1 "github.com/schachte/kafkaclone/api/v1"
	"github.com/schachte/kafkaclone/api/v1/logger"
	"google.golang.org/grpc"
)

type CommitLog interface {
	Append(*logger.Record) (uint64, error)
	Read(uint64) (*logger.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

type grpcServer struct {
	logger.UnimplementedLogServiceServer
	*Config
}

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := grpcFactory(config)
	if err != nil {
		return nil, err
	}
	logger.RegisterLogServiceServer(gsrv, srv)
	return gsrv, nil
}

func grpcFactory(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{Config: config}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *logger.ProduceRequest) (*logger.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &logger.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *logger.ConsumeRequest) (*logger.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &logger.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream logger.LogService_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *logger.ConsumeRequest, stream logger.LogService_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api_v1.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
