package node

import (
	"context"
	"net"
	"sync"
	"time"

	pb "github.com/Conflux-Chain/confura/node/router/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StartGRPC binds the router service before returning.
func StartGRPC(ctx context.Context, wg *sync.WaitGroup, endpoint string, handler *apiHandler) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		return err
	}
	serveGRPC(ctx, wg, listener, &grpcRouterServer{handler: handler})
	return nil
}

func serveGRPC(ctx context.Context, wg *sync.WaitGroup, listener net.Listener, handler pb.RouterServer) {
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	pb.RegisterRouterServer(server, handler)
	wg.Add(1)
	go func() {
		defer wg.Done()
		served := make(chan struct{})
		go func() { defer close(served); _ = server.Serve(listener) }()
		select {
		case <-ctx.Done():
		case <-served:
		}
		stopped := make(chan struct{})
		go func() { server.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			server.Stop()
			<-stopped
		}
		<-served
	}()
}

func MustServeGRPC(ctx context.Context, wg *sync.WaitGroup, endpoint string, handler *apiHandler) {
	if err := StartGRPC(ctx, wg, endpoint, handler); err != nil {
		logrus.WithError(err).Fatal("Failed to start gRPC router")
	}
	<-ctx.Done()
}

type grpcRouterServer struct {
	pb.UnimplementedRouterServer

	handler *apiHandler
}

func (server *grpcRouterServer) Route(ctx context.Context, request *pb.RouteRequest) (*pb.RouteResponse, error) {
	manager, ok := server.handler.pool.manager(Group(request.Group))
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid group [%v]", request.Group)
	}

	return &pb.RouteResponse{
		Url: manager.Route(request.Key),
	}, nil
}
