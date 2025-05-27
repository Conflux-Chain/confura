package node

import (
	"context"
	"net"
	"sync"

	pb "github.com/Conflux-Chain/confura/node/router/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MustStartGRPCRouterServer starts gRPC router service.
func MustStartGRPCRouterServer(ctx context.Context, wg *sync.WaitGroup, endpoint string, handler *apiHandler) {
	wg.Add(1)
	defer wg.Done()

	listener, err := net.Listen("tcp", endpoint)
	if err != nil {
		logrus.WithError(err).WithField("endpoint", endpoint).Fatal("Failed to listen for gRPC router server")
	}

	server := grpc.NewServer()
	pb.RegisterRouterServer(server, &grpcRouterServer{handler: handler})
	go server.Serve(listener)

	logrus.WithField("endpoint", endpoint).Info("Succeeded to run gRPC router server")

	<-ctx.Done()

	server.GracefulStop()
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
