package node

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/Conflux-Chain/confura/node/router/proto"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type blockingRouter struct {
	pb.UnimplementedRouterServer
	entered, exited chan struct{}
}

func (r *blockingRouter) Route(ctx context.Context, _ *pb.RouteRequest) (*pb.RouteResponse, error) {
	close(r.entered)
	<-ctx.Done()
	close(r.exited)
	return nil, ctx.Err()
}

func TestGRPCShutdownCancelsAndJoinsLongRequest(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	handler := &blockingRouter{entered: make(chan struct{}), exited: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	serveGRPC(ctx, &wg, l, handler)
	conn, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	called := make(chan error, 1)
	go func() {
		_, err := pb.NewRouterClient(conn).Route(context.Background(), &pb.RouteRequest{})
		called <- err
	}()
	select {
	case <-handler.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("gRPC shutdown did not finish")
	}
	select {
	case <-handler.exited:
	default:
		t.Fatal("server released resources before request exit")
	}
	require.Error(t, <-called)
}

func TestNodeServerLaterBindFailureUnwinds(t *testing.T) {
	viper.Reset()
	defer viper.Reset()
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	f := newFactory(nil, "127.0.0.1:0", occupied.Addr().String(), nil, "", "")
	require.Error(t, f.StartServer(ctx, &wg, nil))
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("failed startup retained node tasks")
	}
}
