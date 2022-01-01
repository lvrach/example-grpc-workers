// Package main implements a server for Greeter service.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	pb "github.com/lvrach/grpc-workers/workerrouter"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedWorkerRouterServer

	pending map[int64]task
	nextID  int64
	req     chan task
}

type task struct {
	id   int64
	resp chan error
	pb   *pb.Task
}

func (s *server) Pull(stream pb.WorkerRouter_PullServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		t := <-s.req
		err = stream.Send(t.pb)
		if err != nil {
			return err
		}
	}
}

func (s *server) Complete(context.Context, *pb.TaskComplete) (*pb.Empty, error) {

	return nil, nil
}

func (s *server) Request(ctx context.Context, payload string) error {
	resp := make(chan error)
	id := s.nextID
	s.req <- task{
		id:   s.nextID,
		resp: resp,
		pb:   &pb.Task{Id: id, Payload: payload},
	}

	return <-resp
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterWorkerRouterServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
