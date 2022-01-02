// Package main implements a server for Greeter service.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/lvrach/grpc-workers/workerrouter"
	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedWorkerRouterServer
	once sync.Once

	mu      sync.Mutex
	pending map[int64]task
	seqID   int64
	req     chan task
}

type task struct {
	id   int64
	resp chan error
	pb   *pb.Task
}

func (s *server) init() {
	s.once.Do(func() {
		s.pending = make(map[int64]task)
		s.seqID = 0
		s.req = make(chan task, 10)
	})
}

func (s *server) Pull(stream pb.WorkerRouter_PullServer) error {
	s.init()
	fmt.Println("pull connection")

	var activeID int64
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			s.pending[activeID].resp <- stream.Context().Err()
			return nil
		} else if err != nil {
			return err
		}
		activeID = 0

		select {
		case <-stream.Context().Done():
			fmt.Println("close connection")
			s.pending[activeID].resp <- stream.Context().Err()
			return nil
		case t := <-s.req:
			s.mu.Lock()
			s.pending[t.id] = t
			activeID = t.id
			s.mu.Unlock()

			err := stream.Send(&pb.Task{Id: t.id, Payload: t.pb.Payload})
			if err != nil {
				return err
			}
		}

	}
}

func (s *server) Complete(ctx context.Context, t *pb.TaskComplete) (*pb.Empty, error) {
	s.init()
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	if t.Error != "" {
		err = fmt.Errorf("%s", t.Error)
	}
	s.pending[t.Id].resp <- err

	delete(s.pending, t.Id)
	return &pb.Empty{}, nil
}

func (s *server) Request(ctx context.Context, payload string) error {
	s.init()

	resp := make(chan error)
	nextID := atomic.AddInt64(&s.seqID, 1)
	s.req <- task{
		id:   nextID,
		resp: resp,
		pb:   &pb.Task{Id: nextID, Payload: payload},
	}

	return <-resp
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	workShard := &server{}
	s := grpc.NewServer()
	pb.RegisterWorkerRouterServer(s, workShard)
	log.Printf("server listening at %v", lis.Addr())

	// FIXME: Added for benchmarking
	go func() {
		s := time.Now()
		for i := 0; i < 1_000_000; i++ { //2m6.123736752s
			err := workShard.Request(context.Background(), " ")
			if err != nil {
				fmt.Println(err)
			}
		}
		fmt.Println(time.Since(s))
	}()
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
