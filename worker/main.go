package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "github.com/lvrach/grpc-workers/workerrouter"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var addr = flag.String("addr", "localhost:50051", "the address to connect to")

func process(ctx context.Context, c pb.WorkerRouterClient) error {
	stream, err := c.Pull(ctx)
	if err != nil {
		return err
	}

	for {
		stream.Send(&pb.Empty{})

		t, err := stream.Recv()
		if err != nil {
			return err
		}

		time.Sleep(time.Millisecond)

		_, err = c.Complete(ctx, &pb.TaskComplete{
			Id: t.Id,
		})
		if err != nil {
			return err
		}
	}
}

func main() {
	flag.Parse()

	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewWorkerRouterClient(conn)

	ctx := context.Background()

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			return (process(ctx, c))
		})
	}

	if err := g.Wait(); err != nil {
		fmt.Println(err)
	}
}
