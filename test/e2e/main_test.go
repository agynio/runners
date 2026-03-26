//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultRunnersAddress = "runners:50051"
	dialTimeout           = 20 * time.Second
)

var (
	runnerClient runnersv1.RunnersServiceClient
	runnerConn   *grpc.ClientConn
)

func envOrDefault(key, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func TestMain(m *testing.M) {
	addr := envOrDefault("RUNNERS_ADDRESS", defaultRunnersAddress)

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", addr, err)
		os.Exit(1)
	}

	runnerConn = conn
	runnerClient = runnersv1.NewRunnersServiceClient(conn)

	exitCode := m.Run()
	if err := conn.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close gRPC connection: %v\n", err)
	}
	os.Exit(exitCode)
}
