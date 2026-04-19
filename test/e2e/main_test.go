//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	defaultRunnersAddress       = "runners:50051"
	defaultAuthorizationAddress = "authorization:50051"
	dialTimeout                 = 20 * time.Second
	identityMetadataKey         = "x-identity-id"
	identityTypeMetadataKey     = "x-identity-type"
	identityTypeUser            = "user"
	identityTypeAgent           = "agent"
	clusterAdminIdentityID      = "a3c1e9d2-7f4b-5e1a-9c3d-2b8f6a4e7d10"
)

var (
	runnerClient runnersv1.RunnersServiceClient
	runnerConn   *grpc.ClientConn
	authzClient  authorizationv1.AuthorizationServiceClient
	authzConn    *grpc.ClientConn
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
	authorizationAddr := envOrDefault("AUTHORIZATION_ADDRESS", defaultAuthorizationAddress)

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", addr, err)
		os.Exit(1)
	}

	runnerConn = conn
	runnerClient = runnersv1.NewRunnersServiceClient(conn)

	authorizationConn, err := grpc.DialContext(ctx, authorizationAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", authorizationAddr, err)
		_ = conn.Close()
		os.Exit(1)
	}
	authzConn = authorizationConn
	authzClient = authorizationv1.NewAuthorizationServiceClient(authorizationConn)

	exitCode := m.Run()
	if err := conn.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close gRPC connection: %v\n", err)
	}
	if err := authorizationConn.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close authorization connection: %v\n", err)
	}
	os.Exit(exitCode)
}

func contextWithIdentity(ctx context.Context, identityID string, identityType string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(
		identityMetadataKey, identityID,
		identityTypeMetadataKey, identityType,
	))
}

func adminContext(ctx context.Context) context.Context {
	return contextWithIdentity(ctx, clusterAdminIdentityID, identityTypeUser)
}

func agentContext(ctx context.Context, agentID string) context.Context {
	return contextWithIdentity(ctx, agentID, identityTypeAgent)
}
