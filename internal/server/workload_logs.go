package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	runnerv1 "github.com/agynio/runners/.gen/go/agynio/api/runner/v1"
	"github.com/openziti/sdk-golang/ziti/edge"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	dialRetryInitialBackoff = 500 * time.Millisecond
	dialRetryMaxBackoff     = 10 * time.Second
	dialRetryMaxAttempts    = 5
)

var errZitiContextUnavailable = errors.New("ziti context unavailable")

type ZitiDialer interface {
	DialContext(ctx context.Context, service string) (edge.Conn, error)
	NotifyAuthFailure(ctx context.Context)
}

type runnerConnDialer func(ctx context.Context, serviceName string) (*grpc.ClientConn, error)

func (s *Server) StreamWorkloadLogs(req *runnerv1.StreamWorkloadLogsRequest, stream grpc.ServerStreamingServer[runnerv1.StreamWorkloadLogsResponse]) error {
	ctx := stream.Context()
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	workloadID, err := parseUUID(req.GetWorkloadId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "workload_id: %v", err)
	}
	containerName := strings.TrimSpace(req.GetContainerName())
	if containerName == "" {
		return status.Error(codes.InvalidArgument, "container_name must be provided")
	}

	workload, err := s.getWorkloadByID(ctx, workloadID)
	if err != nil {
		return toStatusError(err)
	}
	if err := s.requireOrgMember(ctx, callerID, workload.OrganizationID); err != nil {
		return err
	}

	instanceID := ""
	if workload.InstanceID != nil {
		instanceID = strings.TrimSpace(*workload.InstanceID)
	}
	if instanceID == "" {
		return status.Error(codes.FailedPrecondition, "workload instance_id is required")
	}

	runner, err := s.getRunnerByID(ctx, workload.RunnerID)
	if err != nil {
		return toStatusError(err)
	}
	serviceName := strings.TrimSpace(runner.ZitiServiceName)
	if serviceName == "" {
		return status.Error(codes.Internal, "runner ziti service name is required")
	}

	conn, err := s.dialRunnerConn(ctx, serviceName)
	if err != nil {
		if errors.Is(err, errZitiContextUnavailable) {
			return status.Error(codes.Unavailable, err.Error())
		}
		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, err.Error())
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, err.Error())
		}
		return status.Errorf(codes.Unavailable, "dial runner: %v", err)
	}
	defer conn.Close()

	// Runner log streaming expects workload instance_id, not platform workload UUID.
	runnerReq := &runnerv1.StreamWorkloadLogsRequest{
		WorkloadId:    instanceID,
		Follow:        req.GetFollow(),
		Since:         req.GetSince(),
		Tail:          req.GetTail(),
		Stdout:        req.GetStdout(),
		Stderr:        req.GetStderr(),
		Timestamps:    req.GetTimestamps(),
		ContainerName: containerName,
		TailLines:     req.GetTailLines(),
		SinceTime:     req.GetSinceTime(),
	}

	runnerClient := runnerv1.NewRunnerServiceClient(conn)
	grpcStream, err := runnerClient.StreamWorkloadLogs(ctx, runnerReq)
	if err != nil {
		return err
	}

	for {
		msg, err := grpcStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
}

func (s *Server) dialRunnerConnWithZiti(ctx context.Context, serviceName string) (*grpc.ClientConn, error) {
	if s.zitiDialer == nil {
		return nil, errZitiContextUnavailable
	}

	target := "passthrough:///" + serviceName
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return dialZitiWithRetry(ctx, s.zitiDialer, serviceName)
		}),
	)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func dialZitiWithRetry(ctx context.Context, zitiDialer ZitiDialer, service string) (net.Conn, error) {
	backoff := dialRetryInitialBackoff
	var lastErr error

	for attempt := 1; attempt <= dialRetryMaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		conn, err := zitiDialer.DialContext(ctx, service)
		if err == nil {
			return conn, nil
		}
		if isNoTerminators(err) {
			return nil, fmt.Errorf("dial ziti service %s: %w", service, err)
		}
		log.Printf("dial ziti service %s: attempt %d/%d failed: %v", service, attempt, dialRetryMaxAttempts, err)
		lastErr = err
		if isAuthFailure(err) {
			zitiDialer.NotifyAuthFailure(ctx)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		if attempt == dialRetryMaxAttempts {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
		if backoff > dialRetryMaxBackoff {
			backoff = dialRetryMaxBackoff
		}
	}
	return nil, fmt.Errorf("dial ziti service %s: %w", service, lastErr)
}

func isNoTerminators(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no terminators")
}

func isAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "INVALID_AUTH") || strings.Contains(msg, "no apiSession")
}
