package server

import (
	"context"
	"strings"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// reportableStatuses are the states a runner may report.
//
// Observed state only. Stopping and stopped are lifecycle -- the platform
// decides a workload should end and drives it -- so a runner claiming either
// would let a node take a workload down that the Orchestrator intends to keep
// running. What a runner can legitimately say is that a workload came up, that
// it failed, or that it is gone.
var reportableStatuses = map[runnersv1.WorkloadStatus]struct{}{
	runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING: {},
	runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:  {},
}

// ReportWorkloadState records the runtime state a runner observed for one of
// its own workloads.
//
// The platform used to learn this by asking: the Orchestrator dialed the runner
// on its reconcile interval and called InspectWorkload, so a workload was ready
// and serving for up to a full interval before anything recorded it. This is
// the same write it would have made, moved to the moment the runner saw it.
//
// It changes nothing about who owns the workload. The Orchestrator's
// reconciliation still runs and still converges anything a report never reached
// -- a runner that restarts, loses its watch, or predates this RPC costs
// latency, not correctness.
func (s *Server) ReportWorkloadState(ctx context.Context, req *runnersv1.ReportWorkloadStateRequest) (*runnersv1.ReportWorkloadStateResponse, error) {
	serviceToken := strings.TrimSpace(req.GetServiceToken())
	if serviceToken == "" {
		return nil, status.Error(codes.InvalidArgument, "service_token must be provided")
	}
	runner, err := s.getRunnerByServiceTokenHash(ctx, hashServiceToken(serviceToken))
	if err != nil {
		return nil, toStatusError(err)
	}
	// The token identifies the runner; a runner_id that disagrees with it is a
	// misdirected report rather than a permitted cross-runner write.
	if reported := strings.TrimSpace(req.GetRunnerId()); reported != "" {
		reportedID, parseErr := parseUUID(reported)
		if parseErr != nil {
			return nil, status.Errorf(codes.InvalidArgument, "runner_id: %v", parseErr)
		}
		if reportedID != runner.Meta.ID {
			return nil, status.Error(codes.PermissionDenied, "runner_id does not match the reporting runner")
		}
	}

	workloadID, err := parseUUID(strings.TrimSpace(req.GetWorkloadId()))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "workload_id: %v", err)
	}
	workload, err := s.getWorkloadByID(ctx, workloadID)
	if err != nil {
		return nil, toStatusError(err)
	}
	// A runner speaks only for what runs on it.
	if workload.RunnerID != runner.Meta.ID {
		return nil, status.Error(codes.PermissionDenied, "workload does not belong to the reporting runner")
	}

	reportedStatus := req.GetStatus()
	if !isReportableStatus(reportedStatus) {
		return nil, status.Errorf(codes.InvalidArgument, "status %s is not an observed runtime state", reportedStatus)
	}

	if !reportSupersedes(req.GetObservedAt(), workload) {
		return &runnersv1.ReportWorkloadStateResponse{Applied: false}, nil
	}

	// Deliberately the same path UpdateWorkload takes, so the notifications and
	// the last_activity_at reset on starting -> running behave identically
	// whether the runner reported the transition or the Orchestrator found it.
	if _, err := s.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{
		Id:         workloadID.String(),
		Status:     &reportedStatus,
		Containers: req.GetContainers(),
	}); err != nil {
		return nil, err
	}
	return &runnersv1.ReportWorkloadStateResponse{Applied: true}, nil
}

func isReportableStatus(reported runnersv1.WorkloadStatus) bool {
	_, ok := reportableStatuses[reported]
	return ok
}

// reportSupersedes decides whether an observation still describes the workload.
//
// Retries and informer resyncs mean the same report arrives more than once, and
// sometimes after the platform has moved on. A report the record already
// predates is a view older than what is stored, and applying it would walk the
// status backwards -- back to a running that a stop has since superseded. A
// workload the platform has finished with is not something a runner reopens.
//
// Dropping a late-but-valid report costs latency and nothing else: the
// Orchestrator's reconciliation is still the backstop.
func reportSupersedes(observedAt *timestamppb.Timestamp, workload workloadRecord) bool {
	if workload.RemovedAt != nil {
		return false
	}
	if observedAt == nil {
		return true
	}
	return !observedAt.AsTime().Before(workload.Meta.UpdatedAt)
}
