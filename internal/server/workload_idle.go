package server

import (
	"context"
	"errors"
	"fmt"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReportWorkloadIdle records that the agent in a workload has finished a turn.
//
// The activity sweep would make the same processing -> idle transition once the
// keepalive grace had run out; this makes it the moment it is true, which is
// the moment the Orchestrator may put the workload to sleep. It publishes the
// event the sweep and TouchWorkload publish, and changes nothing else:
// last_activity_at stays with the touches, so the idle clock runs from the last
// keepalive as it always did.
//
// A workload that is not running has no turn to end, and one already idle has
// nothing left to record. Both are left alone and answered the same way, so a
// retried report does not become a second event.
func (s *Server) ReportWorkloadIdle(ctx context.Context, req *runnersv1.ReportWorkloadIdleRequest) (*runnersv1.ReportWorkloadIdleResponse, error) {
	callerID, err := identityFromMetadataOptional(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	workload, err := s.getWorkloadByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	// As TouchWorkload: the workload's own identity. Only an agent instance has
	// a turn to end -- a sandbox runs no agent, and its agent_state is not a
	// signal -- so there is no internal caller for this one either.
	if workload.OwnerKind != runtimeOwnerKindAgentInstance || callerID == nil || *callerID != workload.OwnerID {
		return nil, toStatusError(PermissionDenied())
	}
	idle, err := s.setWorkloadIdle(ctx, workload.Meta.ID, workload.OwnerID)
	if err != nil {
		return nil, toStatusError(err)
	}
	if idle != nil {
		s.publishWorkloadUpdateNotifications(ctx, *idle, false, false, false, true)
	}
	return &runnersv1.ReportWorkloadIdleResponse{}, nil
}

// setWorkloadIdle flips a running, processing agent workload to idle. It
// returns nil when there was nothing to flip.
func (s *Server) setWorkloadIdle(ctx context.Context, id uuid.UUID, ownerID uuid.UUID) (*workloadRecord, error) {
	query := fmt.Sprintf("UPDATE workloads SET agent_state = $1, updated_at = NOW() WHERE id = $2 AND owner_kind = $3 AND owner_id = $4 AND status = $5 AND agent_state = $6 AND removed_at IS NULL RETURNING %s", workloadColumns)
	row := s.pool.QueryRow(ctx, query, workloadAgentStateIdle, id, runtimeOwnerKindAgentInstance, ownerID, workloadStatusRunning, workloadAgentStateProcessing)
	workload, err := scanWorkload(row)
	if err == nil {
		return &workload, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return nil, err
}
