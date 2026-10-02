package server

import (
	"context"
	"fmt"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// tupleDeleteBatchSize is OpenFGA's per-Write limit. An organization has more
// workloads than that over its life, so the deletes go out in batches rather
// than one call that would be rejected whole.
const tupleDeleteBatchSize = 100

// DeleteOrganizationResources removes the organization's workload records,
// provisioned volume records, and org-scoped runners. It is internal: Istio
// settles who may call it, so there is no permission check and no caller
// identity to check against. Step 2 of the organization teardown.
//
// Removing the records is what makes the running pods and disks orphans, which
// the Orchestrator's existing reconciliation then stops and deprovisions. No
// stop-everything path is added for a case reconciliation already covers.
//
// The rows are deleted outright rather than marked removed_at the way
// DeleteWorkload does. A soft delete keeps the row for metering to sample; the
// organization it is scoped by is going away, and a row holding an
// organization_id that resolves to nothing is what the teardown is for.
//
// Idempotent by construction: a retried step lists nothing and deletes nothing.
func (s *Server) DeleteOrganizationResources(ctx context.Context, req *runnersv1.DeleteOrganizationResourcesRequest) (*runnersv1.DeleteOrganizationResourcesResponse, error) {
	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}

	if err := s.deleteOrganizationWorkloads(ctx, organizationID); err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM volumes WHERE organization_id = $1`, organizationID); err != nil {
		return nil, status.Errorf(codes.Internal, "delete organization volumes: %v", err)
	}

	runners, err := s.listRunnersByOrganization(ctx, organizationID)
	if err != nil {
		return nil, toStatusError(err)
	}
	for _, runner := range runners {
		if err := s.deleteRunnerRecord(ctx, runner); err != nil {
			return nil, toStatusError(err)
		}
	}
	return &runnersv1.DeleteOrganizationResourcesResponse{}, nil
}

// deleteOrganizationWorkloads takes the authorization tuples off before the
// rows: a step interrupted after the rows are gone would have nothing left to
// enumerate the tuples from, and they would outlive the organization
// unreachable.
func (s *Server) deleteOrganizationWorkloads(ctx context.Context, organizationID uuid.UUID) error {
	rows, err := s.pool.Query(ctx,
		`SELECT id, agent_id, owner_id FROM workloads WHERE organization_id = $1`, organizationID)
	if err != nil {
		return status.Errorf(codes.Internal, "list organization workloads: %v", err)
	}
	defer rows.Close()

	deletes := []*authorizationv1.TupleKey{}
	for rows.Next() {
		var workloadID, agentID, ownerID uuid.UUID
		if err := rows.Scan(&workloadID, &agentID, &ownerID); err != nil {
			return status.Errorf(codes.Internal, "scan organization workload: %v", err)
		}
		deletes = append(deletes, workloadAuthorizationTuples(workloadID, organizationID, &agentID, &ownerID)...)
	}
	if err := rows.Err(); err != nil {
		return status.Errorf(codes.Internal, "list organization workloads: %v", err)
	}
	rows.Close()

	for start := 0; start < len(deletes); start += tupleDeleteBatchSize {
		end := min(start+tupleDeleteBatchSize, len(deletes))
		if _, err := s.authorizationClient.Write(ctx, &authorizationv1.WriteRequest{Deletes: deletes[start:end]}); err != nil {
			return status.Errorf(codes.Internal, "authorization write: %v", err)
		}
	}

	if _, err := s.pool.Exec(ctx, `DELETE FROM workloads WHERE organization_id = $1`, organizationID); err != nil {
		return status.Errorf(codes.Internal, "delete organization workloads: %v", err)
	}
	return nil
}

func (s *Server) listRunnersByOrganization(ctx context.Context, organizationID uuid.UUID) ([]runnerRecord, error) {
	rows, err := s.pool.Query(ctx,
		fmt.Sprintf(`SELECT %s FROM runners WHERE organization_id = $1`, runnerColumns), organizationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	runners := []runnerRecord{}
	for rows.Next() {
		runner, err := scanRunner(rows)
		if err != nil {
			return nil, err
		}
		runners = append(runners, runner)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return runners, nil
}
