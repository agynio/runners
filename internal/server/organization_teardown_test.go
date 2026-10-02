package server

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeleteOrganizationResourcesRemovesTuplesThenRows(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	organizationID := uuid.New()
	workloadID, agentID, ownerID := uuid.New(), uuid.New(), uuid.New()
	runnerID, identityID := uuid.New(), uuid.New()
	now := time.Now().UTC()

	mockPool.ExpectQuery(regexp.QuoteMeta(`SELECT id, agent_id, owner_id FROM workloads WHERE organization_id = $1`)).
		WithArgs(organizationID).
		WillReturnRows(pgxmock.NewRows([]string{"id", "agent_id", "owner_id"}).AddRow(workloadID, agentID, ownerID))
	mockPool.ExpectExec(regexp.QuoteMeta(`DELETE FROM workloads WHERE organization_id = $1`)).
		WithArgs(organizationID).WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mockPool.ExpectExec(regexp.QuoteMeta(`DELETE FROM volumes WHERE organization_id = $1`)).
		WithArgs(organizationID).WillReturnResult(pgxmock.NewResult("DELETE", 2))
	mockPool.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(`SELECT %s FROM runners WHERE organization_id = $1`, runnerColumns))).
		WithArgs(organizationID).
		WillReturnRows(pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}).
			AddRow(runnerID, "runner-1", pgtype.UUID{Bytes: organizationID, Valid: true}, identityID, "", "", "", runnerStatusOffline, []byte("{}"), []byte("[]"), now, now))
	mockPool.ExpectExec(regexp.QuoteMeta(`DELETE FROM runners WHERE id = $1`)).
		WithArgs(runnerID).WillReturnResult(pgxmock.NewResult("DELETE", 1))

	var writes []*authorizationv1.WriteRequest
	authorizationClient := fakeAuthorizationClient{
		write: func(_ context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			writes = append(writes, req)
			return &authorizationv1.WriteResponse{}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient, ZitiManagementClient: fakeZitiManagementClient{}})

	// Internal RPC: no identity in the context, and none required.
	_, err = srv.DeleteOrganizationResources(context.Background(), &runnersv1.DeleteOrganizationResourcesRequest{
		OrganizationId: organizationID.String(),
	})
	if err != nil {
		t.Fatalf("DeleteOrganizationResources failed: %v", err)
	}

	// One workload contributes an org tuple plus one per distinct viewer.
	if len(writes) == 0 || len(writes[0].GetDeletes()) != 3 {
		t.Fatalf("expected 3 workload tuple deletes, got %v", writes)
	}
	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteOrganizationResourcesOnEmptyOrganization(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}
	organizationID := uuid.New()

	// The cascade retries a step it is unsure finished, so an organization with
	// nothing left has to succeed rather than fail.
	mockPool.ExpectQuery(regexp.QuoteMeta(`SELECT id, agent_id, owner_id FROM workloads WHERE organization_id = $1`)).
		WithArgs(organizationID).WillReturnRows(pgxmock.NewRows([]string{"id", "agent_id", "owner_id"}))
	mockPool.ExpectExec(regexp.QuoteMeta(`DELETE FROM workloads WHERE organization_id = $1`)).
		WithArgs(organizationID).WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mockPool.ExpectExec(regexp.QuoteMeta(`DELETE FROM volumes WHERE organization_id = $1`)).
		WithArgs(organizationID).WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mockPool.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(`SELECT %s FROM runners WHERE organization_id = $1`, runnerColumns))).
		WithArgs(organizationID).
		WillReturnRows(pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "capabilities", "created_at", "updated_at"}))

	srv := New(Options{Pool: mockPool, AuthorizationClient: fakeAuthorizationClient{}, ZitiManagementClient: fakeZitiManagementClient{}})
	if _, err := srv.DeleteOrganizationResources(context.Background(), &runnersv1.DeleteOrganizationResourcesRequest{
		OrganizationId: organizationID.String(),
	}); err != nil {
		t.Fatalf("DeleteOrganizationResources failed: %v", err)
	}
	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteOrganizationResourcesRejectsInvalidOrganizationID(t *testing.T) {
	srv := New(Options{})
	_, err := srv.DeleteOrganizationResources(context.Background(), &runnersv1.DeleteOrganizationResourcesRequest{
		OrganizationId: "not-a-uuid",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}
