package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/runners/.gen/go/agynio/api/ziti_management/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeZitiManagementClient struct {
	createRunnerIdentity func(ctx context.Context, req *zitimanagementv1.CreateRunnerIdentityRequest) (*zitimanagementv1.CreateRunnerIdentityResponse, error)
}

func (f fakeZitiManagementClient) CreateAgentIdentity(ctx context.Context, req *zitimanagementv1.CreateAgentIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateAppIdentity(ctx context.Context, req *zitimanagementv1.CreateAppIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteIdentity(ctx context.Context, req *zitimanagementv1.DeleteIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) DeleteAppIdentity(ctx context.Context, req *zitimanagementv1.DeleteAppIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateRunnerIdentity(ctx context.Context, req *zitimanagementv1.CreateRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateRunnerIdentityResponse, error) {
	if f.createRunnerIdentity == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.createRunnerIdentity(ctx, req)
}

func (f fakeZitiManagementClient) DeleteRunnerIdentity(ctx context.Context, req *zitimanagementv1.DeleteRunnerIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.DeleteRunnerIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ListManagedIdentities(ctx context.Context, req *zitimanagementv1.ListManagedIdentitiesRequest, opts ...grpc.CallOption) (*zitimanagementv1.ListManagedIdentitiesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ResolveIdentity(ctx context.Context, req *zitimanagementv1.ResolveIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.ResolveIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) RequestServiceIdentity(ctx context.Context, req *zitimanagementv1.RequestServiceIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.RequestServiceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) ExtendIdentityLease(ctx context.Context, req *zitimanagementv1.ExtendIdentityLeaseRequest, opts ...grpc.CallOption) (*zitimanagementv1.ExtendIdentityLeaseResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func TestEnrollRunnerRequiresToken(t *testing.T) {
	srv := New(Options{})

	_, err := srv.EnrollRunner(context.Background(), &runnersv1.EnrollRunnerRequest{ServiceToken: "   "})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

func TestEnrollRunnerNotFound(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	token := "missing-token"
	query := fmt.Sprintf(`SELECT %s FROM runners WHERE service_token_hash = $1`, runnerColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(hashServiceToken(token)).WillReturnError(pgx.ErrNoRows)

	srv := New(Options{
		Pool:                 mockPool,
		ZitiManagementClient: fakeZitiManagementClient{},
	})

	_, err = srv.EnrollRunner(context.Background(), &runnersv1.EnrollRunnerRequest{ServiceToken: token})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestEnrollRunnerSuccess(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	matcher := regexp.QuoteMeta(fmt.Sprintf(`SELECT %s FROM runners WHERE service_token_hash = $1`, runnerColumns))
	updateQuery := regexp.QuoteMeta(`UPDATE runners SET status = $1, updated_at = NOW() WHERE id = $2`)

	runnerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "status", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Valid: false}, identityID, runnerStatusPending, now, now)

	token := "enroll-token"
	mockPool.ExpectQuery(matcher).WithArgs(hashServiceToken(token)).WillReturnRows(rows)
	mockPool.ExpectExec(updateQuery).WithArgs(runnerStatusEnrolled, runnerID).WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	identityJSON := []byte("identity-json")
	serviceName := "runner-service"
	var gotReq *zitimanagementv1.CreateRunnerIdentityRequest
	fakeClient := fakeZitiManagementClient{
		createRunnerIdentity: func(ctx context.Context, req *zitimanagementv1.CreateRunnerIdentityRequest) (*zitimanagementv1.CreateRunnerIdentityResponse, error) {
			gotReq = req
			return &zitimanagementv1.CreateRunnerIdentityResponse{
				IdentityJson:    identityJSON,
				ZitiServiceName: serviceName,
				ZitiIdentityId:  "ziti-id",
				ZitiServiceId:   "service-id",
			}, nil
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		ZitiManagementClient: fakeClient,
	})

	resp, err := srv.EnrollRunner(context.Background(), &runnersv1.EnrollRunnerRequest{ServiceToken: token})
	if err != nil {
		t.Fatalf("EnrollRunner failed: %v", err)
	}
	if resp.GetServiceName() != serviceName {
		t.Fatalf("expected service name %q, got %q", serviceName, resp.GetServiceName())
	}
	if string(resp.GetIdentityJson()) != string(identityJSON) {
		t.Fatalf("expected identity json %q, got %q", identityJSON, resp.GetIdentityJson())
	}
	if resp.GetIdentityId() != identityID.String() {
		t.Fatalf("expected identity id %q, got %q", identityID.String(), resp.GetIdentityId())
	}

	if gotReq == nil {
		t.Fatal("expected CreateRunnerIdentity to be called")
	}
	if gotReq.GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID.String(), gotReq.GetRunnerId())
	}
	if len(gotReq.GetRoleAttributes()) != 1 || gotReq.GetRoleAttributes()[0] != "runners" {
		t.Fatalf("unexpected role attributes: %v", gotReq.GetRoleAttributes())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestEnrollRunnerZitiFailure(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	matcher := regexp.QuoteMeta(fmt.Sprintf(`SELECT %s FROM runners WHERE service_token_hash = $1`, runnerColumns))
	runnerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "status", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Valid: false}, identityID, runnerStatusPending, now, now)

	token := "bad-ziti"
	mockPool.ExpectQuery(matcher).WithArgs(hashServiceToken(token)).WillReturnRows(rows)

	fakeClient := fakeZitiManagementClient{
		createRunnerIdentity: func(ctx context.Context, req *zitimanagementv1.CreateRunnerIdentityRequest) (*zitimanagementv1.CreateRunnerIdentityResponse, error) {
			return nil, errors.New("ziti failure")
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		ZitiManagementClient: fakeClient,
	})

	_, err = srv.EnrollRunner(context.Background(), &runnersv1.EnrollRunnerRequest{ServiceToken: token})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
