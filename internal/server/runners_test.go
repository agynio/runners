package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"testing"
	"time"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	identityv1 "github.com/agynio/runners/.gen/go/agynio/api/identity/v1"
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
	createService        func(ctx context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error)
}

func (f fakeZitiManagementClient) CreateAgentIdentity(ctx context.Context, req *zitimanagementv1.CreateAgentIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateAppIdentity(ctx context.Context, req *zitimanagementv1.CreateAppIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateAppIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateService(ctx context.Context, req *zitimanagementv1.CreateServiceRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateServiceResponse, error) {
	if f.createService == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.createService(ctx, req)
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

type fakeIdentityClient struct {
	registerIdentity func(ctx context.Context, req *identityv1.RegisterIdentityRequest) (*identityv1.RegisterIdentityResponse, error)
}

func (f fakeIdentityClient) RegisterIdentity(ctx context.Context, req *identityv1.RegisterIdentityRequest, opts ...grpc.CallOption) (*identityv1.RegisterIdentityResponse, error) {
	if f.registerIdentity == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.registerIdentity(ctx, req)
}

func (f fakeIdentityClient) GetIdentityType(ctx context.Context, req *identityv1.GetIdentityTypeRequest, opts ...grpc.CallOption) (*identityv1.GetIdentityTypeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeIdentityClient) BatchGetIdentityTypes(ctx context.Context, req *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

type fakeAuthorizationClient struct {
	write func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error)
}

func (f fakeAuthorizationClient) Check(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeAuthorizationClient) BatchCheck(ctx context.Context, req *authorizationv1.BatchCheckRequest, opts ...grpc.CallOption) (*authorizationv1.BatchCheckResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeAuthorizationClient) Write(ctx context.Context, req *authorizationv1.WriteRequest, opts ...grpc.CallOption) (*authorizationv1.WriteResponse, error) {
	if f.write == nil {
		return nil, status.Error(codes.Unimplemented, "not implemented")
	}
	return f.write(ctx, req)
}

func (f fakeAuthorizationClient) Read(ctx context.Context, req *authorizationv1.ReadRequest, opts ...grpc.CallOption) (*authorizationv1.ReadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeAuthorizationClient) ListObjects(ctx context.Context, req *authorizationv1.ListObjectsRequest, opts ...grpc.CallOption) (*authorizationv1.ListObjectsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeAuthorizationClient) ListUsers(ctx context.Context, req *authorizationv1.ListUsersRequest, opts ...grpc.CallOption) (*authorizationv1.ListUsersResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func TestRegisterRunnerPersistsLabels(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	labels := map[string]string{"type": "gpu", "region": "us-east-1"}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		t.Fatalf("failed to marshal labels: %v", err)
	}

	runnerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	zitiServiceID := "service-id"
	zitiServiceName := "runner-service"
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Valid: false}, identityID, "", zitiServiceID, zitiServiceName, runnerStatusPending, labelsJSON, now, now)

	matcher := regexp.QuoteMeta(fmt.Sprintf("INSERT INTO runners (id, name, organization_id, identity_id, ziti_identity_id, ziti_service_id, ziti_service_name, service_token_hash, status, labels)\n\t    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)\n\t    RETURNING %s", runnerColumns))
	mockPool.ExpectQuery(matcher).
		WithArgs(pgxmock.AnyArg(), "runner-1", pgxmock.AnyArg(), pgxmock.AnyArg(), "", zitiServiceID, zitiServiceName, pgxmock.AnyArg(), runnerStatusPending, labelsJSON).
		WillReturnRows(rows)

	var gotIdentityReq *identityv1.RegisterIdentityRequest
	identityClient := fakeIdentityClient{
		registerIdentity: func(ctx context.Context, req *identityv1.RegisterIdentityRequest) (*identityv1.RegisterIdentityResponse, error) {
			gotIdentityReq = req
			return &identityv1.RegisterIdentityResponse{}, nil
		},
	}
	var gotWriteReq *authorizationv1.WriteRequest
	authorizationClient := fakeAuthorizationClient{
		write: func(ctx context.Context, req *authorizationv1.WriteRequest) (*authorizationv1.WriteResponse, error) {
			gotWriteReq = req
			return &authorizationv1.WriteResponse{}, nil
		},
	}
	var gotServiceReq *zitimanagementv1.CreateServiceRequest
	zitiClient := fakeZitiManagementClient{
		createService: func(ctx context.Context, req *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
			gotServiceReq = req
			return &zitimanagementv1.CreateServiceResponse{
				ZitiServiceId:   zitiServiceID,
				ZitiServiceName: zitiServiceName,
			}, nil
		},
	}

	srv := New(Options{
		Pool:                 mockPool,
		IdentityClient:       identityClient,
		AuthorizationClient:  authorizationClient,
		ZitiManagementClient: zitiClient,
	})

	resp, err := srv.RegisterRunner(context.Background(), &runnersv1.RegisterRunnerRequest{
		Name:   "runner-1",
		Labels: labels,
	})
	if err != nil {
		t.Fatalf("RegisterRunner failed: %v", err)
	}
	if resp.GetRunner() == nil {
		t.Fatal("expected runner in response")
	}
	if resp.GetRunner().GetName() != "runner-1" {
		t.Fatalf("expected runner name %q, got %q", "runner-1", resp.GetRunner().GetName())
	}
	if !maps.Equal(resp.GetRunner().GetLabels(), labels) {
		t.Fatalf("expected labels %v, got %v", labels, resp.GetRunner().GetLabels())
	}
	if resp.GetServiceToken() == "" {
		t.Fatal("expected service token")
	}
	if gotIdentityReq == nil {
		t.Fatal("expected RegisterIdentity to be called")
	}
	if gotWriteReq == nil {
		t.Fatal("expected authorization Write to be called")
	}
	if gotServiceReq == nil {
		t.Fatal("expected CreateService to be called")
	}
	if gotServiceReq.GetName() != fmt.Sprintf("runner-%s", gotIdentityReq.GetIdentityId()) {
		t.Fatalf("expected service name %q, got %q", fmt.Sprintf("runner-%s", gotIdentityReq.GetIdentityId()), gotServiceReq.GetName())
	}
	if len(gotServiceReq.GetRoleAttributes()) != 1 || gotServiceReq.GetRoleAttributes()[0] != zitiRunnerServiceRole {
		t.Fatalf("unexpected role attributes: %v", gotServiceReq.GetRoleAttributes())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateRunnerUpdatesLabels(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	runnerID := uuid.New()
	identityID := uuid.New()
	labels := map[string]string{"env": "prod", "type": "cpu"}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		t.Fatalf("failed to marshal labels: %v", err)
	}
	now := time.Now().UTC()
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-updated", pgtype.UUID{Valid: false}, identityID, "", "service-id", "runner-service", runnerStatusOffline, labelsJSON, now, now)

	matcher := regexp.QuoteMeta(fmt.Sprintf(`UPDATE runners SET name = COALESCE($1, name), labels = COALESCE($2::jsonb, labels), updated_at = NOW() WHERE id = $3 RETURNING %s`, runnerColumns))
	mockPool.ExpectQuery(matcher).
		WithArgs(pgtype.Text{String: "runner-updated", Valid: true}, pgtype.Text{String: string(labelsJSON), Valid: true}, runnerID).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	name := "runner-updated"
	resp, err := srv.UpdateRunner(context.Background(), &runnersv1.UpdateRunnerRequest{
		Id:     runnerID.String(),
		Name:   &name,
		Labels: labels,
	})
	if err != nil {
		t.Fatalf("UpdateRunner failed: %v", err)
	}
	if resp.GetRunner() == nil {
		t.Fatal("expected runner in response")
	}
	if resp.GetRunner().GetName() != name {
		t.Fatalf("expected runner name %q, got %q", name, resp.GetRunner().GetName())
	}
	if !maps.Equal(resp.GetRunner().GetLabels(), labels) {
		t.Fatalf("expected labels %v, got %v", labels, resp.GetRunner().GetLabels())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
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
	updateQuery := regexp.QuoteMeta(`UPDATE runners SET status = $1, ziti_identity_id = $2, updated_at = NOW() WHERE id = $3`)

	runnerID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	labelsJSON := []byte("{}")
	serviceName := "runner-service"
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Valid: false}, identityID, "", "service-id", serviceName, runnerStatusPending, labelsJSON, now, now)

	token := "enroll-token"
	mockPool.ExpectQuery(matcher).WithArgs(hashServiceToken(token)).WillReturnRows(rows)
	mockPool.ExpectExec(updateQuery).WithArgs(runnerStatusEnrolled, "ziti-id", runnerID).WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	identityJSON := []byte("identity-json")
	var gotReq *zitimanagementv1.CreateRunnerIdentityRequest
	fakeClient := fakeZitiManagementClient{
		createRunnerIdentity: func(ctx context.Context, req *zitimanagementv1.CreateRunnerIdentityRequest) (*zitimanagementv1.CreateRunnerIdentityResponse, error) {
			gotReq = req
			return &zitimanagementv1.CreateRunnerIdentityResponse{
				IdentityJson:   identityJSON,
				ZitiIdentityId: "ziti-id",
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
	if resp.GetIdentityId() != "ziti-id" {
		t.Fatalf("expected identity id %q, got %q", "ziti-id", resp.GetIdentityId())
	}

	if gotReq == nil {
		t.Fatal("expected CreateRunnerIdentity to be called")
	}
	if gotReq.GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID.String(), gotReq.GetRunnerId())
	}
	if len(gotReq.GetRoleAttributes()) != 1 || gotReq.GetRoleAttributes()[0] != zitiRunnerRoleAttribute {
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
	labelsJSON := []byte("{}")
	rows := pgxmock.NewRows([]string{"id", "name", "organization_id", "identity_id", "ziti_identity_id", "ziti_service_id", "ziti_service_name", "status", "labels", "created_at", "updated_at"}).
		AddRow(runnerID, "runner-1", pgtype.UUID{Valid: false}, identityID, "", "service-id", "runner-service", runnerStatusPending, labelsJSON, now, now)

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
