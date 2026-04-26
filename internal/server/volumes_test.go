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
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var volumeRowColumns = []string{
	"id",
	"instance_id",
	"volume_id",
	"thread_id",
	"runner_id",
	"agent_id",
	"organization_id",
	"size_gb",
	"status",
	"removed_at",
	"last_metering_sampled_at",
	"created_at",
	"updated_at",
}

func TestListVolumesFiltersOrganization(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE organization_id = $1 ORDER BY id ASC LIMIT $2", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, 51).
		WillReturnRows(rows)

	checkRelations := make([]string, 0, 2)
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			relation := req.GetTupleKey().GetRelation()
			checkRelations = append(checkRelations, relation)
			switch relation {
			case clusterAdminRelation:
				if req.GetTupleKey().GetObject() != clusterObject {
					t.Fatalf("expected cluster object %s, got %s", clusterObject, req.GetTupleKey().GetObject())
				}
				return &authorizationv1.CheckResponse{Allowed: false}, nil
			case organizationMemberRelation:
				if req.GetTupleKey().GetObject() != organizationObject(organizationID) {
					t.Fatalf("expected organization object %s, got %s", organizationObject(organizationID), req.GetTupleKey().GetObject())
				}
				return &authorizationv1.CheckResponse{Allowed: true}, nil
			default:
				t.Fatalf("unexpected relation %s", relation)
				return nil, status.Error(codes.Internal, "unexpected relation")
			}
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if resp.GetVolumes()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetVolumes()[0].GetOrganizationId())
	}
	if len(checkRelations) != 2 {
		t.Fatalf("expected 2 authorization checks, got %d", len(checkRelations))
	}
	if checkRelations[0] != clusterAdminRelation || checkRelations[1] != organizationMemberRelation {
		t.Fatalf("unexpected relation order %v", checkRelations)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesAllowsClusterAdminForOrganization(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE organization_id = $1 ORDER BY id ASC LIMIT $2", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, 51).
		WillReturnRows(rows)

	checkRelations := make([]string, 0, 1)
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			relation := req.GetTupleKey().GetRelation()
			checkRelations = append(checkRelations, relation)
			if relation != clusterAdminRelation {
				t.Fatalf("expected cluster admin relation, got %s", relation)
			}
			if req.GetTupleKey().GetObject() != clusterObject {
				t.Fatalf("expected cluster object %s, got %s", clusterObject, req.GetTupleKey().GetObject())
			}
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if len(checkRelations) != 1 || checkRelations[0] != clusterAdminRelation {
		t.Fatalf("expected cluster admin check, got %v", checkRelations)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesAllowsClusterAdminWithoutOrganization(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE runner_id = $1 ORDER BY id ASC LIMIT $2", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(runnerID, 51).
		WillReturnRows(rows)

	checkRelations := make([]string, 0, 1)
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			relation := req.GetTupleKey().GetRelation()
			checkRelations = append(checkRelations, relation)
			if relation != clusterAdminRelation {
				t.Fatalf("expected cluster admin relation, got %s", relation)
			}
			if req.GetTupleKey().GetObject() != clusterObject {
				t.Fatalf("expected cluster object %s, got %s", clusterObject, req.GetTupleKey().GetObject())
			}
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	runnerIDValue := runnerID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{RunnerId: &runnerIDValue})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if len(checkRelations) != 1 || checkRelations[0] != clusterAdminRelation {
		t.Fatalf("expected cluster admin check, got %v", checkRelations)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesFiltersRunner(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE runner_id = $1 ORDER BY id ASC LIMIT $2", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(runnerID, 51).
		WillReturnRows(rows)

	checkRelations := make([]string, 0, 2)
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			relation := req.GetTupleKey().GetRelation()
			checkRelations = append(checkRelations, relation)
			switch relation {
			case clusterAdminRelation:
				if req.GetTupleKey().GetObject() != clusterObject {
					t.Fatalf("expected cluster object %s, got %s", clusterObject, req.GetTupleKey().GetObject())
				}
				return &authorizationv1.CheckResponse{Allowed: false}, nil
			case organizationMemberRelation:
				if req.GetTupleKey().GetObject() != organizationObject(organizationID) {
					t.Fatalf("expected organization object %s, got %s", organizationObject(organizationID), req.GetTupleKey().GetObject())
				}
				return &authorizationv1.CheckResponse{Allowed: true}, nil
			default:
				t.Fatalf("unexpected relation %s", relation)
				return nil, status.Error(codes.Internal, "unexpected relation")
			}
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	runnerIDValue := runnerID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{RunnerId: &runnerIDValue})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if resp.GetVolumes()[0].GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID.String(), resp.GetVolumes()[0].GetRunnerId())
	}
	if len(checkRelations) != 2 {
		t.Fatalf("expected 2 authorization checks, got %d", len(checkRelations))
	}
	if checkRelations[0] != clusterAdminRelation || checkRelations[1] != organizationMemberRelation {
		t.Fatalf("unexpected relation order %v", checkRelations)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesByThreadAllowsClusterAdmin(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE thread_id = $1 ORDER BY id ASC LIMIT $2", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(threadID, 51).
		WillReturnRows(rows)

	checkRelations := make([]string, 0, 1)
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			relation := req.GetTupleKey().GetRelation()
			checkRelations = append(checkRelations, relation)
			if relation != clusterAdminRelation {
				t.Fatalf("expected cluster admin relation, got %s", relation)
			}
			if req.GetTupleKey().GetObject() != clusterObject {
				t.Fatalf("expected cluster object %s, got %s", clusterObject, req.GetTupleKey().GetObject())
			}
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumesByThread(ctx, &runnersv1.ListVolumesByThreadRequest{ThreadId: threadID.String()})
	if err != nil {
		t.Fatalf("ListVolumesByThread failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if len(checkRelations) != 1 || checkRelations[0] != clusterAdminRelation {
		t.Fatalf("expected cluster admin check, got %v", checkRelations)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesPendingSample(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE %s ORDER BY id ASC LIMIT $1", volumeColumns, pendingSampleClause)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(51).
		WillReturnRows(rows)

	checkRelations := make([]string, 0, 2)
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			relation := req.GetTupleKey().GetRelation()
			checkRelations = append(checkRelations, relation)
			switch relation {
			case clusterAdminRelation:
				if req.GetTupleKey().GetObject() != clusterObject {
					t.Fatalf("expected cluster object %s, got %s", clusterObject, req.GetTupleKey().GetObject())
				}
				return &authorizationv1.CheckResponse{Allowed: false}, nil
			case organizationMemberRelation:
				if req.GetTupleKey().GetObject() != organizationObject(organizationID) {
					t.Fatalf("expected organization object %s, got %s", organizationObject(organizationID), req.GetTupleKey().GetObject())
				}
				return &authorizationv1.CheckResponse{Allowed: true}, nil
			default:
				t.Fatalf("unexpected relation %s", relation)
				return nil, status.Error(codes.Internal, "unexpected relation")
			}
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	pendingSample := true
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	resp, err := srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{PendingSample: &pendingSample})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if len(checkRelations) != 2 {
		t.Fatalf("expected 2 authorization checks, got %d", len(checkRelations))
	}
	if checkRelations[0] != clusterAdminRelation || checkRelations[1] != organizationMemberRelation {
		t.Fatalf("unexpected relation order %v", checkRelations)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesInvalidUUID(t *testing.T) {
	srv := New(Options{})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, uuid.NewString()))

	cases := []struct {
		name string
		req  *runnersv1.ListVolumesRequest
	}{
		{
			name: "organization_id",
			req: func() *runnersv1.ListVolumesRequest {
				value := "not-a-uuid"
				return &runnersv1.ListVolumesRequest{OrganizationId: &value}
			}(),
		},
		{
			name: "runner_id",
			req: func() *runnersv1.ListVolumesRequest {
				value := "not-a-uuid"
				return &runnersv1.ListVolumesRequest{RunnerId: &value}
			}(),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := srv.ListVolumes(ctx, testCase.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument error, got %v", err)
			}
		})
	}
}

func TestListVolumesRequiresMember(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	organizationID := uuid.New()
	callerID := uuid.New()

	checkRelations := make([]string, 0, 2)
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			relation := req.GetTupleKey().GetRelation()
			checkRelations = append(checkRelations, relation)
			switch relation {
			case clusterAdminRelation:
				return &authorizationv1.CheckResponse{Allowed: false}, nil
			case organizationMemberRelation:
				return &authorizationv1.CheckResponse{Allowed: false}, nil
			default:
				t.Fatalf("unexpected relation %s", relation)
				return nil, status.Error(codes.Internal, "unexpected relation")
			}
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	organizationIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.ListVolumes(ctx, &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}
	if len(checkRelations) != 2 {
		t.Fatalf("expected 2 authorization checks, got %d", len(checkRelations))
	}
	if checkRelations[0] != clusterAdminRelation || checkRelations[1] != organizationMemberRelation {
		t.Fatalf("unexpected relation order %v", checkRelations)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetVolumeRequiresMember(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	callerID := uuid.New()
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE id = $1", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(volumeID).WillReturnRows(rows)

	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(Options{Pool: mockPool, AuthorizationClient: authorizationClient})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))
	_, err = srv.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: volumeID.String()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateVolume(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	volumeID := uuid.New()
	volumeResourceID := uuid.New()
	threadID := uuid.New()
	runnerID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	instanceID := "instance-1"
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, instanceID, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("UPDATE volumes SET status = $1, instance_id = $2, updated_at = NOW() WHERE id = $3 RETURNING %s", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(volumeStatusActive, instanceID, volumeID).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.UpdateVolume(context.Background(), &runnersv1.UpdateVolumeRequest{
		Id:         volumeID.String(),
		Status:     runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE.Enum(),
		InstanceId: &instanceID,
	})
	if err != nil {
		t.Fatalf("UpdateVolume failed: %v", err)
	}
	if resp.GetVolume().GetInstanceId() != instanceID {
		t.Fatalf("expected instance id %q, got %q", instanceID, resp.GetVolume().GetInstanceId())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateVolumeRequiresFields(t *testing.T) {
	srv := New(Options{})

	_, err := srv.UpdateVolume(context.Background(), &runnersv1.UpdateVolumeRequest{Id: uuid.NewString()})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

func TestBatchUpdateVolumeSampledAt(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	firstID := uuid.New()
	secondID := uuid.New()
	firstSampledAt := time.Now().UTC()
	secondSampledAt := firstSampledAt.Add(3 * time.Minute)

	query := "UPDATE volumes AS target SET last_metering_sampled_at = v.sampled_at, updated_at = NOW() FROM (VALUES ($1::uuid, $2::timestamptz), ($3::uuid, $4::timestamptz)) AS v(id, sampled_at) WHERE target.id = v.id"
	mockPool.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(firstID, firstSampledAt, secondID, secondSampledAt).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))

	srv := New(Options{Pool: mockPool})
	_, err = srv.BatchUpdateVolumeSampledAt(context.Background(), &runnersv1.BatchUpdateVolumeSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{
			{Id: firstID.String(), SampledAt: timestamppb.New(firstSampledAt)},
			{Id: secondID.String(), SampledAt: timestamppb.New(secondSampledAt)},
		},
	})
	if err != nil {
		t.Fatalf("BatchUpdateVolumeSampledAt failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBatchUpdateVolumeSampledAtInvalid(t *testing.T) {
	srv := New(Options{})

	_, err := srv.BatchUpdateVolumeSampledAt(context.Background(), &runnersv1.BatchUpdateVolumeSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{{Id: "not-a-uuid", SampledAt: timestamppb.Now()}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}
