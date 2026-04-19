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

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
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
	if resp.GetVolumes()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetVolumes()[0].GetOrganizationId())
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
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

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
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
	if resp.GetVolumes()[0].GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID.String(), resp.GetVolumes()[0].GetRunnerId())
	}
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
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

	var gotCheckReq *authorizationv1.CheckRequest
	authorizationClient := fakeAuthorizationClient{
		check: func(ctx context.Context, req *authorizationv1.CheckRequest) (*authorizationv1.CheckResponse, error) {
			gotCheckReq = req
			return &authorizationv1.CheckResponse{Allowed: true}, nil
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
	if gotCheckReq == nil {
		t.Fatal("expected authorization Check to be called")
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
