package server

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE organization_id = $1 ORDER BY id ASC LIMIT $2", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, 51).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	organizationIDValue := organizationID.String()
	resp, err := srv.ListVolumes(context.Background(), &runnersv1.ListVolumesRequest{OrganizationId: &organizationIDValue})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if resp.GetVolumes()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetVolumes()[0].GetOrganizationId())
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
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE runner_id = $1 ORDER BY id ASC LIMIT $2", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(runnerID, 51).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	runnerIDValue := runnerID.String()
	resp, err := srv.ListVolumes(context.Background(), &runnersv1.ListVolumesRequest{RunnerId: &runnerIDValue})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}
	if resp.GetVolumes()[0].GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID.String(), resp.GetVolumes()[0].GetRunnerId())
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
	now := time.Now().UTC()

	rows := pgxmock.NewRows(volumeRowColumns).
		AddRow(volumeID, nil, volumeResourceID, threadID, runnerID, agentID, organizationID, "10", volumeStatusActive, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM volumes WHERE (removed_at IS NULL OR last_metering_sampled_at IS NULL OR removed_at > last_metering_sampled_at) ORDER BY id ASC LIMIT $1", volumeColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(51).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	pendingSample := true
	resp, err := srv.ListVolumes(context.Background(), &runnersv1.ListVolumesRequest{PendingSample: &pendingSample})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(resp.GetVolumes()))
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListVolumesInvalidUUID(t *testing.T) {
	srv := New(Options{})

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
			_, err := srv.ListVolumes(context.Background(), testCase.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument error, got %v", err)
			}
		})
	}
}
