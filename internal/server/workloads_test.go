package server

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var workloadRowColumns = []string{
	"id",
	"runner_id",
	"thread_id",
	"agent_id",
	"organization_id",
	"status",
	"containers",
	"ziti_identity_id",
	"allocated_cpu_millicores",
	"allocated_ram_bytes",
	"instance_id",
	"last_activity_at",
	"last_metering_sampled_at",
	"removed_at",
	"created_at",
	"updated_at",
}

func TestListWorkloadsFiltersOrganization(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, containersJSON, "ziti-id", int32(0), int64(0), nil, now, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE organization_id = $1 ORDER BY id ASC LIMIT $2", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(organizationID, 51).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	organizationIDValue := organizationID.String()
	resp, err := srv.ListWorkloads(context.Background(), &runnersv1.ListWorkloadsRequest{OrganizationId: &organizationIDValue})
	if err != nil {
		t.Fatalf("ListWorkloads failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	if resp.GetWorkloads()[0].GetOrganizationId() != organizationID.String() {
		t.Fatalf("expected organization id %q, got %q", organizationID.String(), resp.GetWorkloads()[0].GetOrganizationId())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsFiltersRunner(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, containersJSON, "ziti-id", int32(0), int64(0), nil, now, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE runner_id = $1 ORDER BY id ASC LIMIT $2", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(runnerID, 51).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	runnerIDValue := runnerID.String()
	resp, err := srv.ListWorkloads(context.Background(), &runnersv1.ListWorkloadsRequest{RunnerId: &runnerIDValue})
	if err != nil {
		t.Fatalf("ListWorkloads failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}
	if resp.GetWorkloads()[0].GetRunnerId() != runnerID.String() {
		t.Fatalf("expected runner id %q, got %q", runnerID.String(), resp.GetWorkloads()[0].GetRunnerId())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsPendingSample(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, containersJSON, "ziti-id", int32(0), int64(0), nil, now, nil, nil, now, now)

	query := fmt.Sprintf("SELECT %s FROM workloads WHERE %s ORDER BY id ASC LIMIT $1", workloadColumns, pendingSampleClause)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(51).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	pendingSample := true
	resp, err := srv.ListWorkloads(context.Background(), &runnersv1.ListWorkloadsRequest{PendingSample: &pendingSample})
	if err != nil {
		t.Fatalf("ListWorkloads failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(resp.GetWorkloads()))
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListWorkloadsInvalidUUID(t *testing.T) {
	srv := New(Options{})

	cases := []struct {
		name string
		req  *runnersv1.ListWorkloadsRequest
	}{
		{
			name: "organization_id",
			req: func() *runnersv1.ListWorkloadsRequest {
				value := "not-a-uuid"
				return &runnersv1.ListWorkloadsRequest{OrganizationId: &value}
			}(),
		},
		{
			name: "runner_id",
			req: func() *runnersv1.ListWorkloadsRequest {
				value := "not-a-uuid"
				return &runnersv1.ListWorkloadsRequest{RunnerId: &value}
			}(),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := srv.ListWorkloads(context.Background(), testCase.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument error, got %v", err)
			}
		})
	}
}

func TestTouchWorkload(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	query := "UPDATE workloads SET last_activity_at = NOW(), updated_at = NOW() WHERE id = $1"
	mockPool.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(workloadID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	srv := New(Options{Pool: mockPool})
	_, err = srv.TouchWorkload(context.Background(), &runnersv1.TouchWorkloadRequest{Id: workloadID.String()})
	if err != nil {
		t.Fatalf("TouchWorkload failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateWorkload(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	instanceID := "instance-1"
	now := time.Now().UTC()
	containers := []*runnersv1.Container{{
		ContainerId: "container-1",
		Name:        "name",
		Role:        runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
		Image:       "image",
		Status:      runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
	}}
	containerRecords, err := containersFromProto(containers)
	if err != nil {
		t.Fatalf("failed to build container records: %v", err)
	}
	containersJSON, err := json.Marshal(containerRecords)
	if err != nil {
		t.Fatalf("failed to marshal containers: %v", err)
	}

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, containersJSON, "ziti-id", int32(0), int64(0), instanceID, now, nil, nil, now, now)

	query := fmt.Sprintf("UPDATE workloads SET status = $1, containers = $2, instance_id = $3, updated_at = NOW() WHERE id = $4 RETURNING %s", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(workloadStatusRunning, containersJSON, instanceID, workloadID).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:         workloadID.String(),
		Status:     runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING.Enum(),
		Containers: containers,
		InstanceId: &instanceID,
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetInstanceId() != instanceID {
		t.Fatalf("expected instance id %q, got %q", instanceID, resp.GetWorkload().GetInstanceId())
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateWorkloadRequiresFields(t *testing.T) {
	srv := New(Options{})

	_, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{Id: uuid.NewString()})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

func TestBatchUpdateWorkloadSampledAt(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	firstID := uuid.New()
	secondID := uuid.New()
	firstSampledAt := time.Now().UTC()
	secondSampledAt := firstSampledAt.Add(2 * time.Minute)

	query := "UPDATE workloads AS target SET last_metering_sampled_at = v.sampled_at, updated_at = NOW() FROM (VALUES ($1, $2), ($3, $4)) AS v(id, sampled_at) WHERE target.id = v.id"
	mockPool.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs(firstID, firstSampledAt, secondID, secondSampledAt).
		WillReturnResult(pgxmock.NewResult("UPDATE", 2))

	srv := New(Options{Pool: mockPool})
	_, err = srv.BatchUpdateWorkloadSampledAt(context.Background(), &runnersv1.BatchUpdateWorkloadSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{
			{Id: firstID.String(), SampledAt: timestamppb.New(firstSampledAt)},
			{Id: secondID.String(), SampledAt: timestamppb.New(secondSampledAt)},
		},
	})
	if err != nil {
		t.Fatalf("BatchUpdateWorkloadSampledAt failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBatchUpdateWorkloadSampledAtInvalid(t *testing.T) {
	srv := New(Options{})

	_, err := srv.BatchUpdateWorkloadSampledAt(context.Background(), &runnersv1.BatchUpdateWorkloadSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{{Id: "not-a-uuid", SampledAt: timestamppb.Now()}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

func TestSoftDeleteWorkload(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusStopped, containersJSON, "ziti-id", int32(0), int64(0), nil, now, nil, now, now, now)

	query := fmt.Sprintf("UPDATE workloads SET status = $1, removed_at = NOW(), updated_at = NOW() WHERE id = $2 RETURNING %s", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(workloadStatusStopped, workloadID).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	if err := srv.softDeleteWorkload(context.Background(), workloadID); err != nil {
		t.Fatalf("softDeleteWorkload failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
