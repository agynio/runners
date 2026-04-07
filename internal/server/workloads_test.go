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

var workloadRowColumns = []string{
	"id",
	"runner_id",
	"thread_id",
	"agent_id",
	"organization_id",
	"status",
	"containers",
	"ziti_identity_id",
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
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, containersJSON, "ziti-id", now, now)

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
		AddRow(workloadID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, containersJSON, "ziti-id", now, now)

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

func TestListWorkloadsNoFiltersReturnsAll(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	firstID := uuid.New()
	secondID := uuid.New()
	runnerID := uuid.New()
	threadID := uuid.New()
	agentID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	containersJSON := []byte("[]")

	rows := pgxmock.NewRows(workloadRowColumns).
		AddRow(firstID, runnerID, threadID, agentID, organizationID, workloadStatusRunning, containersJSON, "ziti-id", now, now).
		AddRow(secondID, runnerID, threadID, agentID, organizationID, workloadStatusStopped, containersJSON, "ziti-id", now, now)

	query := fmt.Sprintf("SELECT %s FROM workloads ORDER BY id ASC LIMIT $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(51).
		WillReturnRows(rows)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.ListWorkloads(context.Background(), &runnersv1.ListWorkloadsRequest{})
	if err != nil {
		t.Fatalf("ListWorkloads failed: %v", err)
	}
	if len(resp.GetWorkloads()) != 2 {
		t.Fatalf("expected 2 workloads, got %d", len(resp.GetWorkloads()))
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
