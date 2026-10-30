package server

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A runner says what a workload is doing, never what it should be doing.
// Accepting stopping or stopped from a node would let it take down a workload
// the Orchestrator intends to keep running. The states around a sleep are
// observed: the runner is the one pausing and persisting, and a hibernation is
// complete when it says so.
func TestReportableStatusIsObservedStateOnly(t *testing.T) {
	for _, reported := range []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING,
	} {
		if !isReportableStatus(reported) {
			t.Errorf("expected %s to be reportable", reported)
		}
	}
	for _, reported := range []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED,
	} {
		if isReportableStatus(reported) {
			t.Errorf("expected %s to be refused as lifecycle, not observation", reported)
		}
	}
}

func workloadAt(updatedAt time.Time, removedAt *time.Time) workloadRecord {
	return workloadRecord{
		Meta:      entityMeta{UpdatedAt: updatedAt},
		RemovedAt: removedAt,
	}
}

func TestReportSupersedesAcceptsAFreshObservation(t *testing.T) {
	updatedAt := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	observed := timestamppb.New(updatedAt.Add(time.Second))

	if !reportSupersedes(observed, workloadAt(updatedAt, nil)) {
		t.Fatal("expected an observation newer than the record to apply")
	}
}

// Retries and informer resyncs resend the same report, sometimes after the
// platform has moved on. Applying one would walk the status backwards.
func TestReportSupersedesDropsAStaleObservation(t *testing.T) {
	updatedAt := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	observed := timestamppb.New(updatedAt.Add(-time.Second))

	if reportSupersedes(observed, workloadAt(updatedAt, nil)) {
		t.Fatal("expected an observation older than the record to be dropped")
	}
}

// A workload the platform has finished with is not something a runner reopens.
func TestReportSupersedesDropsAReportForARemovedWorkload(t *testing.T) {
	updatedAt := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	removedAt := updatedAt
	observed := timestamppb.New(updatedAt.Add(time.Hour))

	if reportSupersedes(observed, workloadAt(updatedAt, &removedAt)) {
		t.Fatal("expected a removed workload to refuse the report")
	}
}

// A runner that sends no timestamp is not punished for it; the record's own
// guards still apply.
func TestReportSupersedesAcceptsAMissingObservedAt(t *testing.T) {
	if !reportSupersedes(nil, workloadAt(time.Now().UTC(), nil)) {
		t.Fatal("expected a report without observed_at to apply")
	}
}

// expectReportingRunner answers the service-token lookup a report begins with.
func expectReportingRunner(t *testing.T, mockPool pgxmock.PgxPoolIface, runnerID uuid.UUID, token string) {
	t.Helper()
	now := time.Now().UTC()
	rows := pgxmock.NewRows(runnerRowColumns).
		AddRow(runnerID, "runner-1", pgtype.UUID{Valid: false}, uuid.New(), "ziti-id", "service-id", "runner-service", runnerStatusEnrolled, []byte("{}"), []byte(`["hibernate"]`), now, now)
	mockPool.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(`SELECT %s FROM runners WHERE service_token_hash = $1`, runnerColumns))).
		WithArgs(hashServiceToken(token)).
		WillReturnRows(rows)
}

// The runner reports hibernated with the size of what it persisted. The report
// lands through UpdateWorkload, so the service stamps hibernated_at and the
// record reads as the Orchestrator's own write would have left it.
func TestReportWorkloadStateRecordsHibernatedWithSnapshotSize(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	token := "runner-token"
	now := time.Now().UTC()
	existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	existing.Status = workloadStatusHibernating
	existing.AgentState = workloadAgentStateIdle
	expectReportingRunner(t, mockPool, existing.RunnerID, token)
	// Once for the report's own checks, once more on the UpdateWorkload path.
	expectWorkloadSelect(t, mockPool, existing)
	expectWorkloadSelect(t, mockPool, existing)

	snapshotSize := int64(461_000_000)
	hibernated := existing
	hibernated.Status = workloadStatusHibernated
	hibernated.HibernatedAt = &now
	hibernated.SnapshotSizeBytes = &snapshotSize
	expectWorkloadUpdate(t, mockPool, "status = $1, hibernated_at = NOW(), snapshot_size_bytes = $2", []any{workloadStatusHibernated, snapshotSize}, hibernated)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.ReportWorkloadState(context.Background(), &runnersv1.ReportWorkloadStateRequest{
		ServiceToken:      token,
		WorkloadId:        existing.Meta.ID.String(),
		Status:            runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
		ObservedAt:        timestamppb.New(now.Add(time.Second)),
		SnapshotSizeBytes: &snapshotSize,
	})
	if err != nil {
		t.Fatalf("ReportWorkloadState failed: %v", err)
	}
	if !resp.GetApplied() {
		t.Fatal("expected the report to be applied")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// The size describes a hibernated workload and nothing else. Carried on
// running it would outlive the wake that is meant to clear it.
func TestReportWorkloadStateRejectsSnapshotSizeOutsideHibernated(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	token := "runner-token"
	existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())
	existing.Status = workloadStatusWaking
	expectReportingRunner(t, mockPool, existing.RunnerID, token)
	expectWorkloadSelect(t, mockPool, existing)

	snapshotSize := int64(461_000_000)
	srv := New(Options{Pool: mockPool})
	_, err = srv.ReportWorkloadState(context.Background(), &runnersv1.ReportWorkloadStateRequest{
		ServiceToken:      token,
		WorkloadId:        existing.Meta.ID.String(),
		Status:            runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		SnapshotSizeBytes: &snapshotSize,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A runner reporting running for a workload the platform is waking records the
// wake, resets and all, exactly as the Orchestrator's reconciliation would.
func TestReportWorkloadStateRecordsTheWake(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	token := "runner-token"
	now := time.Now().UTC()
	hibernatedAt := now.Add(-time.Hour)
	snapshotSize := int64(461_000_000)
	existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	existing.Status = workloadStatusWaking
	existing.AgentState = workloadAgentStateIdle
	existing.HibernatedAt = &hibernatedAt
	existing.SnapshotSizeBytes = &snapshotSize
	expectReportingRunner(t, mockPool, existing.RunnerID, token)
	expectWorkloadSelect(t, mockPool, existing)
	expectWorkloadSelect(t, mockPool, existing)

	woken := existing
	woken.Status = workloadStatusRunning
	woken.AgentState = workloadAgentStateProcessing
	woken.HibernatedAt = nil
	woken.SnapshotSizeBytes = nil
	woken.WokenAt = &now
	expectWorkloadUpdate(t, mockPool,
		"status = $1, last_metering_sampled_at = NOW(), hibernated_at = NULL, woken_at = NOW(), snapshot_size_bytes = NULL, agent_state = $2, last_activity_at = NOW()",
		[]any{workloadStatusRunning, workloadAgentStateProcessing}, woken)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.ReportWorkloadState(context.Background(), &runnersv1.ReportWorkloadStateRequest{
		ServiceToken: token,
		WorkloadId:   existing.Meta.ID.String(),
		Status:       runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		ObservedAt:   timestamppb.New(now.Add(time.Second)),
	})
	if err != nil {
		t.Fatalf("ReportWorkloadState failed: %v", err)
	}
	if !resp.GetApplied() {
		t.Fatal("expected the report to be applied")
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
