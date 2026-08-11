package server

import (
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A runner says what a workload is doing, never what it should be doing.
// Accepting stopping or stopped from a node would let it take down a workload
// the Orchestrator intends to keep running.
func TestReportableStatusIsObservedStateOnly(t *testing.T) {
	for _, reported := range []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
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
