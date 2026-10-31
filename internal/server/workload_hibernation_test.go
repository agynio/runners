package server

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	notificationsv1 "github.com/agynio/runners/.gen/go/agynio/api/notifications/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func expectWorkloadSelect(t *testing.T, mockPool pgxmock.PgxPoolIface, record workloadRecord) {
	t.Helper()
	query := fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(record.Meta.ID).WillReturnRows(workloadRows(t, record))
}

// expectWorkloadUpdate expects the SET list the write builds, with the row id
// as the final argument, and answers with the record the write is to return.
func expectWorkloadUpdate(t *testing.T, mockPool pgxmock.PgxPoolIface, set string, args []any, result workloadRecord) {
	t.Helper()
	query := fmt.Sprintf("UPDATE workloads SET %s, updated_at = NOW() WHERE id = $%d RETURNING %s", set, len(args)+1, workloadColumns)
	mockPool.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(append(append([]any{}, args...), result.Meta.ID)...).
		WillReturnRows(workloadRows(t, result))
}

func collectNotifications() (fakeNotificationsClient, *[]*notificationsv1.PublishRequest) {
	published := []*notificationsv1.PublishRequest{}
	client := fakeNotificationsClient{publish: func(ctx context.Context, req *notificationsv1.PublishRequest) (*notificationsv1.PublishResponse, error) {
		published = append(published, req)
		return &notificationsv1.PublishResponse{}, nil
	}}
	return client, &published
}

func findEvent(t *testing.T, published []*notificationsv1.PublishRequest, event string) *notificationsv1.PublishRequest {
	t.Helper()
	for _, req := range published {
		if req.GetEvent() == event {
			return req
		}
	}
	t.Fatalf("expected a %s notification, got %d others", event, len(published))
	return nil
}

func payloadString(t *testing.T, req *notificationsv1.PublishRequest, key string) string {
	t.Helper()
	value, ok := req.GetPayload().AsMap()[key].(string)
	if !ok {
		t.Fatalf("expected string payload %s, got %v", key, req.GetPayload().AsMap()[key])
	}
	return value
}

func TestWorkloadStatusVocabularyIncludesHibernation(t *testing.T) {
	for enum, value := range map[runnersv1.WorkloadStatus]string{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING: workloadStatusHibernating,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED:  workloadStatusHibernated,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING:      workloadStatusWaking,
	} {
		got, err := workloadStatusToString(enum)
		if err != nil || got != value {
			t.Errorf("workloadStatusToString(%s) = %q, %v; want %q", enum, got, err, value)
		}
		back, err := workloadStatusFromString(value)
		if err != nil || back != enum {
			t.Errorf("workloadStatusFromString(%q) = %s, %v; want %s", value, back, err, enum)
		}
	}

	// The Orchestrator lists every non-terminal status when it reconciles.
	statuses, err := workloadStatusesToStrings([]runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING,
	})
	if err != nil {
		t.Fatalf("workloadStatusesToStrings: %v", err)
	}
	if len(statuses) != 3 || statuses[0] != workloadStatusHibernating || statuses[1] != workloadStatusHibernated || statuses[2] != workloadStatusWaking {
		t.Fatalf("unexpected statuses: %v", statuses)
	}
}

func TestWorkloadFailureReasonsIncludeHibernation(t *testing.T) {
	for enum, value := range map[runnersv1.WorkloadFailureReason]string{
		runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_WAKE_FAILED:   workloadFailureReasonWakeFailed,
		runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_SNAPSHOT_LOST: workloadFailureReasonSnapshotLost,
	} {
		got, err := workloadFailureReasonToString(enum)
		if err != nil || got != value {
			t.Errorf("workloadFailureReasonToString(%s) = %q, %v; want %q", enum, got, err, value)
		}
		back, err := workloadFailureReasonFromString(value)
		if err != nil || back != enum {
			t.Errorf("workloadFailureReasonFromString(%q) = %s, %v; want %s", value, back, err, enum)
		}
	}
}

// What a status change carries depends on where the workload comes from, so
// the same writer recording running means three different things.
func TestTransitionWorkload(t *testing.T) {
	cases := []struct {
		name     string
		from, to string
		want     workloadTransition
	}{
		{"boot", workloadStatusStarting, workloadStatusRunning, workloadTransition{ResetLastActivity: true}},
		{"wake", workloadStatusWaking, workloadStatusRunning, workloadTransition{ResetLastActivity: true, Woke: true}},
		{"hibernation that did not take", workloadStatusHibernating, workloadStatusRunning, workloadTransition{}},
		{"entering hibernated", workloadStatusHibernating, workloadStatusHibernated, workloadTransition{EnteredHibernated: true}},
		{"hibernated re-reported", workloadStatusHibernated, workloadStatusHibernated, workloadTransition{}},
		{"asked to hibernate", workloadStatusRunning, workloadStatusHibernating, workloadTransition{}},
		{"asked to wake", workloadStatusHibernated, workloadStatusWaking, workloadTransition{}},
		{"stopped", workloadStatusRunning, workloadStatusStopped, workloadTransition{}},
	}
	for _, tc := range cases {
		if got := transitionWorkload(tc.from, tc.to); got != tc.want {
			t.Errorf("%s: transitionWorkload(%q, %q) = %+v, want %+v", tc.name, tc.from, tc.to, got, tc.want)
		}
	}
}

// The runner reports hibernated with the size of what it persisted and no
// timestamp; the service stamps the moment it entered the state.
func TestUpdateWorkloadEnteringHibernatedStampsHibernatedAt(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	now := time.Now().UTC()
	existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	existing.Status = workloadStatusHibernating
	existing.AgentState = workloadAgentStateIdle
	expectWorkloadSelect(t, mockPool, existing)

	snapshotSize := int64(461_000_000)
	hibernated := existing
	hibernated.Status = workloadStatusHibernated
	hibernated.HibernatedAt = &now
	hibernated.SnapshotSizeBytes = &snapshotSize
	expectWorkloadUpdate(t, mockPool, "status = $1, hibernated_at = NOW(), snapshot_size_bytes = $2", []any{workloadStatusHibernated, snapshotSize}, hibernated)

	notificationsClient, published := collectNotifications()
	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	resp, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:                existing.Meta.ID.String(),
		Status:            runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED.Enum(),
		SnapshotSizeBytes: &snapshotSize,
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	workload := resp.GetWorkload()
	if workload.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED {
		t.Fatalf("expected hibernated, got %s", workload.GetStatus())
	}
	if workload.GetHibernatedAt() == nil || !workload.GetHibernatedAt().AsTime().Equal(now) {
		t.Fatalf("expected hibernated_at %v, got %v", now, workload.GetHibernatedAt())
	}
	if workload.SnapshotSizeBytes == nil || workload.GetSnapshotSizeBytes() != snapshotSize {
		t.Fatalf("expected snapshot_size_bytes %d, got %v", snapshotSize, workload.SnapshotSizeBytes)
	}
	if workload.WokenAt != nil {
		t.Fatalf("expected no woken_at before the first wake, got %v", workload.GetWokenAt())
	}

	// A status change: the updated event and the status_changed event, as for
	// any other status, with the new status in both.
	if len(*published) != 2 {
		t.Fatalf("expected 2 notifications, got %d", len(*published))
	}
	for _, event := range []string{"workload.updated", "workload.status_changed"} {
		if got := payloadString(t, findEvent(t, *published, event), "status"); got != workloadStatusHibernated {
			t.Fatalf("expected %s status payload hibernated, got %q", event, got)
		}
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// The Orchestrator's reconciliation supplies hibernated_at itself when it
// catches a report the runner never got through; what it says is what is kept.
func TestUpdateWorkloadHonoursExplicitHibernatedAt(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	now := time.Now().UTC()
	hibernatedAt := now.Add(-time.Minute)
	existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	existing.Status = workloadStatusHibernating
	expectWorkloadSelect(t, mockPool, existing)

	snapshotSize := int64(85_000_000)
	hibernated := existing
	hibernated.Status = workloadStatusHibernated
	hibernated.HibernatedAt = &hibernatedAt
	hibernated.SnapshotSizeBytes = &snapshotSize
	expectWorkloadUpdate(t, mockPool, "status = $1, hibernated_at = $2, snapshot_size_bytes = $3", []any{workloadStatusHibernated, hibernatedAt, snapshotSize}, hibernated)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:                existing.Meta.ID.String(),
		Status:            runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED.Enum(),
		HibernatedAt:      timestamppb.New(hibernatedAt),
		SnapshotSizeBytes: &snapshotSize,
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	if got := resp.GetWorkload().GetHibernatedAt().AsTime(); !got.Equal(hibernatedAt) {
		t.Fatalf("expected hibernated_at %v, got %v", hibernatedAt, got)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// waking -> running is the wake. It records when, resumes compute billing at
// that moment so the sleep is never billed as compute, puts the agent back to
// processing, gives agynd a fresh keepalive window, and clears what described
// the sleep -- whichever writer records it.
func TestUpdateWorkloadWakeCarriesResets(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	now := time.Now().UTC()
	hibernatedAt := now.Add(-time.Hour)
	snapshotSize := int64(461_000_000)
	existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	existing.Status = workloadStatusWaking
	existing.AgentState = workloadAgentStateIdle
	existing.HibernatedAt = &hibernatedAt
	existing.SnapshotSizeBytes = &snapshotSize
	existing.LastMeteringAt = &hibernatedAt
	expectWorkloadSelect(t, mockPool, existing)

	woken := existing
	woken.Status = workloadStatusRunning
	woken.AgentState = workloadAgentStateProcessing
	woken.HibernatedAt = nil
	woken.SnapshotSizeBytes = nil
	woken.WokenAt = &now
	woken.LastMeteringAt = &now
	expectWorkloadUpdate(t, mockPool,
		"status = $1, last_metering_sampled_at = NOW(), hibernated_at = NULL, woken_at = NOW(), snapshot_size_bytes = NULL, agent_state = $2, last_activity_at = NOW()",
		[]any{workloadStatusRunning, workloadAgentStateProcessing}, woken)

	notificationsClient, published := collectNotifications()
	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	resp, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:     existing.Meta.ID.String(),
		Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING.Enum(),
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	workload := resp.GetWorkload()
	if workload.GetWokenAt() == nil || !workload.GetWokenAt().AsTime().Equal(now) {
		t.Fatalf("expected woken_at %v, got %v", now, workload.GetWokenAt())
	}
	if workload.HibernatedAt != nil || workload.SnapshotSizeBytes != nil {
		t.Fatalf("expected the sleep cleared, got hibernated_at %v snapshot_size_bytes %v", workload.GetHibernatedAt(), workload.SnapshotSizeBytes)
	}
	if workload.GetAgentState() != runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_PROCESSING {
		t.Fatalf("expected agent_state processing after the wake, got %s", workload.GetAgentState())
	}

	if len(*published) != 2 {
		t.Fatalf("expected 2 notifications, got %d", len(*published))
	}
	updated := findEvent(t, *published, "workload.updated")
	if got := payloadString(t, updated, "agent_state"); got != workloadAgentStateProcessing {
		t.Fatalf("expected agent_state processing in the event, got %q", got)
	}
	if got := payloadString(t, updated, "status"); got != workloadStatusRunning {
		t.Fatalf("expected status running in the event, got %q", got)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A writer that says when the wake happened, or through when usage is
// recorded, is not second-guessed; only the resets it left out are defaulted.
func TestUpdateWorkloadWakeKeepsExplicitTimestamps(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	now := time.Now().UTC()
	wokenAt := now.Add(-time.Second)
	existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	existing.Status = workloadStatusWaking
	expectWorkloadSelect(t, mockPool, existing)

	woken := existing
	woken.Status = workloadStatusRunning
	woken.WokenAt = &wokenAt
	woken.LastMeteringAt = &wokenAt
	expectWorkloadUpdate(t, mockPool,
		"status = $1, last_metering_sampled_at = $2, hibernated_at = NULL, woken_at = $3, snapshot_size_bytes = NULL, agent_state = $4, last_activity_at = NOW()",
		[]any{workloadStatusRunning, wokenAt, wokenAt, workloadAgentStateProcessing}, woken)

	srv := New(Options{Pool: mockPool})
	_, err = srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:                    existing.Meta.ID.String(),
		Status:                runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING.Enum(),
		WokenAt:               timestamppb.New(wokenAt),
		LastMeteringSampledAt: timestamppb.New(wokenAt),
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// hibernating -> running is a hibernation the runner could not complete. The
// workload never slept, so nothing is reset: the agent stays idle and the
// Orchestrator retries from where it was.
func TestUpdateWorkloadFailedHibernationResetsNothing(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	now := time.Now().UTC()
	existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	existing.Status = workloadStatusHibernating
	existing.AgentState = workloadAgentStateIdle
	expectWorkloadSelect(t, mockPool, existing)

	running := existing
	running.Status = workloadStatusRunning
	expectWorkloadUpdate(t, mockPool, "status = $1", []any{workloadStatusRunning}, running)

	notificationsClient, published := collectNotifications()
	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	resp, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:     existing.Meta.ID.String(),
		Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING.Enum(),
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	if resp.GetWorkload().GetAgentState() != runnersv1.WorkloadAgentState_WORKLOAD_AGENT_STATE_IDLE {
		t.Fatalf("expected the agent left idle, got %s", resp.GetWorkload().GetAgentState())
	}
	if got := payloadString(t, findEvent(t, *published, "workload.updated"), "agent_state"); got != workloadAgentStateIdle {
		t.Fatalf("expected agent_state idle in the event, got %q", got)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestUpdateWorkloadRejectsNegativeSnapshotSize(t *testing.T) {
	srv := New(Options{})
	size := int64(-1)
	_, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:                uuid.NewString(),
		Status:            runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED.Enum(),
		SnapshotSizeBytes: &size,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}

// The hibernation fields are mutable on their own, like removed_at or
// last_metering_sampled_at; a write carrying only one of them is a write.
func TestUpdateWorkloadAcceptsHibernationFieldsAlone(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	now := time.Now().UTC()
	record := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	record.WokenAt = &now
	expectWorkloadUpdate(t, mockPool, "woken_at = $1", []any{now}, record)

	srv := New(Options{Pool: mockPool})
	resp, err := srv.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id:      record.Meta.ID.String(),
		WokenAt: timestamppb.New(now),
	})
	if err != nil {
		t.Fatalf("UpdateWorkload failed: %v", err)
	}
	if got := resp.GetWorkload().GetWokenAt().AsTime(); !got.Equal(now) {
		t.Fatalf("expected woken_at %v, got %v", now, got)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestToProtoWorkloadCarriesHibernationFields(t *testing.T) {
	now := time.Now().UTC()
	record := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)

	awake, err := toProtoWorkload(record)
	if err != nil {
		t.Fatalf("toProtoWorkload: %v", err)
	}
	if awake.HibernatedAt != nil || awake.WokenAt != nil || awake.SnapshotSizeBytes != nil {
		t.Fatalf("expected an awake workload to carry no hibernation fields, got %v %v %v", awake.HibernatedAt, awake.WokenAt, awake.SnapshotSizeBytes)
	}

	hibernatedAt := now.Add(-time.Hour)
	wokenAt := now.Add(-2 * time.Hour)
	snapshotSize := int64(200_000_000)
	record.Status = workloadStatusHibernated
	record.HibernatedAt = &hibernatedAt
	record.WokenAt = &wokenAt
	record.SnapshotSizeBytes = &snapshotSize

	asleep, err := toProtoWorkload(record)
	if err != nil {
		t.Fatalf("toProtoWorkload: %v", err)
	}
	if !asleep.GetHibernatedAt().AsTime().Equal(hibernatedAt) {
		t.Fatalf("expected hibernated_at %v, got %v", hibernatedAt, asleep.GetHibernatedAt())
	}
	if !asleep.GetWokenAt().AsTime().Equal(wokenAt) {
		t.Fatalf("expected woken_at %v, got %v", wokenAt, asleep.GetWokenAt())
	}
	if asleep.GetSnapshotSizeBytes() != snapshotSize {
		t.Fatalf("expected snapshot_size_bytes %d, got %d", snapshotSize, asleep.GetSnapshotSizeBytes())
	}
}
