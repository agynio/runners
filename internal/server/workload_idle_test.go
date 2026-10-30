package server

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const setWorkloadIdleQuery = "UPDATE workloads SET agent_state = $1, updated_at = NOW() WHERE id = $2 AND owner_kind = $3 AND owner_id = $4 AND status = $5 AND agent_state = $6 AND removed_at IS NULL RETURNING %s"

func identityContext(id uuid.UUID) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, id.String()))
}

// The turn is over: the workload goes idle at once, with the event the sweep
// would have published up to KEEPALIVE_GRACE plus a tick later.
func TestReportWorkloadIdleFlipsProcessingToIdle(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	now := time.Now().UTC()
	existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	expectWorkloadSelect(t, mockPool, existing)

	idle := existing
	idle.AgentState = workloadAgentStateIdle
	mockPool.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(setWorkloadIdleQuery, workloadColumns))).
		WithArgs(workloadAgentStateIdle, existing.Meta.ID, runtimeOwnerKindAgentInstance, existing.OwnerID, workloadStatusRunning, workloadAgentStateProcessing).
		WillReturnRows(workloadRows(t, idle))

	notificationsClient, published := collectNotifications()
	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	if _, err := srv.ReportWorkloadIdle(identityContext(existing.OwnerID), &runnersv1.ReportWorkloadIdleRequest{Id: existing.Meta.ID.String()}); err != nil {
		t.Fatalf("ReportWorkloadIdle failed: %v", err)
	}

	if len(*published) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(*published))
	}
	event := (*published)[0]
	if event.GetEvent() != "workload.updated" {
		t.Fatalf("unexpected event: %s", event.GetEvent())
	}
	rooms := event.GetRooms()
	workloadRoom := fmt.Sprintf("workload:%s", existing.Meta.ID)
	orgRoom := fmt.Sprintf("organization:%s", existing.OrganizationID)
	if len(rooms) != 2 || !hasRoom(rooms, workloadRoom) || !hasRoom(rooms, orgRoom) {
		t.Fatalf("unexpected workload.updated rooms: %v", rooms)
	}
	if got := payloadString(t, event, "agent_state"); got != workloadAgentStateIdle {
		t.Fatalf("expected agent_state idle in the event, got %q", got)
	}
	if got := payloadString(t, event, "status"); got != workloadStatusRunning {
		t.Fatalf("expected status running in the event, got %q", got)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Nothing to record is not an error: agynd retries, and a retry must not
// become a second event. The flip is conditional in the database, so a
// workload already idle, or no longer running, matches no row.
func TestReportWorkloadIdleIsANoOpWhenThereIsNothingToFlip(t *testing.T) {
	for name, shape := range map[string]func(*workloadRecord){
		"already idle":  func(w *workloadRecord) { w.AgentState = workloadAgentStateIdle },
		"hibernated":    func(w *workloadRecord) { w.Status = workloadStatusHibernated },
		"still booting": func(w *workloadRecord) { w.Status = workloadStatusStarting },
	} {
		t.Run(name, func(t *testing.T) {
			mockPool, err := pgxmock.NewPool()
			if err != nil {
				t.Fatalf("failed to create mock pool: %v", err)
			}

			existing := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())
			shape(&existing)
			expectWorkloadSelect(t, mockPool, existing)
			mockPool.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf(setWorkloadIdleQuery, workloadColumns))).
				WithArgs(workloadAgentStateIdle, existing.Meta.ID, runtimeOwnerKindAgentInstance, existing.OwnerID, workloadStatusRunning, workloadAgentStateProcessing).
				WillReturnRows(pgxmock.NewRows(workloadRowColumns))

			notificationsClient, published := collectNotifications()
			srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
			if _, err := srv.ReportWorkloadIdle(identityContext(existing.OwnerID), &runnersv1.ReportWorkloadIdleRequest{Id: existing.Meta.ID.String()}); err != nil {
				t.Fatalf("ReportWorkloadIdle failed: %v", err)
			}
			if len(*published) != 0 {
				t.Fatalf("expected no notifications, got %d", len(*published))
			}

			if err := mockPool.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

// As TouchWorkload: only the workload's own identity may end its turn.
func TestReportWorkloadIdleRequiresOwnerIdentity(t *testing.T) {
	now := time.Now().UTC()
	owner := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), now)
	sandbox := owner
	sandbox.OwnerKind = runtimeOwnerKindSandbox
	sandbox.ThreadID = uuid.Nil
	sandbox.AgentID = uuid.Nil

	for name, tc := range map[string]struct {
		workload workloadRecord
		ctx      context.Context
	}{
		"another identity":      {owner, identityContext(uuid.New())},
		"the agent class":       {owner, identityContext(owner.AgentID)},
		"no identity":           {owner, context.Background()},
		"a sandbox, by its own": {sandbox, identityContext(sandbox.OwnerID)},
		"a sandbox, internally": {sandbox, context.Background()},
	} {
		t.Run(name, func(t *testing.T) {
			mockPool, err := pgxmock.NewPool()
			if err != nil {
				t.Fatalf("failed to create mock pool: %v", err)
			}
			expectWorkloadSelect(t, mockPool, tc.workload)

			srv := New(Options{Pool: mockPool})
			_, err = srv.ReportWorkloadIdle(tc.ctx, &runnersv1.ReportWorkloadIdleRequest{Id: tc.workload.Meta.ID.String()})
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("expected PermissionDenied error, got %v", err)
			}

			if err := mockPool.ExpectationsWereMet(); err != nil {
				t.Fatalf("unmet expectations: %v", err)
			}
		})
	}
}

func TestReportWorkloadIdleUnknownWorkload(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workloadID := uuid.New()
	mockPool.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns))).
		WithArgs(workloadID).
		WillReturnError(pgx.ErrNoRows)

	srv := New(Options{Pool: mockPool})
	_, err = srv.ReportWorkloadIdle(identityContext(uuid.New()), &runnersv1.ReportWorkloadIdleRequest{Id: workloadID.String()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound error, got %v", err)
	}

	if err := mockPool.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestReportWorkloadIdleRejectsInvalidID(t *testing.T) {
	srv := New(Options{})
	_, err := srv.ReportWorkloadIdle(identityContext(uuid.New()), &runnersv1.ReportWorkloadIdleRequest{Id: "not-a-uuid"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got %v", err)
	}
}
