package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v3"
)

// Every workload.updated reaches the flat platform room, not only the
// Console's organization and workload rooms: the Orchestrator holds the
// cluster-wide rooms alone and hibernates on the agent_state flip, so the
// event TouchWorkload, ReportWorkloadIdle and the sweep emit for it has to
// arrive the moment the turn ends rather than on the next poll.
func TestWorkloadUpdatedReachesThePlatformRoom(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workload := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())
	workload.AgentState = workloadAgentStateIdle

	notificationsClient, published := collectNotifications()
	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	srv.publishWorkloadUpdateNotifications(context.Background(), workload, false, false, false, true)

	// An agent_state flip is not a status change: workload.updated alone.
	if len(*published) != 1 {
		t.Fatalf("expected 1 notification, got %d", len(*published))
	}
	updated := findEvent(t, *published, "workload.updated")
	workloadRoom := fmt.Sprintf("workload:%s", workload.Meta.ID)
	orgRoom := fmt.Sprintf("organization:%s", workload.OrganizationID)
	rooms := updated.GetRooms()
	if len(rooms) != 3 || !hasRoom(rooms, orgRoom) || !hasRoom(rooms, workloadRoom) || !hasRoom(rooms, platformWorkloadsRoom) {
		t.Fatalf("expected workload.updated in the organization, workload and platform rooms, got %v", rooms)
	}
	if got := payloadString(t, updated, "agent_state"); got != workloadAgentStateIdle {
		t.Fatalf("expected agent_state idle in the event, got %q", got)
	}
}

// A status change publishes both events. workload.updated goes to its three
// rooms as always; workload.status_changed keeps the workload's room and the
// platform's, and never the organization's.
func TestWorkloadStatusChangedKeepsItsRooms(t *testing.T) {
	mockPool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create mock pool: %v", err)
	}

	workload := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())
	workload.Status = workloadStatusHibernated

	notificationsClient, published := collectNotifications()
	srv := New(Options{Pool: mockPool, NotificationsClient: notificationsClient})
	srv.publishWorkloadUpdateNotifications(context.Background(), workload, true, false, false, false)

	if len(*published) != 2 {
		t.Fatalf("expected 2 notifications, got %d", len(*published))
	}
	workloadRoom := fmt.Sprintf("workload:%s", workload.Meta.ID)
	orgRoom := fmt.Sprintf("organization:%s", workload.OrganizationID)

	updatedRooms := findEvent(t, *published, "workload.updated").GetRooms()
	if len(updatedRooms) != 3 || !hasRoom(updatedRooms, orgRoom) || !hasRoom(updatedRooms, workloadRoom) || !hasRoom(updatedRooms, platformWorkloadsRoom) {
		t.Fatalf("expected workload.updated in the organization, workload and platform rooms, got %v", updatedRooms)
	}
	changedRooms := findEvent(t, *published, "workload.status_changed").GetRooms()
	if len(changedRooms) != 2 || !hasRoom(changedRooms, workloadRoom) || !hasRoom(changedRooms, platformWorkloadsRoom) {
		t.Fatalf("expected workload.status_changed in the workload and platform rooms only, got %v", changedRooms)
	}
	for _, event := range *published {
		if got := payloadString(t, event, "status"); got != workloadStatusHibernated {
			t.Fatalf("expected %s status payload hibernated, got %q", event.GetEvent(), got)
		}
	}
}
