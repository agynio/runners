//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBatchUpdateWorkloadSampledAtSingle(t *testing.T) {
	ctx, runnerID := setupRunner(t)

	threadID := uuid.NewString()
	agentID := uuid.NewString()
	organizationID := uuid.NewString()

	workloadID := uuid.NewString()
	createWorkload(t, ctx, workloadID, runnerID, threadID, agentID, organizationID)
	cleanupWorkload(t, workloadID)

	sampledAt := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := runnerClient.BatchUpdateWorkloadSampledAt(ctx, &runnersv1.BatchUpdateWorkloadSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{{
			Id:        workloadID,
			SampledAt: timestamppb.New(sampledAt),
		}},
	}); err != nil {
		t.Fatalf("BatchUpdateWorkloadSampledAt failed: %v", err)
	}

	assertWorkloadSampledAt(t, ctx, workloadID, sampledAt)
}

func TestBatchUpdateWorkloadSampledAtMultiple(t *testing.T) {
	ctx, runnerID := setupRunner(t)

	threadID := uuid.NewString()
	agentID := uuid.NewString()
	organizationID := uuid.NewString()

	workloadIDs := []string{uuid.NewString(), uuid.NewString()}
	for _, workloadID := range workloadIDs {
		createWorkload(t, ctx, workloadID, runnerID, threadID, agentID, organizationID)
		cleanupWorkload(t, workloadID)
	}

	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	workloadTimes := []time.Time{baseTime, baseTime.Add(2 * time.Minute)}

	entries := make([]*runnersv1.SampledAtEntry, 0, len(workloadIDs))
	for i, workloadID := range workloadIDs {
		entries = append(entries, &runnersv1.SampledAtEntry{
			Id:        workloadID,
			SampledAt: timestamppb.New(workloadTimes[i]),
		})
	}

	if _, err := runnerClient.BatchUpdateWorkloadSampledAt(ctx, &runnersv1.BatchUpdateWorkloadSampledAtRequest{
		Entries: entries,
	}); err != nil {
		t.Fatalf("BatchUpdateWorkloadSampledAt failed: %v", err)
	}

	for i, workloadID := range workloadIDs {
		assertWorkloadSampledAt(t, ctx, workloadID, workloadTimes[i])
	}
}

func TestBatchUpdateWorkloadSampledAtIdempotent(t *testing.T) {
	ctx, runnerID := setupRunner(t)

	threadID := uuid.NewString()
	agentID := uuid.NewString()
	organizationID := uuid.NewString()

	workloadID := uuid.NewString()
	createWorkload(t, ctx, workloadID, runnerID, threadID, agentID, organizationID)
	cleanupWorkload(t, workloadID)

	sampledAt := time.Now().UTC().Truncate(time.Microsecond)
	req := &runnersv1.BatchUpdateWorkloadSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{{
			Id:        workloadID,
			SampledAt: timestamppb.New(sampledAt),
		}},
	}

	if _, err := runnerClient.BatchUpdateWorkloadSampledAt(ctx, req); err != nil {
		t.Fatalf("BatchUpdateWorkloadSampledAt failed: %v", err)
	}
	if _, err := runnerClient.BatchUpdateWorkloadSampledAt(ctx, req); err != nil {
		t.Fatalf("BatchUpdateWorkloadSampledAt idempotent failed: %v", err)
	}

	assertWorkloadSampledAt(t, ctx, workloadID, sampledAt)
}

func TestBatchUpdateVolumeSampledAtSingle(t *testing.T) {
	ctx, runnerID := setupRunner(t)

	threadID := uuid.NewString()
	agentID := uuid.NewString()
	organizationID := uuid.NewString()

	volumeID := uuid.NewString()
	volumeExternalID := uuid.NewString()
	createVolume(t, ctx, volumeID, volumeExternalID, runnerID, threadID, agentID, organizationID)
	cleanupVolume(t, volumeID)

	sampledAt := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := runnerClient.BatchUpdateVolumeSampledAt(ctx, &runnersv1.BatchUpdateVolumeSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{{
			Id:        volumeID,
			SampledAt: timestamppb.New(sampledAt),
		}},
	}); err != nil {
		t.Fatalf("BatchUpdateVolumeSampledAt failed: %v", err)
	}

	assertVolumeSampledAt(t, ctx, volumeID, sampledAt)
}

func TestBatchUpdateVolumeSampledAtMultiple(t *testing.T) {
	ctx, runnerID := setupRunner(t)

	threadID := uuid.NewString()
	agentID := uuid.NewString()
	organizationID := uuid.NewString()

	volumeIDs := []string{uuid.NewString(), uuid.NewString()}
	volumeExternalIDs := []string{uuid.NewString(), uuid.NewString()}
	for i, volumeID := range volumeIDs {
		createVolume(t, ctx, volumeID, volumeExternalIDs[i], runnerID, threadID, agentID, organizationID)
		cleanupVolume(t, volumeID)
	}

	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	volumeTimes := []time.Time{baseTime.Add(3 * time.Minute), baseTime.Add(5 * time.Minute)}

	entries := make([]*runnersv1.SampledAtEntry, 0, len(volumeIDs))
	for i, volumeID := range volumeIDs {
		entries = append(entries, &runnersv1.SampledAtEntry{
			Id:        volumeID,
			SampledAt: timestamppb.New(volumeTimes[i]),
		})
	}

	if _, err := runnerClient.BatchUpdateVolumeSampledAt(ctx, &runnersv1.BatchUpdateVolumeSampledAtRequest{
		Entries: entries,
	}); err != nil {
		t.Fatalf("BatchUpdateVolumeSampledAt failed: %v", err)
	}

	for i, volumeID := range volumeIDs {
		assertVolumeSampledAt(t, ctx, volumeID, volumeTimes[i])
	}
}

func TestBatchUpdateVolumeSampledAtIdempotent(t *testing.T) {
	ctx, runnerID := setupRunner(t)

	threadID := uuid.NewString()
	agentID := uuid.NewString()
	organizationID := uuid.NewString()

	volumeID := uuid.NewString()
	volumeExternalID := uuid.NewString()
	createVolume(t, ctx, volumeID, volumeExternalID, runnerID, threadID, agentID, organizationID)
	cleanupVolume(t, volumeID)

	sampledAt := time.Now().UTC().Truncate(time.Microsecond)
	req := &runnersv1.BatchUpdateVolumeSampledAtRequest{
		Entries: []*runnersv1.SampledAtEntry{{
			Id:        volumeID,
			SampledAt: timestamppb.New(sampledAt),
		}},
	}

	if _, err := runnerClient.BatchUpdateVolumeSampledAt(ctx, req); err != nil {
		t.Fatalf("BatchUpdateVolumeSampledAt failed: %v", err)
	}
	if _, err := runnerClient.BatchUpdateVolumeSampledAt(ctx, req); err != nil {
		t.Fatalf("BatchUpdateVolumeSampledAt idempotent failed: %v", err)
	}

	assertVolumeSampledAt(t, ctx, volumeID, sampledAt)
}

func setupRunner(t *testing.T) (context.Context, string) {
	t.Helper()
	ctx := newTestContext(t)
	runnerID := registerRunner(t, ctx)
	cleanupRunner(t, runnerID)
	return ctx, runnerID
}

func newTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

func cleanupRunner(t *testing.T, runnerID string) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout)
		defer cleanupCancel()
		_, _ = runnerClient.DeleteRunner(cleanupCtx, &runnersv1.DeleteRunnerRequest{Id: runnerID})
	})
}

func cleanupWorkload(t *testing.T, workloadID string) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout)
		defer cleanupCancel()
		_, _ = runnerClient.DeleteWorkload(cleanupCtx, &runnersv1.DeleteWorkloadRequest{Id: workloadID})
	})
}

func cleanupVolume(t *testing.T, volumeID string) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout)
		defer cleanupCancel()
		_, _ = runnerClient.UpdateVolume(cleanupCtx, &runnersv1.UpdateVolumeRequest{
			Id:        volumeID,
			Status:    runnersv1.VolumeStatus_VOLUME_STATUS_DELETED.Enum(),
			RemovedAt: timestamppb.New(time.Now().UTC()),
		})
	})
}

func assertWorkloadSampledAt(t *testing.T, ctx context.Context, workloadID string, expected time.Time) {
	t.Helper()
	resp, err := runnerClient.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID})
	if err != nil {
		t.Fatalf("GetWorkload failed: %v", err)
	}
	workload := resp.GetWorkload()
	if workload == nil {
		t.Fatal("GetWorkload missing workload")
	}
	lastSampledAt := workload.GetLastMeteringSampledAt()
	if lastSampledAt == nil {
		t.Fatalf("workload %s missing last_metering_sampled_at (expected %s)", workloadID, expected)
	}
	actual := lastSampledAt.AsTime()
	if !actual.Equal(expected) {
		t.Fatalf("workload %s sampled_at mismatch: got %s, want %s", workloadID, actual, expected)
	}
}

func assertVolumeSampledAt(t *testing.T, ctx context.Context, volumeID string, expected time.Time) {
	t.Helper()
	resp, err := runnerClient.GetVolume(ctx, &runnersv1.GetVolumeRequest{Id: volumeID})
	if err != nil {
		t.Fatalf("GetVolume failed: %v", err)
	}
	volume := resp.GetVolume()
	if volume == nil {
		t.Fatal("GetVolume missing volume")
	}
	lastSampledAt := volume.GetLastMeteringSampledAt()
	if lastSampledAt == nil {
		t.Fatalf("volume %s missing last_metering_sampled_at (expected %s)", volumeID, expected)
	}
	actual := lastSampledAt.AsTime()
	if !actual.Equal(expected) {
		t.Fatalf("volume %s sampled_at mismatch: got %s, want %s", volumeID, actual, expected)
	}
}

func registerRunner(t *testing.T, ctx context.Context) string {
	t.Helper()
	resp, err := runnerClient.RegisterRunner(ctx, &runnersv1.RegisterRunnerRequest{
		Name: "e2e-runner-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("RegisterRunner failed: %v", err)
	}
	runner := resp.GetRunner()
	if runner == nil || runner.GetMeta() == nil {
		t.Fatal("runner metadata missing")
	}
	runnerID := runner.GetMeta().GetId()
	if runnerID == "" {
		t.Fatal("runner ID missing")
	}
	return runnerID
}

func createWorkload(t *testing.T, ctx context.Context, workloadID, runnerID, threadID, agentID, organizationID string) {
	t.Helper()
	resp, err := runnerClient.CreateWorkload(ctx, &runnersv1.CreateWorkloadRequest{
		Id:             workloadID,
		RunnerId:       runnerID,
		ThreadId:       threadID,
		AgentId:        agentID,
		OrganizationId: organizationID,
		Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		Containers:     defaultContainers(),
		ZitiIdentityId: "ziti-test",
	})
	if err != nil {
		t.Fatalf("CreateWorkload failed: %v", err)
	}
	workload := resp.GetWorkload()
	if workload == nil || workload.GetMeta() == nil {
		t.Fatal("CreateWorkload missing workload metadata")
	}
	if workload.GetMeta().GetId() != workloadID {
		t.Fatalf("CreateWorkload returned unexpected ID")
	}
}

func createVolume(t *testing.T, ctx context.Context, volumeID, volumeExternalID, runnerID, threadID, agentID, organizationID string) {
	t.Helper()
	resp, err := runnerClient.CreateVolume(ctx, &runnersv1.CreateVolumeRequest{
		Id:             volumeID,
		VolumeId:       volumeExternalID,
		ThreadId:       threadID,
		RunnerId:       runnerID,
		AgentId:        agentID,
		OrganizationId: organizationID,
		SizeGb:         "10",
		Status:         runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	volume := resp.GetVolume()
	if volume == nil || volume.GetMeta() == nil {
		t.Fatal("CreateVolume missing volume metadata")
	}
	if volume.GetMeta().GetId() != volumeID {
		t.Fatalf("CreateVolume returned unexpected ID")
	}
}

func defaultContainers() []*runnersv1.Container {
	return []*runnersv1.Container{
		{
			ContainerId: "main",
			Name:        "main",
			Role:        runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
			Image:       "alpine:latest",
			Status:      runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
		},
	}
}
