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

func TestBatchUpdateSampledAt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	runnerID := registerRunner(t, ctx)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout)
		defer cleanupCancel()
		_, _ = runnerClient.DeleteRunner(cleanupCtx, &runnersv1.DeleteRunnerRequest{Id: runnerID})
	})

	threadID := uuid.NewString()
	agentID := uuid.NewString()
	organizationID := uuid.NewString()

	workloadIDs := []string{uuid.NewString(), uuid.NewString()}
	for _, workloadID := range workloadIDs {
		createWorkload(t, ctx, workloadID, runnerID, threadID, agentID, organizationID)
		workloadID := workloadID
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout)
			defer cleanupCancel()
			_, _ = runnerClient.DeleteWorkload(cleanupCtx, &runnersv1.DeleteWorkloadRequest{Id: workloadID})
		})
	}

	volumeIDs := []string{uuid.NewString(), uuid.NewString()}
	volumeExternalIDs := []string{uuid.NewString(), uuid.NewString()}
	for i, volumeID := range volumeIDs {
		createVolume(t, ctx, volumeID, volumeExternalIDs[i], runnerID, threadID, agentID, organizationID)
		volumeID := volumeID
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

	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	workloadTimes := []time.Time{baseTime, baseTime.Add(2 * time.Minute)}
	volumeTimes := []time.Time{baseTime.Add(3 * time.Minute), baseTime.Add(5 * time.Minute)}

	workloadEntries := make([]*runnersv1.SampledAtEntry, 0, len(workloadIDs))
	for i, workloadID := range workloadIDs {
		workloadEntries = append(workloadEntries, &runnersv1.SampledAtEntry{
			Id:        workloadID,
			SampledAt: timestamppb.New(workloadTimes[i]),
		})
	}

	if _, err := runnerClient.BatchUpdateWorkloadSampledAt(ctx, &runnersv1.BatchUpdateWorkloadSampledAtRequest{
		Entries: workloadEntries,
	}); err != nil {
		t.Fatalf("BatchUpdateWorkloadSampledAt failed: %v", err)
	}

	for i, workloadID := range workloadIDs {
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
			t.Fatalf("workload %s missing last_metering_sampled_at", workloadID)
		}
		if !lastSampledAt.AsTime().Equal(workloadTimes[i]) {
			t.Fatalf("workload %s sampled_at mismatch: got %s", workloadID, lastSampledAt.AsTime())
		}
	}

	volumeEntries := make([]*runnersv1.SampledAtEntry, 0, len(volumeIDs))
	for i, volumeID := range volumeIDs {
		volumeEntries = append(volumeEntries, &runnersv1.SampledAtEntry{
			Id:        volumeID,
			SampledAt: timestamppb.New(volumeTimes[i]),
		})
	}

	if _, err := runnerClient.BatchUpdateVolumeSampledAt(ctx, &runnersv1.BatchUpdateVolumeSampledAtRequest{
		Entries: volumeEntries,
	}); err != nil {
		t.Fatalf("BatchUpdateVolumeSampledAt failed: %v", err)
	}

	for i, volumeID := range volumeIDs {
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
			t.Fatalf("volume %s missing last_metering_sampled_at", volumeID)
		}
		if !lastSampledAt.AsTime().Equal(volumeTimes[i]) {
			t.Fatalf("volume %s sampled_at mismatch: got %s", volumeID, lastSampledAt.AsTime())
		}
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
