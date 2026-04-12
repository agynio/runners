//go:build e2e

package e2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
)

const testTimeout = 60 * time.Second

func TestRunnerLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	registerLabels := map[string]string{
		"region": "test",
		"tier":   "e2e",
	}
	registerResp, err := runnerClient.RegisterRunner(ctx, &runnersv1.RegisterRunnerRequest{
		Name:   "e2e-runner",
		Labels: registerLabels,
	})
	if err != nil {
		t.Fatalf("RegisterRunner failed: %v", err)
	}

	runner := registerResp.GetRunner()
	if runner == nil || runner.GetMeta() == nil {
		t.Fatal("runner metadata missing")
	}
	runnerID := runner.GetMeta().GetId()
	if runnerID == "" {
		t.Fatal("runner ID missing")
	}
	if !reflect.DeepEqual(runner.GetLabels(), registerLabels) {
		t.Fatalf("RegisterRunner returned unexpected labels")
	}
	token := registerResp.GetServiceToken()
	if token == "" {
		t.Fatal("service token missing")
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout)
		defer cleanupCancel()
		_, _ = runnerClient.DeleteRunner(cleanupCtx, &runnersv1.DeleteRunnerRequest{Id: runnerID})
	})

	validateResp, err := runnerClient.ValidateServiceToken(ctx, &runnersv1.ValidateServiceTokenRequest{
		TokenHash: token,
	})
	if err != nil {
		t.Fatalf("ValidateServiceToken failed: %v", err)
	}
	if validateResp.GetRunner().GetMeta().GetId() != runnerID {
		t.Fatalf("ValidateServiceToken returned unexpected runner ID")
	}

	workloadID := uuid.NewString()
	threadID := uuid.NewString()
	agentID := uuid.NewString()
	organizationID := uuid.NewString()

	createResp, err := runnerClient.CreateWorkload(ctx, &runnersv1.CreateWorkloadRequest{
		Id:             workloadID,
		RunnerId:       runnerID,
		ThreadId:       threadID,
		AgentId:        agentID,
		OrganizationId: organizationID,
		Status:         runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		Containers: []*runnersv1.Container{
			{
				ContainerId: "main",
				Name:        "main",
				Role:        runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
				Image:       "alpine:latest",
				Status:      runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
			},
		},
		ZitiIdentityId: "ziti-test",
	})
	if err != nil {
		t.Fatalf("CreateWorkload failed: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout)
		defer cleanupCancel()
		_, _ = runnerClient.DeleteWorkload(cleanupCtx, &runnersv1.DeleteWorkloadRequest{Id: workloadID})
	})

	if createResp.GetWorkload().GetMeta().GetId() != workloadID {
		t.Fatalf("CreateWorkload returned unexpected ID")
	}

	updateResp, err := runnerClient.UpdateWorkloadStatus(ctx, &runnersv1.UpdateWorkloadStatusRequest{
		Id:     workloadID,
		Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		Containers: []*runnersv1.Container{
			{
				ContainerId: "main",
				Name:        "main",
				Role:        runnersv1.ContainerRole_CONTAINER_ROLE_MAIN,
				Image:       "alpine:latest",
				Status:      runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateWorkloadStatus failed: %v", err)
	}
	if updateResp.GetWorkload().GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
		t.Fatalf("UpdateWorkloadStatus did not return running status")
	}

	if _, err := runnerClient.TouchWorkload(ctx, &runnersv1.TouchWorkloadRequest{Id: workloadID}); err != nil {
		t.Fatalf("TouchWorkload failed: %v", err)
	}

	getResp, err := runnerClient.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID})
	if err != nil {
		t.Fatalf("GetWorkload failed: %v", err)
	}
	if getResp.GetWorkload().GetThreadId() != threadID {
		t.Fatalf("GetWorkload returned unexpected thread ID")
	}

	listByThreadResp, err := runnerClient.ListWorkloadsByThread(ctx, &runnersv1.ListWorkloadsByThreadRequest{
		ThreadId:  threadID,
		PageSize:  10,
		PageToken: "",
	})
	if err != nil {
		t.Fatalf("ListWorkloadsByThread failed: %v", err)
	}
	if !containsWorkload(listByThreadResp.GetWorkloads(), workloadID) {
		t.Fatalf("ListWorkloadsByThread missing workload")
	}

	listResp, err := runnerClient.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{
		PageSize: 10,
		Statuses: []runnersv1.WorkloadStatus{runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
	})
	if err != nil {
		t.Fatalf("ListWorkloads failed: %v", err)
	}
	if !containsWorkload(listResp.GetWorkloads(), workloadID) {
		t.Fatalf("ListWorkloads missing workload")
	}

	if _, err := runnerClient.DeleteWorkload(ctx, &runnersv1.DeleteWorkloadRequest{Id: workloadID}); err != nil {
		t.Fatalf("DeleteWorkload failed: %v", err)
	}

	deletedResp, err := runnerClient.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID})
	if err != nil {
		t.Fatalf("GetWorkload after delete failed: %v", err)
	}
	if deletedResp.GetWorkload().GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED {
		t.Fatalf("expected stopped status after delete")
	}
	if deletedResp.GetWorkload().GetRemovedAt() == nil {
		t.Fatalf("expected removed_at after delete")
	}

	if _, err := runnerClient.DeleteRunner(ctx, &runnersv1.DeleteRunnerRequest{Id: runnerID}); err != nil {
		t.Fatalf("DeleteRunner failed: %v", err)
	}
}

func containsWorkload(workloads []*runnersv1.Workload, workloadID string) bool {
	for _, workload := range workloads {
		if workload.GetMeta().GetId() == workloadID {
			return true
		}
	}
	return false
}
