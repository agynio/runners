//go:build e2e

package e2e

import (
	"context"
	"reflect"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	_, err = runnerClient.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound after deleting workload, got %v", err)
	}

	if _, err := runnerClient.DeleteRunner(ctx, &runnersv1.DeleteRunnerRequest{Id: runnerID}); err != nil {
		t.Fatalf("DeleteRunner failed: %v", err)
	}
}

func TestUpdateRunner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	registerLabels := map[string]string{
		"a": "1",
		"b": "2",
	}
	registerResp, err := runnerClient.RegisterRunner(ctx, &runnersv1.RegisterRunnerRequest{
		Name:   "update-runner",
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

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout)
		defer cleanupCancel()
		_, _ = runnerClient.DeleteRunner(cleanupCtx, &runnersv1.DeleteRunnerRequest{Id: runnerID})
	})

	updatedName := "update-runner-name"
	updateResp, err := runnerClient.UpdateRunner(ctx, &runnersv1.UpdateRunnerRequest{Id: runnerID, Name: &updatedName})
	if err != nil {
		t.Fatalf("UpdateRunner name failed: %v", err)
	}
	if updateResp.GetRunner().GetName() != updatedName {
		t.Fatalf("UpdateRunner name did not update")
	}
	if !reflect.DeepEqual(updateResp.GetRunner().GetLabels(), registerLabels) {
		t.Fatalf("UpdateRunner name changed labels unexpectedly")
	}

	labelsOnly := map[string]string{"c": "3"}
	updateResp, err = runnerClient.UpdateRunner(ctx, &runnersv1.UpdateRunnerRequest{Id: runnerID, Labels: labelsOnly})
	if err != nil {
		t.Fatalf("UpdateRunner labels failed: %v", err)
	}
	if updateResp.GetRunner().GetName() != updatedName {
		t.Fatalf("UpdateRunner labels changed name unexpectedly")
	}
	if !reflect.DeepEqual(updateResp.GetRunner().GetLabels(), labelsOnly) {
		t.Fatalf("UpdateRunner labels did not replace labels")
	}

	updatedBothName := "update-runner-both"
	updatedBothLabels := map[string]string{"env": "prod"}
	updateResp, err = runnerClient.UpdateRunner(ctx, &runnersv1.UpdateRunnerRequest{
		Id:     runnerID,
		Name:   &updatedBothName,
		Labels: updatedBothLabels,
	})
	if err != nil {
		t.Fatalf("UpdateRunner name+labels failed: %v", err)
	}
	if updateResp.GetRunner().GetName() != updatedBothName {
		t.Fatalf("UpdateRunner name+labels did not update name")
	}
	if !reflect.DeepEqual(updateResp.GetRunner().GetLabels(), updatedBothLabels) {
		t.Fatalf("UpdateRunner name+labels did not update labels")
	}

	_, err = runnerClient.UpdateRunner(ctx, &runnersv1.UpdateRunnerRequest{Id: runnerID, Labels: map[string]string{}})
	if err == nil {
		t.Fatal("expected InvalidArgument for empty labels (proto3 empty map omitted)")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for empty labels, got %v", err)
	}
	if status.Convert(err).Message() != "at least one field must be provided" {
		t.Fatalf("unexpected error message for empty labels: %v", status.Convert(err).Message())
	}

	missingName := "missing"
	_, err = runnerClient.UpdateRunner(ctx, &runnersv1.UpdateRunnerRequest{Id: uuid.NewString(), Name: &missingName})
	if err == nil {
		t.Fatal("expected NotFound for missing runner")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for missing runner, got %v", err)
	}

	invalidName := "invalid"
	_, err = runnerClient.UpdateRunner(ctx, &runnersv1.UpdateRunnerRequest{Id: "", Name: &invalidName})
	if err == nil {
		t.Fatal("expected InvalidArgument for empty id")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for empty id, got %v", err)
	}

	emptyName := "   "
	_, err = runnerClient.UpdateRunner(ctx, &runnersv1.UpdateRunnerRequest{Id: runnerID, Name: &emptyName})
	if err == nil {
		t.Fatal("expected InvalidArgument for empty name")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for empty name, got %v", err)
	}
	if status.Convert(err).Message() != "name must not be empty" {
		t.Fatalf("unexpected error message for empty name: %v", status.Convert(err).Message())
	}

	_, err = runnerClient.UpdateRunner(ctx, &runnersv1.UpdateRunnerRequest{Id: runnerID})
	if err == nil {
		t.Fatal("expected InvalidArgument for missing fields")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for missing fields, got %v", err)
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
