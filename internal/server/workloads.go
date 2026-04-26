package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	notificationsv1 "github.com/agynio/runners/.gen/go/agynio/api/notifications/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	workloadStatusStarting = "starting"
	workloadStatusRunning  = "running"
	workloadStatusStopping = "stopping"
	workloadStatusStopped  = "stopped"
	workloadStatusFailed   = "failed"

	workloadFailureReasonStartFailed     = "start_failed"
	workloadFailureReasonImagePullFailed = "image_pull_failed"
	workloadFailureReasonConfigInvalid   = "config_invalid"
	workloadFailureReasonCrashloop       = "crashloop"
	workloadFailureReasonRuntimeLost     = "runtime_lost"

	containerRoleMain    = "main"
	containerRoleSidecar = "sidecar"
	containerRoleInit    = "init"

	containerStatusRunning    = "running"
	containerStatusTerminated = "terminated"
	containerStatusWaiting    = "waiting"

	workloadColumns = `id, runner_id, thread_id, agent_id, organization_id, status, failure_reason, failure_message, containers, ziti_identity_id, allocated_cpu_millicores, allocated_ram_bytes, instance_id, last_activity_at, last_metering_sampled_at, removed_at, created_at, updated_at`
)

type workloadRecord struct {
	Meta                   entityMeta
	RunnerID               uuid.UUID
	ThreadID               uuid.UUID
	AgentID                uuid.UUID
	OrganizationID         uuid.UUID
	Status                 string
	FailureReason          *string
	FailureMessage         *string
	Containers             []containerRecord
	ZitiIdentityID         string
	AllocatedCPUMillicores int32
	AllocatedRAMBytes      int64
	InstanceID             *string
	LastActivityAt         time.Time
	RemovedAt              *time.Time
	LastMeteringAt         *time.Time
}

type workloadInsertInput struct {
	ID                     uuid.UUID
	RunnerID               uuid.UUID
	ThreadID               uuid.UUID
	AgentID                uuid.UUID
	OrganizationID         uuid.UUID
	Status                 string
	ContainersJSON         []byte
	ZitiIdentityID         string
	AllocatedCPUMillicores int32
	AllocatedRAMBytes      int64
}

type workloadUpdateInput struct {
	ID             uuid.UUID
	Status         *string
	FailureReason  *string
	FailureMessage *string
	ContainersJSON *[]byte
	InstanceID     *string
	RemovedAt      *time.Time
	LastMeteringAt *time.Time
}

type containerRecord struct {
	ContainerID  string     `json:"container_id"`
	Name         string     `json:"name"`
	Role         string     `json:"role"`
	Image        string     `json:"image"`
	Status       string     `json:"status"`
	Reason       *string    `json:"reason,omitempty"`
	Message      *string    `json:"message,omitempty"`
	ExitCode     *int32     `json:"exit_code,omitempty"`
	RestartCount int32      `json:"restart_count"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
}

func (s *Server) CreateWorkload(ctx context.Context, req *runnersv1.CreateWorkloadRequest) (*runnersv1.CreateWorkloadResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	runnerID, err := parseUUID(req.GetRunnerId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "runner_id: %v", err)
	}
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	agentID, err := parseUUID(req.GetAgentId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "agent_id: %v", err)
	}
	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}

	statusValue, err := workloadStatusToString(req.GetStatus())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
	}
	containers, err := containersFromProto(req.GetContainers())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "containers: %v", err)
	}
	containersJSON, err := json.Marshal(containers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal containers: %v", err)
	}

	workload, err := s.insertWorkload(ctx, workloadInsertInput{
		ID:                     id,
		RunnerID:               runnerID,
		ThreadID:               threadID,
		AgentID:                agentID,
		OrganizationID:         organizationID,
		Status:                 statusValue,
		ContainersJSON:         containersJSON,
		ZitiIdentityID:         strings.TrimSpace(req.GetZitiIdentityId()),
		AllocatedCPUMillicores: req.GetAllocatedCpuMillicores(),
		AllocatedRAMBytes:      req.GetAllocatedRamBytes(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.CreateWorkloadResponse{Workload: protoWorkload}, nil
}

func (s *Server) UpdateWorkload(ctx context.Context, req *runnersv1.UpdateWorkloadRequest) (*runnersv1.UpdateWorkloadResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}

	var statusValue *string
	if req.Status != nil {
		value, err := workloadStatusToString(req.GetStatus())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
		}
		statusValue = &value
	}

	var failureReason *string
	if req.FailureReason != nil {
		value, err := workloadFailureReasonToString(req.GetFailureReason())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failure_reason: %v", err)
		}
		failureReason = &value
	}

	var failureMessage *string
	if req.FailureMessage != nil {
		value := strings.TrimSpace(req.GetFailureMessage())
		failureMessage = &value
	}

	var containerRecords []containerRecord
	var containersJSON *[]byte
	if req.Containers != nil {
		containers, err := containersFromProto(req.GetContainers())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "containers: %v", err)
		}
		payload, err := json.Marshal(containers)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal containers: %v", err)
		}
		containerRecords = containers
		containersJSON = &payload
	}

	var instanceID *string
	if req.InstanceId != nil {
		trimmed := strings.TrimSpace(req.GetInstanceId())
		if trimmed == "" {
			return nil, status.Error(codes.InvalidArgument, "instance_id must not be empty")
		}
		instanceID = &trimmed
	}

	var removedAt *time.Time
	if req.RemovedAt != nil {
		if err := req.GetRemovedAt().CheckValid(); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "removed_at: %v", err)
		}
		value := req.GetRemovedAt().AsTime()
		removedAt = &value
	}

	var lastMeteringAt *time.Time
	if req.LastMeteringSampledAt != nil {
		if err := req.GetLastMeteringSampledAt().CheckValid(); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "last_metering_sampled_at: %v", err)
		}
		value := req.GetLastMeteringSampledAt().AsTime()
		lastMeteringAt = &value
	}

	if statusValue == nil && containersJSON == nil && instanceID == nil && removedAt == nil && lastMeteringAt == nil && failureReason == nil && failureMessage == nil {
		return nil, status.Error(codes.InvalidArgument, "at least one field must be provided")
	}

	var existingWorkload *workloadRecord
	if s.notificationsClient != nil && (statusValue != nil || containersJSON != nil || failureReason != nil || failureMessage != nil) {
		workload, err := s.getWorkloadByID(ctx, id)
		if err != nil {
			return nil, toStatusError(err)
		}
		existingWorkload = &workload
	}

	workload, err := s.updateWorkload(ctx, workloadUpdateInput{
		ID:             id,
		Status:         statusValue,
		FailureReason:  failureReason,
		FailureMessage: failureMessage,
		ContainersJSON: containersJSON,
		InstanceID:     instanceID,
		RemovedAt:      removedAt,
		LastMeteringAt: lastMeteringAt,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	statusChanged := existingWorkload != nil && statusValue != nil && *statusValue != existingWorkload.Status
	containersChanged := existingWorkload != nil && containersJSON != nil && !containersEqualByName(existingWorkload.Containers, containerRecords)
	failureReasonChanged := existingWorkload != nil && failureReason != nil && (existingWorkload.FailureReason == nil || *existingWorkload.FailureReason != *failureReason)
	failureMessageChanged := existingWorkload != nil && failureMessage != nil && (existingWorkload.FailureMessage == nil || *existingWorkload.FailureMessage != *failureMessage)
	if statusChanged || containersChanged || failureReasonChanged || failureMessageChanged {
		s.publishWorkloadUpdateNotifications(ctx, workload, statusChanged, containersChanged, failureReasonChanged || failureMessageChanged)
	}
	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.UpdateWorkloadResponse{Workload: protoWorkload}, nil
}

func (s *Server) publishWorkloadUpdateNotifications(ctx context.Context, workload workloadRecord, statusChanged, containersChanged, failureChanged bool) {
	if s.notificationsClient == nil {
		return
	}
	payloadFields := map[string]any{
		"workload_id": workload.Meta.ID.String(),
		"status":      workload.Status,
	}
	if workload.FailureReason != nil {
		payloadFields["failure_reason"] = *workload.FailureReason
	}
	if workload.FailureMessage != nil {
		payloadFields["failure_message"] = *workload.FailureMessage
	}
	payload, err := structpb.NewStruct(payloadFields)
	if err != nil {
		log.Printf("runners: build workload notification payload: %v", err)
		return
	}
	workloadRoom := fmt.Sprintf("workload:%s", workload.Meta.ID.String())
	updatedRooms := []string{
		fmt.Sprintf("organization:%s", workload.OrganizationID.String()),
		workloadRoom,
	}
	if statusChanged || containersChanged || failureChanged {
		s.publishWorkloadNotification(ctx, "workload.updated", updatedRooms, payload)
	}
	if statusChanged {
		s.publishWorkloadNotification(ctx, "workload.status_changed", []string{workloadRoom}, payload)
	}
}

func (s *Server) publishWorkloadNotification(ctx context.Context, event string, rooms []string, payload *structpb.Struct) {
	if s.notificationsClient == nil {
		return
	}
	_, err := s.notificationsClient.Publish(ctx, &notificationsv1.PublishRequest{
		Event:   event,
		Rooms:   rooms,
		Payload: payload,
		Source:  "runners",
	})
	if err != nil {
		log.Printf("runners: publish %s notification: %v", event, err)
	}
}

func (s *Server) UpdateWorkloadStatus(ctx context.Context, req *runnersv1.UpdateWorkloadStatusRequest) (*runnersv1.UpdateWorkloadStatusResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	statusValue, err := workloadStatusToString(req.GetStatus())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
	}
	containers, err := containersFromProto(req.GetContainers())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "containers: %v", err)
	}
	containersJSON, err := json.Marshal(containers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal containers: %v", err)
	}

	workload, err := s.updateWorkload(ctx, workloadUpdateInput{
		ID:             id,
		Status:         &statusValue,
		ContainersJSON: &containersJSON,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.UpdateWorkloadStatusResponse{Workload: protoWorkload}, nil
}

func (s *Server) TouchWorkload(ctx context.Context, req *runnersv1.TouchWorkloadRequest) (*runnersv1.TouchWorkloadResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	if err := s.touchWorkloadForAgent(ctx, id, callerID); err != nil {
		return nil, toStatusError(err)
	}
	return &runnersv1.TouchWorkloadResponse{}, nil
}

func (s *Server) DeleteWorkload(ctx context.Context, req *runnersv1.DeleteWorkloadRequest) (*runnersv1.DeleteWorkloadResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	if err := s.softDeleteWorkload(ctx, id); err != nil {
		return nil, toStatusError(err)
	}
	return &runnersv1.DeleteWorkloadResponse{}, nil
}

func (s *Server) GetWorkload(ctx context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	workload, err := s.getWorkloadByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	if err := s.requireOrgMember(ctx, callerID, workload.OrganizationID); err != nil {
		return nil, err
	}
	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.GetWorkloadResponse{Workload: protoWorkload}, nil
}

func (s *Server) ListWorkloadsByThread(ctx context.Context, req *runnersv1.ListWorkloadsByThreadRequest) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	var agentID *uuid.UUID
	if req.AgentId != nil {
		parsed, err := parseUUID(req.GetAgentId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "agent_id: %v", err)
		}
		agentID = &parsed
	}
	statuses, err := workloadStatusesToStrings(req.GetStatuses())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "statuses: %v", err)
	}
	workloads, nextToken, err := s.listWorkloadsByThread(ctx, threadID, agentID, statuses, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list workloads: %v", err)
	}
	memberCache := map[uuid.UUID]bool{}
	filtered := make([]workloadRecord, 0, len(workloads))
	for _, workload := range workloads {
		allowed, err := s.memberAllowed(ctx, callerID, workload.OrganizationID, memberCache)
		if err != nil {
			return nil, err
		}
		if allowed {
			filtered = append(filtered, workload)
		}
	}
	protoWorkloads, err := toProtoWorkloadList(filtered)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workloads: %v", err)
	}
	return &runnersv1.ListWorkloadsByThreadResponse{Workloads: protoWorkloads, NextPageToken: nextToken}, nil
}

func (s *Server) ListWorkloads(ctx context.Context, req *runnersv1.ListWorkloadsRequest) (*runnersv1.ListWorkloadsResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	statuses, err := workloadStatusesToStrings(req.GetStatuses())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "statuses: %v", err)
	}
	var organizationID *uuid.UUID
	if req.OrganizationId != nil {
		parsed, err := parseUUID(req.GetOrganizationId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		organizationID = &parsed
	}
	if organizationID != nil {
		if err := s.requireOrgMember(ctx, callerID, *organizationID); err != nil {
			return nil, err
		}
	}

	var runnerID *uuid.UUID
	if req.RunnerId != nil {
		parsed, err := parseUUID(req.GetRunnerId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "runner_id: %v", err)
		}
		runnerID = &parsed
	}

	pendingSample := req.PendingSample != nil && req.GetPendingSample()

	workloads, nextToken, err := s.listWorkloads(ctx, statuses, organizationID, runnerID, pendingSample, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list workloads: %v", err)
	}
	if organizationID == nil {
		memberCache := map[uuid.UUID]bool{}
		filtered := make([]workloadRecord, 0, len(workloads))
		for _, workload := range workloads {
			allowed, err := s.memberAllowed(ctx, callerID, workload.OrganizationID, memberCache)
			if err != nil {
				return nil, err
			}
			if allowed {
				filtered = append(filtered, workload)
			}
		}
		workloads = filtered
	}
	protoWorkloads, err := toProtoWorkloadList(workloads)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workloads: %v", err)
	}
	return &runnersv1.ListWorkloadsResponse{Workloads: protoWorkloads, NextPageToken: nextToken}, nil
}

func (s *Server) BatchUpdateWorkloadSampledAt(ctx context.Context, req *runnersv1.BatchUpdateWorkloadSampledAtRequest) (*runnersv1.BatchUpdateWorkloadSampledAtResponse, error) {
	entries := req.GetEntries()
	if len(entries) == 0 {
		return &runnersv1.BatchUpdateWorkloadSampledAtResponse{}, nil
	}
	updates, err := parseSampledAtEntries(entries)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.batchUpdateWorkloadSampledAt(ctx, updates); err != nil {
		return nil, status.Errorf(codes.Internal, "batch update workloads: %v", err)
	}
	return &runnersv1.BatchUpdateWorkloadSampledAtResponse{}, nil
}

func (s *Server) insertWorkload(ctx context.Context, input workloadInsertInput) (workloadRecord, error) {
	containersJSON := input.ContainersJSON
	if len(containersJSON) == 0 {
		containersJSON = []byte("[]")
	}
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`INSERT INTO workloads (id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id, allocated_cpu_millicores, allocated_ram_bytes, last_activity_at, created_at, updated_at)
	    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW(), NOW())
	    RETURNING %s`, workloadColumns),
		input.ID,
		input.RunnerID,
		input.ThreadID,
		input.AgentID,
		input.OrganizationID,
		input.Status,
		containersJSON,
		input.ZitiIdentityID,
		input.AllocatedCPUMillicores,
		input.AllocatedRAMBytes,
	)
	workload, err := scanWorkload(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "23505" {
				return workloadRecord{}, AlreadyExists("workload")
			}
			if pgErr.Code == "23503" {
				return workloadRecord{}, NotFound("runner")
			}
		}
		return workloadRecord{}, err
	}
	return workload, nil
}

func (s *Server) updateWorkload(ctx context.Context, input workloadUpdateInput) (workloadRecord, error) {
	clauses := make([]string, 0, 6)
	args := make([]any, 0, 6)

	if input.Status != nil {
		addUpdateClause(&clauses, &args, "status", *input.Status)
	}
	if input.FailureReason != nil {
		addUpdateClause(&clauses, &args, "failure_reason", *input.FailureReason)
	}
	if input.FailureMessage != nil {
		addUpdateClause(&clauses, &args, "failure_message", *input.FailureMessage)
	}
	if input.ContainersJSON != nil {
		payload := *input.ContainersJSON
		if len(payload) == 0 {
			payload = []byte("[]")
		}
		addUpdateClause(&clauses, &args, "containers", payload)
	}
	if input.InstanceID != nil {
		addUpdateClause(&clauses, &args, "instance_id", *input.InstanceID)
	}
	if input.RemovedAt != nil {
		addUpdateClause(&clauses, &args, "removed_at", *input.RemovedAt)
	}
	if input.LastMeteringAt != nil {
		addUpdateClause(&clauses, &args, "last_metering_sampled_at", *input.LastMeteringAt)
	}
	query, args := buildUpdateQuery("workloads", workloadColumns, clauses, args, input.ID)
	row := s.pool.QueryRow(ctx, query, args...)
	workload, err := scanWorkload(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workloadRecord{}, NotFound("workload")
		}
		return workloadRecord{}, err
	}
	return workload, nil
}

func (s *Server) getWorkloadByID(ctx context.Context, id uuid.UUID) (workloadRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT %s FROM workloads WHERE id = $1`, workloadColumns),
		id,
	)
	workload, err := scanWorkload(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return workloadRecord{}, NotFound("workload")
		}
		return workloadRecord{}, err
	}
	return workload, nil
}

func (s *Server) softDeleteWorkload(ctx context.Context, id uuid.UUID) error {
	statusValue := workloadStatusStopped
	clauses := make([]string, 0, 2)
	args := make([]any, 0, 2)
	addUpdateClause(&clauses, &args, "status", statusValue)
	clauses = append(clauses, "removed_at = NOW()")
	query, args := buildUpdateQuery("workloads", workloadColumns, clauses, args, id)
	row := s.pool.QueryRow(ctx, query, args...)
	_, err := scanWorkload(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return NotFound("workload")
		}
		return err
	}
	return nil
}

func (s *Server) touchWorkloadForAgent(ctx context.Context, id uuid.UUID, agentID uuid.UUID) error {
	result, err := s.pool.Exec(ctx, `UPDATE workloads SET last_activity_at = NOW(), updated_at = NOW() WHERE id = $1 AND agent_id = $2`, id, agentID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		_, err := s.getWorkloadByID(ctx, id)
		if err != nil {
			return err
		}
		return PermissionDenied()
	}
	return nil
}

func (s *Server) listWorkloadsByThread(ctx context.Context, threadID uuid.UUID, agentID *uuid.UUID, statuses []string, pageSize int32, pageToken string) ([]workloadRecord, string, error) {
	limit := normalizePageSize(pageSize)
	clauses := []string{"thread_id = $1"}
	args := []any{threadID}

	if agentID != nil {
		clauses = append(clauses, fmt.Sprintf("agent_id = $%d", len(args)+1))
		args = append(args, *agentID)
	}
	if len(statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](statuses))
	}

	if pageToken != "" {
		cursor, err := decodeWorkloadCursor(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		clauses = append(clauses, fmt.Sprintf("(created_at < $%d OR (created_at = $%d AND id < $%d))", len(args)+1, len(args)+1, len(args)+2))
		args = append(args, cursor.CreatedAt, cursor.ID)
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM workloads", workloadColumns))
	query.WriteString(" WHERE ")
	query.WriteString(strings.Join(clauses, " AND "))
	query.WriteString(fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	workloads := make([]workloadRecord, 0, limit)
	var (
		lastID  uuid.UUID
		lastAt  time.Time
		hasMore bool
	)
	for rows.Next() {
		if int32(len(workloads)) == limit {
			hasMore = true
			break
		}
		workload, err := scanWorkload(rows)
		if err != nil {
			return nil, "", err
		}
		workloads = append(workloads, workload)
		lastID = workload.Meta.ID
		lastAt = workload.Meta.CreatedAt
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		nextToken = encodeWorkloadCursor(lastAt, lastID)
	}
	return workloads, nextToken, nil
}

type workloadCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

func encodeWorkloadCursor(createdAt time.Time, id uuid.UUID) string {
	payload := fmt.Sprintf("%s|%s", createdAt.UTC().Format(time.RFC3339Nano), id.String())
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

func decodeWorkloadCursor(token string) (workloadCursor, error) {
	if token == "" {
		return workloadCursor{}, errors.New("empty token")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return workloadCursor{}, fmt.Errorf("decode token: %w", err)
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 {
		return workloadCursor{}, errors.New("invalid token")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return workloadCursor{}, fmt.Errorf("parse created_at: %w", err)
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return workloadCursor{}, fmt.Errorf("parse id: %w", err)
	}
	return workloadCursor{CreatedAt: createdAt, ID: id}, nil
}

func (s *Server) listWorkloads(ctx context.Context, statuses []string, organizationID *uuid.UUID, runnerID *uuid.UUID, pendingSample bool, pageSize int32, pageToken string) ([]workloadRecord, string, error) {
	limit := normalizePageSize(pageSize)
	query, args, err := buildListQuery(listQueryInput{
		Table:          "workloads",
		Columns:        workloadColumns,
		Statuses:       statuses,
		OrganizationID: organizationID,
		RunnerID:       runnerID,
		PendingSample:  pendingSample,
		PageToken:      pageToken,
		Limit:          limit,
	})
	if err != nil {
		return nil, "", err
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	workloads := make([]workloadRecord, 0, limit)
	var (
		lastID  uuid.UUID
		hasMore bool
	)
	for rows.Next() {
		if int32(len(workloads)) == limit {
			hasMore = true
			break
		}
		workload, err := scanWorkload(rows)
		if err != nil {
			return nil, "", err
		}
		workloads = append(workloads, workload)
		lastID = workload.Meta.ID
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		nextToken = encodePageToken(lastID)
	}
	return workloads, nextToken, nil
}

func (s *Server) batchUpdateWorkloadSampledAt(ctx context.Context, entries []sampledAtEntry) error {
	query, args := buildBatchSampledAtUpdateQuery("workloads", entries)
	if _, err := s.pool.Exec(ctx, query, args...); err != nil {
		return err
	}
	return nil
}

func scanWorkload(row pgx.Row) (workloadRecord, error) {
	var (
		workload       workloadRecord
		containersData []byte
		failureReason  pgtype.Text
		failureMessage pgtype.Text
		instanceID     pgtype.Text
		removedAt      pgtype.Timestamptz
		lastMeteringAt pgtype.Timestamptz
	)
	if err := row.Scan(
		&workload.Meta.ID,
		&workload.RunnerID,
		&workload.ThreadID,
		&workload.AgentID,
		&workload.OrganizationID,
		&workload.Status,
		&failureReason,
		&failureMessage,
		&containersData,
		&workload.ZitiIdentityID,
		&workload.AllocatedCPUMillicores,
		&workload.AllocatedRAMBytes,
		&instanceID,
		&workload.LastActivityAt,
		&lastMeteringAt,
		&removedAt,
		&workload.Meta.CreatedAt,
		&workload.Meta.UpdatedAt,
	); err != nil {
		return workloadRecord{}, err
	}
	if len(containersData) == 0 {
		containersData = []byte("[]")
	}
	if err := json.Unmarshal(containersData, &workload.Containers); err != nil {
		return workloadRecord{}, err
	}
	if failureReason.Valid {
		value := failureReason.String
		workload.FailureReason = &value
	}
	if failureMessage.Valid {
		value := failureMessage.String
		workload.FailureMessage = &value
	}
	if instanceID.Valid {
		value := instanceID.String
		workload.InstanceID = &value
	}
	if removedAt.Valid {
		value := removedAt.Time
		workload.RemovedAt = &value
	}
	if lastMeteringAt.Valid {
		value := lastMeteringAt.Time
		workload.LastMeteringAt = &value
	}
	return workload, nil
}

func toProtoWorkload(record workloadRecord) (*runnersv1.Workload, error) {
	statusValue, err := workloadStatusFromString(record.Status)
	if err != nil {
		return nil, err
	}
	containers, err := containersToProto(record.Containers)
	if err != nil {
		return nil, err
	}
	protoWorkload := &runnersv1.Workload{
		Meta:                   toProtoEntityMeta(record.Meta),
		RunnerId:               record.RunnerID.String(),
		ThreadId:               record.ThreadID.String(),
		AgentId:                record.AgentID.String(),
		OrganizationId:         record.OrganizationID.String(),
		Status:                 statusValue,
		Containers:             containers,
		ZitiIdentityId:         record.ZitiIdentityID,
		LastActivityAt:         timestamppb.New(record.LastActivityAt),
		AllocatedCpuMillicores: record.AllocatedCPUMillicores,
		AllocatedRamBytes:      record.AllocatedRAMBytes,
	}
	if record.InstanceID != nil {
		protoWorkload.InstanceId = record.InstanceID
	}
	if record.RemovedAt != nil {
		protoWorkload.RemovedAt = timestamppb.New(*record.RemovedAt)
	}
	if record.LastMeteringAt != nil {
		protoWorkload.LastMeteringSampledAt = timestamppb.New(*record.LastMeteringAt)
	}
	if record.FailureReason != nil {
		failureReason, err := workloadFailureReasonFromString(*record.FailureReason)
		if err != nil {
			return nil, err
		}
		protoWorkload.FailureReason = &failureReason
	}
	if record.FailureMessage != nil {
		protoWorkload.FailureMessage = record.FailureMessage
	}
	return protoWorkload, nil
}

func toProtoWorkloadList(records []workloadRecord) ([]*runnersv1.Workload, error) {
	workloads := make([]*runnersv1.Workload, 0, len(records))
	for _, record := range records {
		workload, err := toProtoWorkload(record)
		if err != nil {
			return nil, err
		}
		workloads = append(workloads, workload)
	}
	return workloads, nil
}

func containersFromProto(containers []*runnersv1.Container) ([]containerRecord, error) {
	if len(containers) == 0 {
		return []containerRecord{}, nil
	}
	records := make([]containerRecord, len(containers))
	for i, container := range containers {
		if container == nil {
			return nil, fmt.Errorf("container %d is nil", i)
		}
		role, err := containerRoleToString(container.GetRole())
		if err != nil {
			return nil, err
		}
		statusValue, err := containerStatusToString(container.GetStatus())
		if err != nil {
			return nil, err
		}
		name := container.GetName()
		startedAt, err := containerTimestamp(container.GetStartedAt(), name, "started_at")
		if err != nil {
			return nil, err
		}
		finishedAt, err := containerTimestamp(container.GetFinishedAt(), name, "finished_at")
		if err != nil {
			return nil, err
		}
		var reason *string
		if container.Reason != nil {
			value := container.GetReason()
			reason = &value
		}
		var message *string
		if container.Message != nil {
			value := container.GetMessage()
			message = &value
		}
		var exitCode *int32
		if container.ExitCode != nil {
			value := container.GetExitCode()
			exitCode = &value
		}
		records[i] = containerRecord{
			ContainerID:  container.GetContainerId(),
			Name:         name,
			Role:         role,
			Image:        container.GetImage(),
			Status:       statusValue,
			Reason:       reason,
			Message:      message,
			ExitCode:     exitCode,
			RestartCount: container.GetRestartCount(),
			StartedAt:    startedAt,
			FinishedAt:   finishedAt,
		}
	}
	return records, nil
}

func containersToProto(records []containerRecord) ([]*runnersv1.Container, error) {
	if len(records) == 0 {
		return []*runnersv1.Container{}, nil
	}
	containers := make([]*runnersv1.Container, len(records))
	for i, record := range records {
		role, err := containerRoleFromString(record.Role)
		if err != nil {
			return nil, err
		}
		statusValue, err := containerStatusFromString(record.Status)
		if err != nil {
			return nil, err
		}
		containers[i] = &runnersv1.Container{
			ContainerId:  record.ContainerID,
			Name:         record.Name,
			Role:         role,
			Image:        record.Image,
			Status:       statusValue,
			Reason:       record.Reason,
			Message:      record.Message,
			ExitCode:     record.ExitCode,
			RestartCount: record.RestartCount,
			StartedAt:    timestampProto(record.StartedAt),
			FinishedAt:   timestampProto(record.FinishedAt),
		}
	}
	return containers, nil
}

func containersEqualByName(existing, updated []containerRecord) bool {
	if len(existing) != len(updated) {
		return false
	}
	byName := make(map[string]containerRecord, len(existing))
	for _, container := range existing {
		if _, ok := byName[container.Name]; ok {
			return false
		}
		byName[container.Name] = container
	}
	for _, container := range updated {
		current, ok := byName[container.Name]
		if !ok {
			return false
		}
		if !containerRecordEqual(current, container) {
			return false
		}
	}
	return true
}

func containerRecordEqual(left, right containerRecord) bool {
	if left.ContainerID != right.ContainerID ||
		left.Name != right.Name ||
		left.Role != right.Role ||
		left.Image != right.Image ||
		left.Status != right.Status ||
		left.RestartCount != right.RestartCount {
		return false
	}
	if !optionalStringEqual(left.Reason, right.Reason) || !optionalStringEqual(left.Message, right.Message) {
		return false
	}
	if !optionalInt32Equal(left.ExitCode, right.ExitCode) {
		return false
	}
	if !optionalTimeEqual(left.StartedAt, right.StartedAt) || !optionalTimeEqual(left.FinishedAt, right.FinishedAt) {
		return false
	}
	return true
}

func optionalStringEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func optionalInt32Equal(left, right *int32) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func optionalTimeEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Equal(*right)
}

func containerTimestamp(value *timestamppb.Timestamp, name, field string) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	if err := value.CheckValid(); err != nil {
		if name == "" {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
		return nil, fmt.Errorf("container %s %s: %w", name, field, err)
	}
	timestamp := value.AsTime()
	return &timestamp, nil
}

func timestampProto(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(*value)
}

func workloadStatusToString(status runnersv1.WorkloadStatus) (string, error) {
	switch status {
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING:
		return workloadStatusStarting, nil
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING:
		return workloadStatusRunning, nil
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING:
		return workloadStatusStopping, nil
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED:
		return workloadStatusStopped, nil
	case runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED:
		return workloadStatusFailed, nil
	default:
		return "", fmt.Errorf("invalid workload status: %s", status.String())
	}
}

func workloadFailureReasonToString(reason runnersv1.WorkloadFailureReason) (string, error) {
	switch reason {
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED:
		return workloadFailureReasonStartFailed, nil
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED:
		return workloadFailureReasonImagePullFailed, nil
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID:
		return workloadFailureReasonConfigInvalid, nil
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP:
		return workloadFailureReasonCrashloop, nil
	case runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST:
		return workloadFailureReasonRuntimeLost, nil
	default:
		return "", fmt.Errorf("invalid workload failure reason: %s", reason.String())
	}
}

func workloadStatusFromString(value string) (runnersv1.WorkloadStatus, error) {
	switch value {
	case workloadStatusStarting:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING, nil
	case workloadStatusRunning:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING, nil
	case workloadStatusStopping:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING, nil
	case workloadStatusStopped:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED, nil
	case workloadStatusFailed:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED, nil
	default:
		return runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, fmt.Errorf("invalid workload status: %s", value)
	}
}

func workloadFailureReasonFromString(value string) (runnersv1.WorkloadFailureReason, error) {
	switch value {
	case workloadFailureReasonStartFailed:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_START_FAILED, nil
	case workloadFailureReasonImagePullFailed:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_IMAGE_PULL_FAILED, nil
	case workloadFailureReasonConfigInvalid:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CONFIG_INVALID, nil
	case workloadFailureReasonCrashloop:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_CRASHLOOP, nil
	case workloadFailureReasonRuntimeLost:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_RUNTIME_LOST, nil
	default:
		return runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_UNSPECIFIED, fmt.Errorf("invalid workload failure reason: %s", value)
	}
}

func workloadStatusesToStrings(statuses []runnersv1.WorkloadStatus) ([]string, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	values := make([]string, len(statuses))
	for i, statusValue := range statuses {
		value, err := workloadStatusToString(statusValue)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func containerRoleToString(role runnersv1.ContainerRole) (string, error) {
	switch role {
	case runnersv1.ContainerRole_CONTAINER_ROLE_MAIN:
		return containerRoleMain, nil
	case runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR:
		return containerRoleSidecar, nil
	case runnersv1.ContainerRole_CONTAINER_ROLE_INIT:
		return containerRoleInit, nil
	default:
		return "", fmt.Errorf("invalid container role: %s", role.String())
	}
}

func containerRoleFromString(value string) (runnersv1.ContainerRole, error) {
	switch value {
	case containerRoleMain:
		return runnersv1.ContainerRole_CONTAINER_ROLE_MAIN, nil
	case containerRoleSidecar:
		return runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR, nil
	case containerRoleInit:
		return runnersv1.ContainerRole_CONTAINER_ROLE_INIT, nil
	default:
		return runnersv1.ContainerRole_CONTAINER_ROLE_UNSPECIFIED, fmt.Errorf("invalid container role: %s", value)
	}
}

func containerStatusToString(status runnersv1.ContainerStatus) (string, error) {
	switch status {
	case runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING:
		return containerStatusRunning, nil
	case runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED:
		return containerStatusTerminated, nil
	case runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING:
		return containerStatusWaiting, nil
	default:
		return "", fmt.Errorf("invalid container status: %s", status.String())
	}
}

func containerStatusFromString(value string) (runnersv1.ContainerStatus, error) {
	switch value {
	case containerStatusRunning:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING, nil
	case containerStatusTerminated:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED, nil
	case containerStatusWaiting:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING, nil
	default:
		return runnersv1.ContainerStatus_CONTAINER_STATUS_UNSPECIFIED, fmt.Errorf("invalid container status: %s", value)
	}
}
