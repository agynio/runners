package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	workloadStatusStarting = "starting"
	workloadStatusRunning  = "running"
	workloadStatusStopping = "stopping"
	workloadStatusStopped  = "stopped"
	workloadStatusFailed   = "failed"

	containerRoleMain    = "main"
	containerRoleSidecar = "sidecar"
	containerRoleInit    = "init"

	containerStatusRunning    = "running"
	containerStatusTerminated = "terminated"
	containerStatusWaiting    = "waiting"

	workloadColumns = `id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id, instance_id, last_activity_at, last_metering_sampled_at, removed_at, created_at, updated_at`
)

type workloadRecord struct {
	Meta           entityMeta
	RunnerID       uuid.UUID
	ThreadID       uuid.UUID
	AgentID        uuid.UUID
	OrganizationID uuid.UUID
	Status         string
	Containers     []containerRecord
	ZitiIdentityID string
	InstanceID     *string
	LastActivityAt time.Time
	RemovedAt      *time.Time
	LastMeteringAt *time.Time
}

type workloadInsertInput struct {
	ID             uuid.UUID
	RunnerID       uuid.UUID
	ThreadID       uuid.UUID
	AgentID        uuid.UUID
	OrganizationID uuid.UUID
	Status         string
	ContainersJSON []byte
	ZitiIdentityID string
}

type workloadUpdateInput struct {
	ID             uuid.UUID
	Status         *string
	ContainersJSON *[]byte
	InstanceID     *string
	RemovedAt      *time.Time
	LastMeteringAt *time.Time
}

type containerRecord struct {
	ContainerID string `json:"container_id"`
	Name        string `json:"name"`
	Role        string `json:"role"`
	Image       string `json:"image"`
	Status      string `json:"status"`
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
		ID:             id,
		RunnerID:       runnerID,
		ThreadID:       threadID,
		AgentID:        agentID,
		OrganizationID: organizationID,
		Status:         statusValue,
		ContainersJSON: containersJSON,
		ZitiIdentityID: strings.TrimSpace(req.GetZitiIdentityId()),
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

	if statusValue == nil && containersJSON == nil && instanceID == nil && removedAt == nil && lastMeteringAt == nil {
		return nil, status.Error(codes.InvalidArgument, "at least one field must be provided")
	}

	workload, err := s.updateWorkload(ctx, workloadUpdateInput{
		ID:             id,
		Status:         statusValue,
		ContainersJSON: containersJSON,
		InstanceID:     instanceID,
		RemovedAt:      removedAt,
		LastMeteringAt: lastMeteringAt,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.UpdateWorkloadResponse{Workload: protoWorkload}, nil
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
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	if err := s.touchWorkload(ctx, id); err != nil {
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
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	workload, err := s.getWorkloadByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.GetWorkloadResponse{Workload: protoWorkload}, nil
}

func (s *Server) ListWorkloadsByThread(ctx context.Context, req *runnersv1.ListWorkloadsByThreadRequest) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	workloads, nextToken, err := s.listWorkloadsByThread(ctx, threadID, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list workloads: %v", err)
	}
	protoWorkloads, err := toProtoWorkloadList(workloads)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workloads: %v", err)
	}
	return &runnersv1.ListWorkloadsByThreadResponse{Workloads: protoWorkloads, NextPageToken: nextToken}, nil
}

func (s *Server) ListWorkloads(ctx context.Context, req *runnersv1.ListWorkloadsRequest) (*runnersv1.ListWorkloadsResponse, error) {
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
		fmt.Sprintf(`INSERT INTO workloads (id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id, last_activity_at, created_at, updated_at)
	    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW(), NOW())
	    RETURNING %s`, workloadColumns),
		input.ID,
		input.RunnerID,
		input.ThreadID,
		input.AgentID,
		input.OrganizationID,
		input.Status,
		containersJSON,
		input.ZitiIdentityID,
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

func (s *Server) touchWorkload(ctx context.Context, id uuid.UUID) error {
	result, err := s.pool.Exec(ctx, `UPDATE workloads SET last_activity_at = NOW(), updated_at = NOW() WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return NotFound("workload")
	}
	return nil
}

func (s *Server) listWorkloadsByThread(ctx context.Context, threadID uuid.UUID, pageSize int32, pageToken string) ([]workloadRecord, string, error) {
	limit := normalizePageSize(pageSize)
	clauses := []string{fmt.Sprintf("thread_id = $1")}
	args := []any{threadID}

	if pageToken != "" {
		afterID, err := decodePageToken(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		clauses = append(clauses, fmt.Sprintf("id > $%d", len(args)+1))
		args = append(args, afterID)
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM workloads", workloadColumns))
	query.WriteString(" WHERE ")
	query.WriteString(strings.Join(clauses, " AND "))
	query.WriteString(fmt.Sprintf(" ORDER BY id ASC LIMIT $%d", len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
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
		&containersData,
		&workload.ZitiIdentityID,
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
		Meta:           toProtoEntityMeta(record.Meta),
		RunnerId:       record.RunnerID.String(),
		ThreadId:       record.ThreadID.String(),
		AgentId:        record.AgentID.String(),
		OrganizationId: record.OrganizationID.String(),
		Status:         statusValue,
		Containers:     containers,
		ZitiIdentityId: record.ZitiIdentityID,
		LastActivityAt: timestamppb.New(record.LastActivityAt),
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
		role, err := containerRoleToString(container.GetRole())
		if err != nil {
			return nil, err
		}
		statusValue, err := containerStatusToString(container.GetStatus())
		if err != nil {
			return nil, err
		}
		records[i] = containerRecord{
			ContainerID: container.GetContainerId(),
			Name:        container.GetName(),
			Role:        role,
			Image:       container.GetImage(),
			Status:      statusValue,
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
			ContainerId: record.ContainerID,
			Name:        record.Name,
			Role:        role,
			Image:       record.Image,
			Status:      statusValue,
		}
	}
	return containers, nil
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
