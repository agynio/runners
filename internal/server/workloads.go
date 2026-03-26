package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	workloadColumns = `id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id, created_at, updated_at`
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

	workload, err := s.updateWorkloadStatus(ctx, id, statusValue, containersJSON)
	if err != nil {
		return nil, toStatusError(err)
	}
	protoWorkload, err := toProtoWorkload(workload)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert workload: %v", err)
	}
	return &runnersv1.UpdateWorkloadStatusResponse{Workload: protoWorkload}, nil
}

func (s *Server) DeleteWorkload(ctx context.Context, req *runnersv1.DeleteWorkloadRequest) (*runnersv1.DeleteWorkloadResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	if err := s.deleteWorkload(ctx, id); err != nil {
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
	workloads, nextToken, err := s.listWorkloads(ctx, statuses, req.GetPageSize(), req.GetPageToken())
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

func (s *Server) insertWorkload(ctx context.Context, input workloadInsertInput) (workloadRecord, error) {
	containersJSON := input.ContainersJSON
	if len(containersJSON) == 0 {
		containersJSON = []byte("[]")
	}
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`INSERT INTO workloads (id, runner_id, thread_id, agent_id, organization_id, status, containers, ziti_identity_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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

func (s *Server) updateWorkloadStatus(ctx context.Context, id uuid.UUID, statusValue string, containersJSON []byte) (workloadRecord, error) {
	if len(containersJSON) == 0 {
		containersJSON = []byte("[]")
	}
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`UPDATE workloads SET status = $1, containers = $2, updated_at = NOW() WHERE id = $3 RETURNING %s`, workloadColumns),
		statusValue,
		containersJSON,
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

func (s *Server) deleteWorkload(ctx context.Context, id uuid.UUID) error {
	result, err := s.pool.Exec(ctx, `DELETE FROM workloads WHERE id = $1`, id)
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

func (s *Server) listWorkloads(ctx context.Context, statuses []string, pageSize int32, pageToken string) ([]workloadRecord, string, error) {
	limit := normalizePageSize(pageSize)

	var (
		clauses []string
		args    []any
	)
	if len(statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](statuses))
	}
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
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}
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

func scanWorkload(row pgx.Row) (workloadRecord, error) {
	var (
		workload       workloadRecord
		containersData []byte
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
	return &runnersv1.Workload{
		Meta:           toProtoEntityMeta(record.Meta),
		RunnerId:       record.RunnerID.String(),
		ThreadId:       record.ThreadID.String(),
		AgentId:        record.AgentID.String(),
		OrganizationId: record.OrganizationID.String(),
		Status:         statusValue,
		Containers:     containers,
		ZitiIdentityId: record.ZitiIdentityID,
	}, nil
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
