package server

import (
	"context"
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
	volumeStatusProvisioning = "provisioning"
	volumeStatusActive       = "active"
	volumeStatusDeprovision  = "deprovisioning"
	volumeStatusDeleted      = "deleted"
	volumeStatusFailed       = "failed"

	volumeColumns = `id, instance_id, volume_id, thread_id, runner_id, agent_id, organization_id, size_gb, status, removed_at, last_metering_sampled_at, created_at, updated_at`
)

type volumeRecord struct {
	Meta           entityMeta
	InstanceID     *string
	VolumeID       uuid.UUID
	ThreadID       uuid.UUID
	RunnerID       uuid.UUID
	AgentID        uuid.UUID
	OrganizationID uuid.UUID
	SizeGB         string
	Status         string
	RemovedAt      *time.Time
	LastMeteringAt *time.Time
}

type volumeInsertInput struct {
	ID             uuid.UUID
	VolumeID       uuid.UUID
	ThreadID       uuid.UUID
	RunnerID       uuid.UUID
	AgentID        uuid.UUID
	OrganizationID uuid.UUID
	SizeGB         string
	Status         string
}

type volumeUpdateInput struct {
	ID             uuid.UUID
	Status         *string
	InstanceID     *string
	RemovedAt      *time.Time
	LastMeteringAt *time.Time
}

func (s *Server) CreateVolume(ctx context.Context, req *runnersv1.CreateVolumeRequest) (*runnersv1.CreateVolumeResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	volumeID, err := parseUUID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "volume_id: %v", err)
	}
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	runnerID, err := parseUUID(req.GetRunnerId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "runner_id: %v", err)
	}
	agentID, err := parseUUID(req.GetAgentId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "agent_id: %v", err)
	}
	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}

	statusValue, err := volumeStatusToString(req.GetStatus())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
	}
	sizeGB := strings.TrimSpace(req.GetSizeGb())
	if sizeGB == "" {
		return nil, status.Error(codes.InvalidArgument, "size_gb must be provided")
	}

	volume, err := s.insertVolume(ctx, volumeInsertInput{
		ID:             id,
		VolumeID:       volumeID,
		ThreadID:       threadID,
		RunnerID:       runnerID,
		AgentID:        agentID,
		OrganizationID: organizationID,
		SizeGB:         sizeGB,
		Status:         statusValue,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	protoVolume, err := toProtoVolume(volume)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	return &runnersv1.CreateVolumeResponse{Volume: protoVolume}, nil
}

func (s *Server) UpdateVolume(ctx context.Context, req *runnersv1.UpdateVolumeRequest) (*runnersv1.UpdateVolumeResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}

	var statusValue *string
	if req.Status != nil {
		value, err := volumeStatusToString(req.GetStatus())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
		}
		statusValue = &value
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

	if statusValue == nil && instanceID == nil && removedAt == nil && lastMeteringAt == nil {
		return nil, status.Error(codes.InvalidArgument, "at least one field must be provided")
	}

	volume, err := s.updateVolume(ctx, volumeUpdateInput{
		ID:             id,
		Status:         statusValue,
		InstanceID:     instanceID,
		RemovedAt:      removedAt,
		LastMeteringAt: lastMeteringAt,
	})
	if err != nil {
		return nil, toStatusError(err)
	}
	protoVolume, err := toProtoVolume(volume)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	return &runnersv1.UpdateVolumeResponse{Volume: protoVolume}, nil
}

func (s *Server) GetVolume(ctx context.Context, req *runnersv1.GetVolumeRequest) (*runnersv1.GetVolumeResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	volume, err := s.getVolumeByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	if err := s.requireOrgMember(ctx, callerID, volume.OrganizationID); err != nil {
		return nil, err
	}
	protoVolume, err := toProtoVolume(volume)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	return &runnersv1.GetVolumeResponse{Volume: protoVolume}, nil
}

func (s *Server) ListVolumesByThread(ctx context.Context, req *runnersv1.ListVolumesByThreadRequest) (*runnersv1.ListVolumesByThreadResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	volumes, nextToken, err := s.listVolumesByThread(ctx, threadID, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}
	isClusterAdmin, err := s.clusterAdminAllowed(ctx, callerID)
	if err != nil {
		return nil, err
	}
	if !isClusterAdmin {
		memberCache := map[uuid.UUID]bool{}
		filtered := make([]volumeRecord, 0, len(volumes))
		for _, volume := range volumes {
			allowed, err := s.memberAllowed(ctx, callerID, volume.OrganizationID, memberCache)
			if err != nil {
				return nil, err
			}
			if allowed {
				filtered = append(filtered, volume)
			}
		}
		volumes = filtered
	}
	protoVolumes, err := toProtoVolumeList(volumes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volumes: %v", err)
	}
	return &runnersv1.ListVolumesByThreadResponse{Volumes: protoVolumes, NextPageToken: nextToken}, nil
}

func (s *Server) ListVolumes(ctx context.Context, req *runnersv1.ListVolumesRequest) (*runnersv1.ListVolumesResponse, error) {
	callerID, err := identityFromMetadata(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "unauthenticated: %v", err)
	}
	statuses, err := volumeStatusesToStrings(req.GetStatuses())
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

	isClusterAdmin, err := s.clusterAdminAllowed(ctx, callerID)
	if err != nil {
		return nil, err
	}
	if organizationID != nil && !isClusterAdmin {
		if err := s.requireOrgMember(ctx, callerID, *organizationID); err != nil {
			return nil, err
		}
	}

	pendingSample := req.PendingSample != nil && req.GetPendingSample()

	volumes, nextToken, err := s.listVolumes(ctx, statuses, organizationID, runnerID, pendingSample, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}
	if organizationID == nil && !isClusterAdmin {
		memberCache := map[uuid.UUID]bool{}
		filtered := make([]volumeRecord, 0, len(volumes))
		for _, volume := range volumes {
			allowed, err := s.memberAllowed(ctx, callerID, volume.OrganizationID, memberCache)
			if err != nil {
				return nil, err
			}
			if allowed {
				filtered = append(filtered, volume)
			}
		}
		volumes = filtered
	}
	protoVolumes, err := toProtoVolumeList(volumes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volumes: %v", err)
	}
	return &runnersv1.ListVolumesResponse{Volumes: protoVolumes, NextPageToken: nextToken}, nil
}

func (s *Server) BatchUpdateVolumeSampledAt(ctx context.Context, req *runnersv1.BatchUpdateVolumeSampledAtRequest) (*runnersv1.BatchUpdateVolumeSampledAtResponse, error) {
	entries := req.GetEntries()
	if len(entries) == 0 {
		return &runnersv1.BatchUpdateVolumeSampledAtResponse{}, nil
	}
	updates, err := parseSampledAtEntries(entries)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.batchUpdateVolumeSampledAt(ctx, updates); err != nil {
		return nil, status.Errorf(codes.Internal, "batch update volumes: %v", err)
	}
	return &runnersv1.BatchUpdateVolumeSampledAtResponse{}, nil
}

func (s *Server) insertVolume(ctx context.Context, input volumeInsertInput) (volumeRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`INSERT INTO volumes (id, volume_id, thread_id, runner_id, agent_id, organization_id, size_gb, status)
	    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	    RETURNING %s`, volumeColumns),
		input.ID,
		input.VolumeID,
		input.ThreadID,
		input.RunnerID,
		input.AgentID,
		input.OrganizationID,
		input.SizeGB,
		input.Status,
	)
	volume, err := scanVolume(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "23505" {
				return volumeRecord{}, AlreadyExists("volume")
			}
			if pgErr.Code == "23503" {
				return volumeRecord{}, NotFound("runner")
			}
		}
		return volumeRecord{}, err
	}
	return volume, nil
}

func (s *Server) updateVolume(ctx context.Context, input volumeUpdateInput) (volumeRecord, error) {
	clauses := make([]string, 0, 5)
	args := make([]any, 0, 5)

	if input.Status != nil {
		addUpdateClause(&clauses, &args, "status", *input.Status)
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
	query, args := buildUpdateQuery("volumes", volumeColumns, clauses, args, input.ID)
	row := s.pool.QueryRow(ctx, query, args...)
	volume, err := scanVolume(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return volumeRecord{}, NotFound("volume")
		}
		return volumeRecord{}, err
	}
	return volume, nil
}

func (s *Server) getVolumeByID(ctx context.Context, id uuid.UUID) (volumeRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT %s FROM volumes WHERE id = $1`, volumeColumns),
		id,
	)
	volume, err := scanVolume(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return volumeRecord{}, NotFound("volume")
		}
		return volumeRecord{}, err
	}
	return volume, nil
}

func (s *Server) listVolumesByThread(ctx context.Context, threadID uuid.UUID, pageSize int32, pageToken string) ([]volumeRecord, string, error) {
	limit := normalizePageSize(pageSize)
	clauses := []string{"thread_id = $1"}
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
	query.WriteString(fmt.Sprintf("SELECT %s FROM volumes", volumeColumns))
	query.WriteString(" WHERE ")
	query.WriteString(strings.Join(clauses, " AND "))
	query.WriteString(fmt.Sprintf(" ORDER BY id ASC LIMIT $%d", len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	volumes := make([]volumeRecord, 0, limit)
	var (
		lastID  uuid.UUID
		hasMore bool
	)
	for rows.Next() {
		if int32(len(volumes)) == limit {
			hasMore = true
			break
		}
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, "", err
		}
		volumes = append(volumes, volume)
		lastID = volume.Meta.ID
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		nextToken = encodePageToken(lastID)
	}
	return volumes, nextToken, nil
}

func (s *Server) listVolumes(ctx context.Context, statuses []string, organizationID *uuid.UUID, runnerID *uuid.UUID, pendingSample bool, pageSize int32, pageToken string) ([]volumeRecord, string, error) {
	limit := normalizePageSize(pageSize)
	query, args, err := buildListQuery(listQueryInput{
		Table:          "volumes",
		Columns:        volumeColumns,
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

	volumes := make([]volumeRecord, 0, limit)
	var (
		lastID  uuid.UUID
		hasMore bool
	)
	for rows.Next() {
		if int32(len(volumes)) == limit {
			hasMore = true
			break
		}
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, "", err
		}
		volumes = append(volumes, volume)
		lastID = volume.Meta.ID
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		nextToken = encodePageToken(lastID)
	}
	return volumes, nextToken, nil
}

func (s *Server) batchUpdateVolumeSampledAt(ctx context.Context, entries []sampledAtEntry) error {
	query, args := buildBatchSampledAtUpdateQuery("volumes", entries)
	if _, err := s.pool.Exec(ctx, query, args...); err != nil {
		return err
	}
	return nil
}

func scanVolume(row pgx.Row) (volumeRecord, error) {
	var (
		volume         volumeRecord
		instanceID     pgtype.Text
		removedAt      pgtype.Timestamptz
		lastMeteringAt pgtype.Timestamptz
	)
	if err := row.Scan(
		&volume.Meta.ID,
		&instanceID,
		&volume.VolumeID,
		&volume.ThreadID,
		&volume.RunnerID,
		&volume.AgentID,
		&volume.OrganizationID,
		&volume.SizeGB,
		&volume.Status,
		&removedAt,
		&lastMeteringAt,
		&volume.Meta.CreatedAt,
		&volume.Meta.UpdatedAt,
	); err != nil {
		return volumeRecord{}, err
	}
	if instanceID.Valid {
		value := instanceID.String
		volume.InstanceID = &value
	}
	if removedAt.Valid {
		value := removedAt.Time
		volume.RemovedAt = &value
	}
	if lastMeteringAt.Valid {
		value := lastMeteringAt.Time
		volume.LastMeteringAt = &value
	}
	return volume, nil
}

func toProtoVolume(record volumeRecord) (*runnersv1.Volume, error) {
	statusValue, err := volumeStatusFromString(record.Status)
	if err != nil {
		return nil, err
	}
	protoVolume := &runnersv1.Volume{
		Meta:           toProtoEntityMeta(record.Meta),
		VolumeId:       record.VolumeID.String(),
		ThreadId:       record.ThreadID.String(),
		RunnerId:       record.RunnerID.String(),
		AgentId:        record.AgentID.String(),
		OrganizationId: record.OrganizationID.String(),
		SizeGb:         record.SizeGB,
		Status:         statusValue,
	}
	if record.InstanceID != nil {
		protoVolume.InstanceId = record.InstanceID
	}
	if record.RemovedAt != nil {
		protoVolume.RemovedAt = timestamppb.New(*record.RemovedAt)
	}
	if record.LastMeteringAt != nil {
		protoVolume.LastMeteringSampledAt = timestamppb.New(*record.LastMeteringAt)
	}
	return protoVolume, nil
}

func toProtoVolumeList(records []volumeRecord) ([]*runnersv1.Volume, error) {
	volumes := make([]*runnersv1.Volume, 0, len(records))
	for _, record := range records {
		volume, err := toProtoVolume(record)
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, volume)
	}
	return volumes, nil
}

func volumeStatusToString(status runnersv1.VolumeStatus) (string, error) {
	switch status {
	case runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING:
		return volumeStatusProvisioning, nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE:
		return volumeStatusActive, nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING:
		return volumeStatusDeprovision, nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_DELETED:
		return volumeStatusDeleted, nil
	case runnersv1.VolumeStatus_VOLUME_STATUS_FAILED:
		return volumeStatusFailed, nil
	default:
		return "", fmt.Errorf("invalid volume status: %s", status.String())
	}
}

func volumeStatusFromString(value string) (runnersv1.VolumeStatus, error) {
	switch value {
	case volumeStatusProvisioning:
		return runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING, nil
	case volumeStatusActive:
		return runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE, nil
	case volumeStatusDeprovision:
		return runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING, nil
	case volumeStatusDeleted:
		return runnersv1.VolumeStatus_VOLUME_STATUS_DELETED, nil
	case volumeStatusFailed:
		return runnersv1.VolumeStatus_VOLUME_STATUS_FAILED, nil
	default:
		return runnersv1.VolumeStatus_VOLUME_STATUS_UNSPECIFIED, fmt.Errorf("invalid volume status: %s", value)
	}
}

func volumeStatusesToStrings(statuses []runnersv1.VolumeStatus) ([]string, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	values := make([]string, len(statuses))
	for i, statusValue := range statuses {
		value, err := volumeStatusToString(statusValue)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}
