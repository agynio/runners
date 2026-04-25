package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
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

type volumeListFilter struct {
	OrganizationID     uuid.UUID
	RunnerIDs          []uuid.UUID
	Statuses           []string
	AttachedKinds      []runnersv1.VolumeAttachmentFilterKind
	PendingSample      bool
	VolumeNameContains string
}

type volumeSortField int

const (
	volumeSortName volumeSortField = iota
	volumeSortSize
	volumeSortStatus
	volumeSortCreated
)

type volumeListSort struct {
	Field     volumeSortField
	Direction sortDirection
}

type volumeListItem struct {
	record      volumeRecord
	volumeName  string
	attachments []*runnersv1.Attachment
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

	var existingVolume *volumeRecord
	if s.notificationsClient != nil && (statusValue != nil || instanceID != nil || removedAt != nil) {
		volume, err := s.getVolumeByID(ctx, id)
		if err != nil {
			return nil, toStatusError(err)
		}
		existingVolume = &volume
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
	if existingVolume != nil {
		statusChanged := statusValue != nil && *statusValue != existingVolume.Status
		instanceChanged := instanceID != nil && (existingVolume.InstanceID == nil || *instanceID != *existingVolume.InstanceID)
		removedChanged := removedAt != nil && (existingVolume.RemovedAt == nil || !existingVolume.RemovedAt.Equal(*removedAt))
		if statusChanged || instanceChanged || removedChanged {
			s.publishVolumeUpdateNotification(ctx, volume)
		}
	}
	protoVolume, err := toProtoVolume(volume)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	return &runnersv1.UpdateVolumeResponse{Volume: protoVolume}, nil
}

func (s *Server) publishVolumeUpdateNotification(ctx context.Context, volume volumeRecord) {
	if s.notificationsClient == nil {
		return
	}
	payloadFields := map[string]any{
		"volume_id": volume.Meta.ID.String(),
		"status":    volume.Status,
	}
	payload, err := structpb.NewStruct(payloadFields)
	if err != nil {
		log.Printf("runners: build volume notification payload: %v", err)
		return
	}
	rooms := []string{
		fmt.Sprintf("organization:%s", volume.OrganizationID.String()),
		fmt.Sprintf("volume:%s", volume.Meta.ID.String()),
	}
	s.publishVolumeNotification(ctx, "volume.updated", rooms, payload)
}

func (s *Server) publishVolumeNotification(ctx context.Context, event string, rooms []string, payload *structpb.Struct) {
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
	items, err := s.buildVolumeItems(ctx, []volumeRecord{volume})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
	}
	protoVolume, err := toProtoVolumeWithEnrichment(items[0].record, items[0].volumeName, items[0].attachments)
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
	orgValue := strings.TrimSpace(req.GetOrganizationId())
	if orgValue == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id: value is empty")
	}
	organizationID, err := parseUUID(orgValue)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}
	if err := s.requireRelation(ctx, callerID, organizationViewVolumes, organizationObject(organizationID)); err != nil {
		return nil, err
	}

	filter := volumeListFilter{OrganizationID: organizationID}
	pendingSampleSet := false
	if req.Filter != nil {
		filter.RunnerIDs = make([]uuid.UUID, 0, len(req.Filter.RunnerIdIn))
		for _, runnerValue := range req.Filter.RunnerIdIn {
			parsed, err := parseUUID(runnerValue)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "filter.runner_id_in: %v", err)
			}
			filter.RunnerIDs = append(filter.RunnerIDs, parsed)
		}
		filter.AttachedKinds = make([]runnersv1.VolumeAttachmentFilterKind, 0, len(req.Filter.AttachedToKindIn))
		for _, kind := range req.Filter.AttachedToKindIn {
			switch kind {
			case runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_UNSPECIFIED:
				return nil, status.Error(codes.InvalidArgument, "filter.attached_to_kind_in: unspecified kind")
			case runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_AGENT,
				runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_MCP,
				runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_HOOK,
				runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_UNATTACHED:
				filter.AttachedKinds = append(filter.AttachedKinds, kind)
			default:
				return nil, status.Errorf(codes.InvalidArgument, "filter.attached_to_kind_in: %s", kind.String())
			}
		}
		if req.Filter.PendingSample != nil {
			filter.PendingSample = req.Filter.GetPendingSample()
			pendingSampleSet = true
		}
		if req.Filter.VolumeNameSubstring != nil {
			filter.VolumeNameContains = strings.TrimSpace(req.Filter.GetVolumeNameSubstring())
		}
	}

	if req.RunnerId != nil {
		parsed, err := parseUUID(req.GetRunnerId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "runner_id: %v", err)
		}
		filter.RunnerIDs = append(filter.RunnerIDs, parsed)
	}
	if req.PendingSample != nil && !pendingSampleSet {
		filter.PendingSample = req.GetPendingSample()
	}

	statusFilters := make([]runnersv1.VolumeStatus, 0)
	if req.Filter != nil {
		statusFilters = append(statusFilters, req.Filter.StatusIn...)
	}
	statusFilters = append(statusFilters, req.GetStatuses()...)
	statuses, err := volumeStatusesToStrings(statusFilters)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "statuses: %v", err)
	}
	filter.Statuses = statuses

	sort, err := parseVolumeSort(req.GetSort())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "sort: %v", err)
	}

	records, err := s.listVolumes(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}
	items, err := s.buildVolumeItems(ctx, records)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "enrich volumes: %v", err)
	}
	items = filterVolumeItems(items, filter)
	sortVolumeItems(items, sort)
	items, err = applyVolumeCursor(items, sort, req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "invalid page_token: %v", err)
	}
	pageItems, nextToken, err := paginateVolumeItems(items, sort, req.GetPageSize())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "paginate volumes: %v", err)
	}
	protoVolumes := make([]*runnersv1.Volume, 0, len(pageItems))
	for _, item := range pageItems {
		volume, err := toProtoVolumeWithEnrichment(item.record, item.volumeName, item.attachments)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "convert volume: %v", err)
		}
		protoVolumes = append(protoVolumes, volume)
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

func parseVolumeSort(sort *runnersv1.ListVolumesSort) (volumeListSort, error) {
	field := volumeSortName
	if sort != nil {
		switch sort.GetField() {
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_UNSPECIFIED:
			field = volumeSortName
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_NAME:
			field = volumeSortName
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_SIZE:
			field = volumeSortSize
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_STATUS:
			field = volumeSortStatus
		case runnersv1.ListVolumesSortField_LIST_VOLUMES_SORT_FIELD_CREATED:
			field = volumeSortCreated
		default:
			return volumeListSort{}, fmt.Errorf("invalid sort field: %s", sort.GetField().String())
		}
	}
	defaultDirection := sortAsc
	if sort == nil {
		return volumeListSort{Field: field, Direction: defaultDirection}, nil
	}
	direction, err := parseSortDirection(sort.GetDirection(), defaultDirection)
	if err != nil {
		return volumeListSort{}, err
	}
	return volumeListSort{Field: field, Direction: direction}, nil
}

func volumeCursorPrimary(field volumeSortField, cursor listCursor) (any, error) {
	switch field {
	case volumeSortName:
		return strings.ToLower(cursor.Primary), nil
	case volumeSortSize:
		return cursor.Primary, nil
	case volumeSortStatus:
		return cursor.Primary, nil
	case volumeSortCreated:
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.Primary)
		if err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
		return createdAt, nil
	default:
		return nil, errors.New("invalid sort field")
	}
}

func volumePrimaryValue(item volumeListItem, field volumeSortField) (string, error) {
	switch field {
	case volumeSortName:
		return strings.ToLower(item.volumeName), nil
	case volumeSortSize:
		return item.record.SizeGB, nil
	case volumeSortStatus:
		return item.record.Status, nil
	case volumeSortCreated:
		return item.record.Meta.CreatedAt.UTC().Format(time.RFC3339Nano), nil
	default:
		return "", errors.New("invalid sort field")
	}
}

func compareVolumePrimary(item volumeListItem, other volumeListItem, field volumeSortField) int {
	switch field {
	case volumeSortName:
		return strings.Compare(strings.ToLower(item.volumeName), strings.ToLower(other.volumeName))
	case volumeSortSize:
		return strings.Compare(item.record.SizeGB, other.record.SizeGB)
	case volumeSortStatus:
		return strings.Compare(item.record.Status, other.record.Status)
	case volumeSortCreated:
		if item.record.Meta.CreatedAt.Before(other.record.Meta.CreatedAt) {
			return -1
		}
		if item.record.Meta.CreatedAt.After(other.record.Meta.CreatedAt) {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func compareVolumePrimaryToCursor(item volumeListItem, cursorPrimary any, field volumeSortField) (int, error) {
	switch field {
	case volumeSortName:
		cursorName, ok := cursorPrimary.(string)
		if !ok {
			return 0, errors.New("invalid cursor name")
		}
		return strings.Compare(strings.ToLower(item.volumeName), cursorName), nil
	case volumeSortSize:
		cursorSize, ok := cursorPrimary.(string)
		if !ok {
			return 0, errors.New("invalid cursor size")
		}
		return strings.Compare(item.record.SizeGB, cursorSize), nil
	case volumeSortStatus:
		cursorStatus, ok := cursorPrimary.(string)
		if !ok {
			return 0, errors.New("invalid cursor status")
		}
		return strings.Compare(item.record.Status, cursorStatus), nil
	case volumeSortCreated:
		cursorTime, ok := cursorPrimary.(time.Time)
		if !ok {
			return 0, errors.New("invalid cursor created_at")
		}
		if item.record.Meta.CreatedAt.Before(cursorTime) {
			return -1, nil
		}
		if item.record.Meta.CreatedAt.After(cursorTime) {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, errors.New("invalid sort field")
	}
}

func (s *Server) listVolumes(ctx context.Context, filter volumeListFilter) ([]volumeRecord, error) {
	clauses := []string{fmt.Sprintf("organization_id = $%d", 1)}
	args := []any{filter.OrganizationID}

	if len(filter.RunnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("runner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.RunnerIDs))
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.Statuses))
	}
	if filter.PendingSample {
		clauses = append(clauses, pendingSampleClause)
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM volumes", volumeColumns))
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	volumes := []volumeRecord{}
	for rows.Next() {
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, volume)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return volumes, nil
}

func (s *Server) listVolumeAttachments(ctx context.Context, volumeID uuid.UUID) ([]*agentsv1.VolumeAttachment, error) {
	if s.agentsClient == nil {
		return nil, errors.New("agents client not configured")
	}
	attachments := []*agentsv1.VolumeAttachment{}
	pageToken := ""
	for {
		resp, err := s.agentsClient.ListVolumeAttachments(ctx, &agentsv1.ListVolumeAttachmentsRequest{
			PageSize:  maxListPageSize,
			PageToken: pageToken,
			VolumeId:  volumeID.String(),
		})
		if err != nil {
			return nil, err
		}
		attachments = append(attachments, resp.GetVolumeAttachments()...)
		if resp.GetNextPageToken() == "" {
			break
		}
		pageToken = resp.GetNextPageToken()
	}
	return attachments, nil
}

func (s *Server) buildVolumeItems(ctx context.Context, records []volumeRecord) ([]volumeListItem, error) {
	if len(records) == 0 {
		return []volumeListItem{}, nil
	}
	volumeIDs := make([]uuid.UUID, 0, len(records))
	for _, record := range records {
		volumeIDs = append(volumeIDs, record.VolumeID)
	}
	volumeNames, err := s.resolveVolumeNames(ctx, uniqueUUIDs(volumeIDs))
	if err != nil {
		return nil, err
	}

	attachmentsByVolume := make(map[uuid.UUID][]*agentsv1.VolumeAttachment, len(records))
	attachmentAgentIDs := []uuid.UUID{}
	mcpIDs := []uuid.UUID{}
	hookIDs := []uuid.UUID{}

	for _, record := range records {
		attachments, err := s.listVolumeAttachments(ctx, record.VolumeID)
		if err != nil {
			return nil, fmt.Errorf("list volume attachments: %w", err)
		}
		attachmentsByVolume[record.VolumeID] = attachments
		for _, attachment := range attachments {
			if attachment.GetAgentId() != "" {
				parsed, err := parseUUID(attachment.GetAgentId())
				if err != nil {
					return nil, fmt.Errorf("attachment agent_id: %w", err)
				}
				attachmentAgentIDs = append(attachmentAgentIDs, parsed)
				continue
			}
			if attachment.GetMcpId() != "" {
				parsed, err := parseUUID(attachment.GetMcpId())
				if err != nil {
					return nil, fmt.Errorf("attachment mcp_id: %w", err)
				}
				mcpIDs = append(mcpIDs, parsed)
				continue
			}
			if attachment.GetHookId() != "" {
				parsed, err := parseUUID(attachment.GetHookId())
				if err != nil {
					return nil, fmt.Errorf("attachment hook_id: %w", err)
				}
				hookIDs = append(hookIDs, parsed)
				continue
			}
			return nil, errors.New("attachment target missing")
		}
	}

	agentNames, err := s.resolveAgentNames(ctx, uniqueUUIDs(attachmentAgentIDs))
	if err != nil {
		return nil, err
	}
	mcpNames := make(map[uuid.UUID]string, len(mcpIDs))
	for _, mcpID := range uniqueUUIDs(mcpIDs) {
		name, err := s.resolveMcpName(ctx, mcpID)
		if err != nil {
			return nil, err
		}
		mcpNames[mcpID] = name
	}
	hookNames := make(map[uuid.UUID]string, len(hookIDs))
	for _, hookID := range uniqueUUIDs(hookIDs) {
		name, err := s.resolveHookName(ctx, hookID)
		if err != nil {
			return nil, err
		}
		hookNames[hookID] = name
	}

	items := make([]volumeListItem, 0, len(records))
	for _, record := range records {
		name, ok := volumeNames[record.VolumeID]
		if !ok {
			return nil, fmt.Errorf("volume name missing for %s", record.VolumeID)
		}
		attachments := attachmentsByVolume[record.VolumeID]
		protoAttachments := make([]*runnersv1.Attachment, 0, len(attachments))
		for _, attachment := range attachments {
			switch {
			case attachment.GetAgentId() != "":
				parsed, err := parseUUID(attachment.GetAgentId())
				if err != nil {
					return nil, fmt.Errorf("attachment agent_id: %w", err)
				}
				name, ok := agentNames[parsed]
				if !ok {
					return nil, fmt.Errorf("agent name missing for %s", parsed)
				}
				protoAttachments = append(protoAttachments, &runnersv1.Attachment{
					Kind: runnersv1.AttachmentKind_ATTACHMENT_KIND_AGENT,
					Id:   parsed.String(),
					Name: name,
				})
			case attachment.GetMcpId() != "":
				parsed, err := parseUUID(attachment.GetMcpId())
				if err != nil {
					return nil, fmt.Errorf("attachment mcp_id: %w", err)
				}
				name, ok := mcpNames[parsed]
				if !ok {
					return nil, fmt.Errorf("mcp name missing for %s", parsed)
				}
				protoAttachments = append(protoAttachments, &runnersv1.Attachment{
					Kind: runnersv1.AttachmentKind_ATTACHMENT_KIND_MCP,
					Id:   parsed.String(),
					Name: name,
				})
			case attachment.GetHookId() != "":
				parsed, err := parseUUID(attachment.GetHookId())
				if err != nil {
					return nil, fmt.Errorf("attachment hook_id: %w", err)
				}
				name, ok := hookNames[parsed]
				if !ok {
					return nil, fmt.Errorf("hook name missing for %s", parsed)
				}
				protoAttachments = append(protoAttachments, &runnersv1.Attachment{
					Kind: runnersv1.AttachmentKind_ATTACHMENT_KIND_HOOK,
					Id:   parsed.String(),
					Name: name,
				})
			default:
				return nil, errors.New("attachment target missing")
			}
		}
		items = append(items, volumeListItem{record: record, volumeName: name, attachments: protoAttachments})
	}
	return items, nil
}

func filterVolumeItems(items []volumeListItem, filter volumeListFilter) []volumeListItem {
	if len(items) == 0 {
		return items
	}
	nameFilter := strings.TrimSpace(filter.VolumeNameContains)
	nameFilter = strings.ToLower(nameFilter)
	kindFilter := map[runnersv1.VolumeAttachmentFilterKind]bool{}
	for _, kind := range filter.AttachedKinds {
		kindFilter[kind] = true
	}

	filtered := make([]volumeListItem, 0, len(items))
	for _, item := range items {
		if nameFilter != "" && !strings.Contains(strings.ToLower(item.volumeName), nameFilter) {
			continue
		}
		if len(kindFilter) > 0 {
			if len(item.attachments) == 0 {
				if !kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_UNATTACHED] {
					continue
				}
			} else {
				matches := false
				for _, attachment := range item.attachments {
					switch attachment.GetKind() {
					case runnersv1.AttachmentKind_ATTACHMENT_KIND_AGENT:
						if kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_AGENT] {
							matches = true
						}
					case runnersv1.AttachmentKind_ATTACHMENT_KIND_MCP:
						if kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_MCP] {
							matches = true
						}
					case runnersv1.AttachmentKind_ATTACHMENT_KIND_HOOK:
						if kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_HOOK] {
							matches = true
						}
					}
					if matches {
						break
					}
				}
				if !matches {
					continue
				}
			}
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func sortVolumeItems(items []volumeListItem, sortSpec volumeListSort) {
	sort.SliceStable(items, func(i, j int) bool {
		primaryCmp := compareVolumePrimary(items[i], items[j], sortSpec.Field)
		if primaryCmp == 0 {
			return compareUUID(items[i].record.Meta.ID, items[j].record.Meta.ID) < 0
		}
		if sortSpec.Direction == sortAsc {
			return primaryCmp < 0
		}
		return primaryCmp > 0
	})
}

func applyVolumeCursor(items []volumeListItem, sort volumeListSort, pageToken string) ([]volumeListItem, error) {
	if pageToken == "" {
		return items, nil
	}
	cursor, cursorID, err := decodeListCursor(pageToken)
	if err != nil {
		return nil, InvalidPageToken(err)
	}
	cursorPrimary, err := volumeCursorPrimary(sort.Field, cursor)
	if err != nil {
		return nil, InvalidPageToken(err)
	}

	for idx, item := range items {
		primaryCmp, err := compareVolumePrimaryToCursor(item, cursorPrimary, sort.Field)
		if err != nil {
			return nil, err
		}
		if sort.Direction == sortAsc {
			if primaryCmp > 0 || (primaryCmp == 0 && compareUUID(item.record.Meta.ID, cursorID) > 0) {
				return items[idx:], nil
			}
			continue
		}
		if primaryCmp < 0 || (primaryCmp == 0 && compareUUID(item.record.Meta.ID, cursorID) > 0) {
			return items[idx:], nil
		}
	}
	return []volumeListItem{}, nil
}

func paginateVolumeItems(items []volumeListItem, sort volumeListSort, pageSize int32) ([]volumeListItem, string, error) {
	limit := normalizePageSize(pageSize)
	if len(items) <= int(limit) {
		return items, "", nil
	}
	page := items[:limit]
	lastItem := page[len(page)-1]
	primary, err := volumePrimaryValue(lastItem, sort.Field)
	if err != nil {
		return nil, "", err
	}
	nextToken, err := encodeListCursor(primary, lastItem.record.Meta.ID)
	if err != nil {
		return nil, "", err
	}
	return page, nextToken, nil
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

func toProtoVolumeWithEnrichment(record volumeRecord, volumeName string, attachments []*runnersv1.Attachment) (*runnersv1.Volume, error) {
	volume, err := toProtoVolume(record)
	if err != nil {
		return nil, err
	}
	volume.VolumeName = volumeName
	volume.Attachments = attachments
	return volume, nil
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
