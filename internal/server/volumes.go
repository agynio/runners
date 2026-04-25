package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
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

type volumeEnrichmentCache struct {
	volumeNames map[uuid.UUID]string
	attachments map[uuid.UUID][]*runnersv1.Attachment
	agentNames  map[uuid.UUID]string
	mcpNames    map[uuid.UUID]string
	hookNames   map[uuid.UUID]string
}

func newVolumeEnrichmentCache() *volumeEnrichmentCache {
	return &volumeEnrichmentCache{
		volumeNames: map[uuid.UUID]string{},
		attachments: map[uuid.UUID][]*runnersv1.Attachment{},
		agentNames:  map[uuid.UUID]string{},
		mcpNames:    map[uuid.UUID]string{},
		hookNames:   map[uuid.UUID]string{},
	}
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
	if err := s.requireRelation(ctx, callerID, organizationViewVolumes, organizationObject(volume.OrganizationID)); err != nil {
		return nil, err
	}
	cache := newVolumeEnrichmentCache()
	items, err := s.buildVolumeItems(ctx, []volumeRecord{volume}, cache, true)
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
	isClusterAdmin, err := s.clusterAdminAllowed(ctx, callerID)
	if err != nil {
		return nil, err
	}
	if !isClusterAdmin {
		if err := s.requireRelation(ctx, callerID, organizationViewVolumes, organizationObject(organizationID)); err != nil {
			return nil, err
		}
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

	cache := newVolumeEnrichmentCache()
	var (
		pageItems []volumeListItem
		nextToken string
	)
	if sort.Field == volumeSortName {
		pageItems, nextToken, err = s.listVolumesByName(ctx, filter, sort, req.GetPageSize(), req.GetPageToken(), cache)
	} else {
		pageItems, nextToken, err = s.listVolumesPaged(ctx, filter, sort, req.GetPageSize(), req.GetPageToken(), cache)
	}
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
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

func (s *Server) listVolumesByName(ctx context.Context, filter volumeListFilter, sort volumeListSort, pageSize int32, pageToken string, cache *volumeEnrichmentCache) ([]volumeListItem, string, error) {
	limit := normalizePageSize(pageSize)
	volumeIDs, err := s.listVolumeIDs(ctx, filter)
	if err != nil {
		return nil, "", err
	}
	if len(volumeIDs) == 0 {
		if pageToken != "" {
			cursor, _, err := decodeListCursor(pageToken)
			if err != nil {
				return nil, "", InvalidPageToken(err)
			}
			if _, err := volumeCursorPrimary(sort.Field, cursor); err != nil {
				return nil, "", InvalidPageToken(err)
			}
		}
		return []volumeListItem{}, "", nil
	}
	if err := s.ensureVolumeNames(ctx, cache, volumeIDs); err != nil {
		return nil, "", err
	}
	volumeNames := make(map[uuid.UUID]string, len(volumeIDs))
	for _, volumeID := range volumeIDs {
		name, ok := cache.volumeNames[volumeID]
		if !ok {
			return nil, "", fmt.Errorf("volume name missing for %s", volumeID)
		}
		volumeNames[volumeID] = name
	}

	nameFilter := strings.ToLower(strings.TrimSpace(filter.VolumeNameContains))
	kindFilter := map[runnersv1.VolumeAttachmentFilterKind]bool{}
	for _, kind := range filter.AttachedKinds {
		kindFilter[kind] = true
	}

	collected := make([]volumeListItem, 0, limit+1)
	currentToken := pageToken
	for {
		records, nextToken, err := s.listVolumesByNamePage(ctx, filter, sort, pageSize, currentToken, volumeNames)
		if err != nil {
			return nil, "", err
		}
		if len(records) == 0 {
			break
		}
		items, err := s.buildVolumeItems(ctx, records, cache, false)
		if err != nil {
			return nil, "", err
		}
		for _, item := range items {
			if nameFilter != "" && !strings.Contains(strings.ToLower(item.volumeName), nameFilter) {
				continue
			}
			if err := s.ensureVolumeAttachments(ctx, cache, []uuid.UUID{item.record.VolumeID}); err != nil {
				return nil, "", err
			}
			item.attachments = cache.attachments[item.record.VolumeID]
			if len(kindFilter) > 0 && !matchesVolumeAttachmentKinds(item.attachments, kindFilter) {
				continue
			}
			collected = append(collected, item)
			if len(collected) == int(limit)+1 {
				break
			}
		}
		if len(collected) == int(limit)+1 {
			break
		}
		if nextToken == "" {
			break
		}
		currentToken = nextToken
	}
	if len(collected) <= int(limit) {
		return collected, "", nil
	}
	page := collected[:limit]
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

func (s *Server) listVolumesPaged(ctx context.Context, filter volumeListFilter, sort volumeListSort, pageSize int32, pageToken string, cache *volumeEnrichmentCache) ([]volumeListItem, string, error) {
	limit := normalizePageSize(pageSize)
	collected := make([]volumeListItem, 0, limit+1)
	currentToken := pageToken
	kindFilter := map[runnersv1.VolumeAttachmentFilterKind]bool{}
	for _, kind := range filter.AttachedKinds {
		kindFilter[kind] = true
	}

	for {
		records, nextToken, err := s.listVolumesPage(ctx, filter, sort, pageSize, currentToken)
		if err != nil {
			return nil, "", err
		}
		if len(records) == 0 {
			break
		}
		items, err := s.buildVolumeItems(ctx, records, cache, false)
		if err != nil {
			return nil, "", err
		}
		items = filterVolumeItemsByName(items, filter.VolumeNameContains)
		for _, item := range items {
			if err := s.ensureVolumeAttachments(ctx, cache, []uuid.UUID{item.record.VolumeID}); err != nil {
				return nil, "", err
			}
			item.attachments = cache.attachments[item.record.VolumeID]
			if len(kindFilter) > 0 && !matchesVolumeAttachmentKinds(item.attachments, kindFilter) {
				continue
			}
			collected = append(collected, item)
			if len(collected) == int(limit)+1 {
				break
			}
		}
		if len(collected) == int(limit)+1 {
			break
		}
		if nextToken == "" {
			break
		}
		currentToken = nextToken
	}
	if len(collected) <= int(limit) {
		return collected, "", nil
	}
	page := collected[:limit]
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

func volumeSortColumn(field volumeSortField) (string, error) {
	switch field {
	case volumeSortSize:
		return "volumes.size_gb::numeric", nil
	case volumeSortStatus:
		return "volumes.status", nil
	case volumeSortCreated:
		return "volumes.created_at", nil
	default:
		return "", errors.New("invalid sort field")
	}
}

func volumeCursorCast(field volumeSortField) string {
	if field == volumeSortSize {
		return "::numeric"
	}
	return ""
}

func buildVolumeNameSortExpr(volumeNames map[uuid.UUID]string, startIndex int) (string, []any, error) {
	if len(volumeNames) == 0 {
		return "", nil, errors.New("volume names empty")
	}
	ids := make([]uuid.UUID, 0, len(volumeNames))
	for id := range volumeNames {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})

	parts := make([]string, 0, len(ids))
	args := make([]any, 0, len(ids)*2)
	idx := startIndex
	for _, id := range ids {
		name := strings.ToLower(strings.TrimSpace(volumeNames[id]))
		parts = append(parts, fmt.Sprintf("WHEN $%d THEN $%d", idx, idx+1))
		args = append(args, id, name)
		idx += 2
	}
	return fmt.Sprintf("CASE volumes.volume_id %s END", strings.Join(parts, " ")), args, nil
}

func parseVolumeSize(value string) (*big.Rat, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, errors.New("size_gb is empty")
	}
	size, ok := new(big.Rat).SetString(trimmed)
	if !ok {
		return nil, fmt.Errorf("invalid size_gb %q", value)
	}
	return size, nil
}

func compareVolumeSize(a, b string) (int, error) {
	left, err := parseVolumeSize(a)
	if err != nil {
		return 0, err
	}
	right, err := parseVolumeSize(b)
	if err != nil {
		return 0, err
	}
	return left.Cmp(right), nil
}

func volumeCursorPrimary(field volumeSortField, cursor listCursor) (any, error) {
	switch field {
	case volumeSortName:
		primary := strings.TrimSpace(cursor.Primary)
		if primary == "" {
			return nil, errors.New("cursor primary is empty")
		}
		return strings.ToLower(primary), nil
	case volumeSortSize:
		primary := strings.TrimSpace(cursor.Primary)
		if primary == "" {
			return nil, errors.New("cursor primary is empty")
		}
		if _, err := parseVolumeSize(primary); err != nil {
			return nil, err
		}
		return primary, nil
	case volumeSortStatus:
		if strings.TrimSpace(cursor.Primary) == "" {
			return nil, errors.New("cursor primary is empty")
		}
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
		return strings.ToLower(strings.TrimSpace(item.volumeName)), nil
	case volumeSortSize:
		value := strings.TrimSpace(item.record.SizeGB)
		if _, err := parseVolumeSize(value); err != nil {
			return "", err
		}
		return value, nil
	case volumeSortStatus:
		return item.record.Status, nil
	case volumeSortCreated:
		return item.record.Meta.CreatedAt.UTC().Format(time.RFC3339Nano), nil
	default:
		return "", errors.New("invalid sort field")
	}
}

func compareVolumePrimary(item volumeListItem, other volumeListItem, field volumeSortField) (int, error) {
	switch field {
	case volumeSortName:
		return strings.Compare(strings.ToLower(item.volumeName), strings.ToLower(other.volumeName)), nil
	case volumeSortSize:
		return compareVolumeSize(item.record.SizeGB, other.record.SizeGB)
	case volumeSortStatus:
		return strings.Compare(item.record.Status, other.record.Status), nil
	case volumeSortCreated:
		if item.record.Meta.CreatedAt.Before(other.record.Meta.CreatedAt) {
			return -1, nil
		}
		if item.record.Meta.CreatedAt.After(other.record.Meta.CreatedAt) {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, errors.New("invalid sort field")
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
		return compareVolumeSize(item.record.SizeGB, cursorSize)
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

func (s *Server) listVolumeIDs(ctx context.Context, filter volumeListFilter) ([]uuid.UUID, error) {
	clauses := []string{fmt.Sprintf("volumes.organization_id = $%d", 1)}
	args := []any{filter.OrganizationID}

	if len(filter.RunnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.runner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.RunnerIDs))
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.Statuses))
	}
	if filter.PendingSample {
		clauses = append(clauses, pendingSampleClause)
	}

	query := strings.Builder{}
	query.WriteString("SELECT DISTINCT volume_id FROM volumes")
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	volumeIDs := []uuid.UUID{}
	for rows.Next() {
		var volumeID uuid.UUID
		if err := rows.Scan(&volumeID); err != nil {
			return nil, err
		}
		volumeIDs = append(volumeIDs, volumeID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return volumeIDs, nil
}

func addVolumeCursorClause(clauses *[]string, args *[]any, column string, direction sortDirection, primary any, id uuid.UUID, cast string) {
	operator := ">"
	if direction == sortDesc {
		operator = "<"
	}
	clause := fmt.Sprintf("(%s %s $%d%s OR (%s = $%d%s AND volumes.id > $%d))", column, operator, len(*args)+1, cast, column, len(*args)+1, cast, len(*args)+2)
	*clauses = append(*clauses, clause)
	*args = append(*args, primary, id)
}

func (s *Server) listVolumesPage(ctx context.Context, filter volumeListFilter, sort volumeListSort, pageSize int32, pageToken string) ([]volumeRecord, string, error) {
	limit := normalizePageSize(pageSize)
	clauses := []string{fmt.Sprintf("volumes.organization_id = $%d", 1)}
	args := []any{filter.OrganizationID}

	if len(filter.RunnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.runner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.RunnerIDs))
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.Statuses))
	}
	if filter.PendingSample {
		clauses = append(clauses, pendingSampleClause)
	}

	sortColumn, err := volumeSortColumn(sort.Field)
	if err != nil {
		return nil, "", err
	}

	if pageToken != "" {
		cursor, cursorID, err := decodeListCursor(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		primaryValue, err := volumeCursorPrimary(sort.Field, cursor)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		addVolumeCursorClause(&clauses, &args, sortColumn, sort.Direction, primaryValue, cursorID, volumeCursorCast(sort.Field))
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM volumes", volumeColumns))
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}
	query.WriteString(fmt.Sprintf(" ORDER BY %s %s, volumes.id ASC LIMIT $%d", sortColumn, sort.Direction, len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	volumes := make([]volumeRecord, 0, limit)
	var (
		lastRecord volumeRecord
		hasMore    bool
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
		lastRecord = volume
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		primary, err := volumePrimaryValue(volumeListItem{record: lastRecord}, sort.Field)
		if err != nil {
			return nil, "", err
		}
		nextToken, err = encodeListCursor(primary, lastRecord.Meta.ID)
		if err != nil {
			return nil, "", err
		}
	}
	return volumes, nextToken, nil
}

func (s *Server) listVolumesByNamePage(ctx context.Context, filter volumeListFilter, sort volumeListSort, pageSize int32, pageToken string, volumeNames map[uuid.UUID]string) ([]volumeRecord, string, error) {
	limit := normalizePageSize(pageSize)
	clauses := []string{fmt.Sprintf("volumes.organization_id = $%d", 1)}
	args := []any{filter.OrganizationID}

	if len(filter.RunnerIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.runner_id = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.RunnerIDs))
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("volumes.status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](filter.Statuses))
	}
	if filter.PendingSample {
		clauses = append(clauses, pendingSampleClause)
	}

	sortColumn, sortArgs, err := buildVolumeNameSortExpr(volumeNames, len(args)+1)
	if err != nil {
		return nil, "", err
	}
	args = append(args, sortArgs...)

	if pageToken != "" {
		cursor, cursorID, err := decodeListCursor(pageToken)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		primaryValue, err := volumeCursorPrimary(sort.Field, cursor)
		if err != nil {
			return nil, "", InvalidPageToken(err)
		}
		addVolumeCursorClause(&clauses, &args, sortColumn, sort.Direction, primaryValue, cursorID, "")
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM volumes", volumeColumns))
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}
	query.WriteString(fmt.Sprintf(" ORDER BY %s %s, volumes.id ASC LIMIT $%d", sortColumn, sort.Direction, len(args)+1))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	volumes := make([]volumeRecord, 0, limit)
	var (
		lastRecord volumeRecord
		hasMore    bool
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
		lastRecord = volume
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		name, ok := volumeNames[lastRecord.VolumeID]
		if !ok {
			return nil, "", fmt.Errorf("volume name missing for %s", lastRecord.VolumeID)
		}
		primary, err := volumePrimaryValue(volumeListItem{record: lastRecord, volumeName: name}, sort.Field)
		if err != nil {
			return nil, "", err
		}
		nextToken, err = encodeListCursor(primary, lastRecord.Meta.ID)
		if err != nil {
			return nil, "", err
		}
	}
	return volumes, nextToken, nil
}

func (s *Server) listVolumeAttachments(ctx context.Context, volumeID uuid.UUID) ([]*agentsv1.VolumeAttachment, error) {
	if s.agentsClient == nil {
		return nil, errors.New("agents client not configured")
	}
	attachmentsCtx := outgoingContext(ctx)
	attachments := []*agentsv1.VolumeAttachment{}
	pageToken := ""
	for {
		resp, err := s.agentsClient.ListVolumeAttachments(attachmentsCtx, &agentsv1.ListVolumeAttachmentsRequest{
			PageSize:  maxListPageSize,
			PageToken: pageToken,
			VolumeId:  volumeID.String(),
		})
		if err != nil {
			if isNotFoundGrpcError(err) {
				return []*agentsv1.VolumeAttachment{}, nil
			}
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

func (s *Server) ensureVolumeNames(ctx context.Context, cache *volumeEnrichmentCache, volumeIDs []uuid.UUID) error {
	missing := []uuid.UUID{}
	for _, volumeID := range uniqueUUIDs(volumeIDs) {
		if _, ok := cache.volumeNames[volumeID]; ok {
			continue
		}
		missing = append(missing, volumeID)
	}
	if len(missing) == 0 {
		return nil
	}
	resolved, err := s.resolveVolumeNames(ctx, missing)
	if err != nil {
		return err
	}
	for id, name := range resolved {
		cache.volumeNames[id] = name
	}
	return nil
}

func (s *Server) ensureAgentNames(ctx context.Context, cache *volumeEnrichmentCache, agentIDs []uuid.UUID) error {
	missing := []uuid.UUID{}
	for _, agentID := range uniqueUUIDs(agentIDs) {
		if _, ok := cache.agentNames[agentID]; ok {
			continue
		}
		missing = append(missing, agentID)
	}
	if len(missing) == 0 {
		return nil
	}
	resolved, err := s.resolveAgentNames(ctx, missing)
	if err != nil {
		return err
	}
	for id, name := range resolved {
		cache.agentNames[id] = name
	}
	return nil
}

func (s *Server) ensureMcpNames(ctx context.Context, cache *volumeEnrichmentCache, mcpIDs []uuid.UUID) error {
	for _, mcpID := range uniqueUUIDs(mcpIDs) {
		if _, ok := cache.mcpNames[mcpID]; ok {
			continue
		}
		name, err := s.resolveMcpName(ctx, mcpID)
		if err != nil {
			return err
		}
		cache.mcpNames[mcpID] = name
	}
	return nil
}

func (s *Server) ensureHookNames(ctx context.Context, cache *volumeEnrichmentCache, hookIDs []uuid.UUID) error {
	for _, hookID := range uniqueUUIDs(hookIDs) {
		if _, ok := cache.hookNames[hookID]; ok {
			continue
		}
		name, err := s.resolveHookName(ctx, hookID)
		if err != nil {
			return err
		}
		cache.hookNames[hookID] = name
	}
	return nil
}

func (s *Server) ensureVolumeAttachments(ctx context.Context, cache *volumeEnrichmentCache, volumeIDs []uuid.UUID) error {
	missing := []uuid.UUID{}
	for _, volumeID := range uniqueUUIDs(volumeIDs) {
		if _, ok := cache.attachments[volumeID]; ok {
			continue
		}
		missing = append(missing, volumeID)
	}
	if len(missing) == 0 {
		return nil
	}

	attachmentsByVolume := make(map[uuid.UUID][]*agentsv1.VolumeAttachment, len(missing))
	attachmentAgentIDs := []uuid.UUID{}
	mcpIDs := []uuid.UUID{}
	hookIDs := []uuid.UUID{}

	for _, volumeID := range missing {
		attachments, err := s.listVolumeAttachments(ctx, volumeID)
		if err != nil {
			return fmt.Errorf("list volume attachments: %w", err)
		}
		attachmentsByVolume[volumeID] = attachments
		for _, attachment := range attachments {
			if attachment.GetAgentId() != "" {
				parsed, err := parseUUID(attachment.GetAgentId())
				if err != nil {
					return fmt.Errorf("attachment agent_id: %w", err)
				}
				attachmentAgentIDs = append(attachmentAgentIDs, parsed)
				continue
			}
			if attachment.GetMcpId() != "" {
				parsed, err := parseUUID(attachment.GetMcpId())
				if err != nil {
					return fmt.Errorf("attachment mcp_id: %w", err)
				}
				mcpIDs = append(mcpIDs, parsed)
				continue
			}
			if attachment.GetHookId() != "" {
				parsed, err := parseUUID(attachment.GetHookId())
				if err != nil {
					return fmt.Errorf("attachment hook_id: %w", err)
				}
				hookIDs = append(hookIDs, parsed)
				continue
			}
			return errors.New("attachment target missing")
		}
	}

	if err := s.ensureAgentNames(ctx, cache, attachmentAgentIDs); err != nil {
		return err
	}
	if err := s.ensureMcpNames(ctx, cache, mcpIDs); err != nil {
		return err
	}
	if err := s.ensureHookNames(ctx, cache, hookIDs); err != nil {
		return err
	}

	for _, volumeID := range missing {
		attachments := attachmentsByVolume[volumeID]
		protoAttachments := make([]*runnersv1.Attachment, 0, len(attachments))
		for _, attachment := range attachments {
			switch {
			case attachment.GetAgentId() != "":
				parsed, err := parseUUID(attachment.GetAgentId())
				if err != nil {
					return fmt.Errorf("attachment agent_id: %w", err)
				}
				name, ok := cache.agentNames[parsed]
				if !ok {
					return fmt.Errorf("agent name missing for %s", parsed)
				}
				protoAttachments = append(protoAttachments, &runnersv1.Attachment{
					Kind: runnersv1.AttachmentKind_ATTACHMENT_KIND_AGENT,
					Id:   parsed.String(),
					Name: name,
				})
			case attachment.GetMcpId() != "":
				parsed, err := parseUUID(attachment.GetMcpId())
				if err != nil {
					return fmt.Errorf("attachment mcp_id: %w", err)
				}
				name, ok := cache.mcpNames[parsed]
				if !ok {
					return fmt.Errorf("mcp name missing for %s", parsed)
				}
				protoAttachments = append(protoAttachments, &runnersv1.Attachment{
					Kind: runnersv1.AttachmentKind_ATTACHMENT_KIND_MCP,
					Id:   parsed.String(),
					Name: name,
				})
			case attachment.GetHookId() != "":
				parsed, err := parseUUID(attachment.GetHookId())
				if err != nil {
					return fmt.Errorf("attachment hook_id: %w", err)
				}
				name, ok := cache.hookNames[parsed]
				if !ok {
					return fmt.Errorf("hook name missing for %s", parsed)
				}
				protoAttachments = append(protoAttachments, &runnersv1.Attachment{
					Kind: runnersv1.AttachmentKind_ATTACHMENT_KIND_HOOK,
					Id:   parsed.String(),
					Name: name,
				})
			default:
				return errors.New("attachment target missing")
			}
		}
		cache.attachments[volumeID] = protoAttachments
	}
	return nil
}

func (s *Server) buildVolumeItems(ctx context.Context, records []volumeRecord, cache *volumeEnrichmentCache, includeAttachments bool) ([]volumeListItem, error) {
	if len(records) == 0 {
		return []volumeListItem{}, nil
	}
	volumeIDs := make([]uuid.UUID, 0, len(records))
	for _, record := range records {
		volumeIDs = append(volumeIDs, record.VolumeID)
	}
	if err := s.ensureVolumeNames(ctx, cache, volumeIDs); err != nil {
		return nil, err
	}
	if includeAttachments {
		if err := s.ensureVolumeAttachments(ctx, cache, volumeIDs); err != nil {
			return nil, err
		}
	}

	items := make([]volumeListItem, 0, len(records))
	for _, record := range records {
		name, ok := cache.volumeNames[record.VolumeID]
		if !ok {
			return nil, fmt.Errorf("volume name missing for %s", record.VolumeID)
		}
		attachments := []*runnersv1.Attachment{}
		if includeAttachments {
			attachments = cache.attachments[record.VolumeID]
		}
		items = append(items, volumeListItem{record: record, volumeName: name, attachments: attachments})
	}
	return items, nil
}

func filterVolumeItemsByName(items []volumeListItem, nameFilter string) []volumeListItem {
	if len(items) == 0 {
		return items
	}
	nameFilter = strings.ToLower(strings.TrimSpace(nameFilter))
	if nameFilter == "" {
		return items
	}
	filtered := make([]volumeListItem, 0, len(items))
	for _, item := range items {
		if strings.Contains(strings.ToLower(item.volumeName), nameFilter) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func matchesVolumeAttachmentKinds(attachments []*runnersv1.Attachment, kindFilter map[runnersv1.VolumeAttachmentFilterKind]bool) bool {
	if len(kindFilter) == 0 {
		return true
	}
	if len(attachments) == 0 {
		return kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_UNATTACHED]
	}
	for _, attachment := range attachments {
		switch attachment.GetKind() {
		case runnersv1.AttachmentKind_ATTACHMENT_KIND_AGENT:
			if kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_AGENT] {
				return true
			}
		case runnersv1.AttachmentKind_ATTACHMENT_KIND_MCP:
			if kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_MCP] {
				return true
			}
		case runnersv1.AttachmentKind_ATTACHMENT_KIND_HOOK:
			if kindFilter[runnersv1.VolumeAttachmentFilterKind_VOLUME_ATTACHMENT_FILTER_KIND_HOOK] {
				return true
			}
		}
	}
	return false
}

func filterVolumeItemsByAttachmentKind(items []volumeListItem, filterKinds []runnersv1.VolumeAttachmentFilterKind) []volumeListItem {
	if len(items) == 0 {
		return items
	}
	if len(filterKinds) == 0 {
		return items
	}
	kindFilter := map[runnersv1.VolumeAttachmentFilterKind]bool{}
	for _, kind := range filterKinds {
		kindFilter[kind] = true
	}
	filtered := make([]volumeListItem, 0, len(items))
	for _, item := range items {
		if matchesVolumeAttachmentKinds(item.attachments, kindFilter) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func filterVolumeItems(items []volumeListItem, filter volumeListFilter) []volumeListItem {
	items = filterVolumeItemsByName(items, filter.VolumeNameContains)
	return filterVolumeItemsByAttachmentKind(items, filter.AttachedKinds)
}

func sortVolumeItems(items []volumeListItem, sortSpec volumeListSort) error {
	var sortErr error
	sort.SliceStable(items, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		primaryCmp, err := compareVolumePrimary(items[i], items[j], sortSpec.Field)
		if err != nil {
			sortErr = err
			return false
		}
		if primaryCmp == 0 {
			return compareUUID(items[i].record.Meta.ID, items[j].record.Meta.ID) < 0
		}
		if sortSpec.Direction == sortAsc {
			return primaryCmp < 0
		}
		return primaryCmp > 0
	})
	return sortErr
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
