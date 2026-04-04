package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	identityv1 "github.com/agynio/runners/.gen/go/agynio/api/identity/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/runners/.gen/go/agynio/api/ziti_management/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	runnerStatusPending  = "pending"
	runnerStatusEnrolled = "enrolled"
	runnerStatusOffline  = "offline"

	zitiRunnerRoleAttribute = "runners"

	runnerColumns = `id, name, organization_id, identity_id, status, labels, created_at, updated_at`
)

type runnerRecord struct {
	Meta           entityMeta
	Name           string
	OrganizationID *uuid.UUID
	IdentityID     uuid.UUID
	Status         string
	Labels         map[string]string
}

type runnerInsertInput struct {
	ID               uuid.UUID
	Name             string
	OrganizationID   *uuid.UUID
	IdentityID       uuid.UUID
	ServiceTokenHash string
	Status           string
	Labels           map[string]string
}

type runnerUpdateInput struct {
	ID     uuid.UUID
	Name   *string
	Labels *map[string]string
}

func (s *Server) RegisterRunner(ctx context.Context, req *runnersv1.RegisterRunnerRequest) (*runnersv1.RegisterRunnerResponse, error) {
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name must be provided")
	}
	labels := req.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	var organizationID *uuid.UUID
	if req.OrganizationId != nil {
		parsed, err := parseUUID(req.GetOrganizationId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		organizationID = &parsed
	}

	runnerID := uuid.New()

	if _, err := s.identityClient.RegisterIdentity(ctx, &identityv1.RegisterIdentityRequest{
		IdentityId:   runnerID.String(),
		IdentityType: identityv1.IdentityType_IDENTITY_TYPE_RUNNER,
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "register identity: %v", err)
	}

	if err := s.writeRunnerAuthorization(ctx, runnerID, organizationID); err != nil {
		return nil, err
	}

	token, tokenHash, err := generateServiceToken()
	if err != nil {
		s.cleanupRunnerAuthorization(ctx, runnerID, organizationID)
		return nil, status.Errorf(codes.Internal, "generate service token: %v", err)
	}

	runner, err := s.insertRunner(ctx, runnerInsertInput{
		ID:               runnerID,
		Name:             name,
		OrganizationID:   organizationID,
		IdentityID:       runnerID,
		ServiceTokenHash: tokenHash,
		Status:           runnerStatusPending,
		Labels:           labels,
	})
	if err != nil {
		s.cleanupRunnerAuthorization(ctx, runnerID, organizationID)
		return nil, toStatusError(err)
	}

	protoRunner, err := toProtoRunner(runner)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
	}
	return &runnersv1.RegisterRunnerResponse{Runner: protoRunner, ServiceToken: token}, nil
}

func (s *Server) GetRunner(ctx context.Context, req *runnersv1.GetRunnerRequest) (*runnersv1.GetRunnerResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}
	runner, err := s.getRunnerByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}
	protoRunner, err := toProtoRunner(runner)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
	}
	return &runnersv1.GetRunnerResponse{Runner: protoRunner}, nil
}

func (s *Server) UpdateRunner(ctx context.Context, req *runnersv1.UpdateRunnerRequest) (*runnersv1.UpdateRunnerResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}

	var name *string
	if req.Name != nil {
		trimmed := strings.TrimSpace(req.GetName())
		if trimmed == "" {
			return nil, status.Error(codes.InvalidArgument, "name must be provided")
		}
		name = &trimmed
	}

	var labels *map[string]string
	if req.Labels != nil {
		value := req.Labels
		labels = &value
	}

	runner, err := s.updateRunner(ctx, runnerUpdateInput{ID: id, Name: name, Labels: labels})
	if err != nil {
		return nil, toStatusError(err)
	}
	protoRunner, err := toProtoRunner(runner)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
	}
	return &runnersv1.UpdateRunnerResponse{Runner: protoRunner}, nil
}

func (s *Server) ListRunners(ctx context.Context, req *runnersv1.ListRunnersRequest) (*runnersv1.ListRunnersResponse, error) {
	var organizationID *uuid.UUID
	if req.OrganizationId != nil {
		parsed, err := parseUUID(req.GetOrganizationId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		organizationID = &parsed
	}

	runners, nextToken, err := s.listRunners(ctx, organizationID, req.GetPageSize(), req.GetPageToken())
	if err != nil {
		var invalidToken *InvalidPageTokenError
		if errors.As(err, &invalidToken) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", invalidToken.Err)
		}
		return nil, status.Errorf(codes.Internal, "list runners: %v", err)
	}

	protoRunners := make([]*runnersv1.Runner, 0, len(runners))
	for _, runner := range runners {
		protoRunner, err := toProtoRunner(runner)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
		}
		protoRunners = append(protoRunners, protoRunner)
	}
	return &runnersv1.ListRunnersResponse{Runners: protoRunners, NextPageToken: nextToken}, nil
}

func (s *Server) DeleteRunner(ctx context.Context, req *runnersv1.DeleteRunnerRequest) (*runnersv1.DeleteRunnerResponse, error) {
	id, err := parseUUID(req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "id: %v", err)
	}

	runner, err := s.getRunnerByID(ctx, id)
	if err != nil {
		return nil, toStatusError(err)
	}

	s.cleanupRunnerAuthorization(ctx, runner.IdentityID, runner.OrganizationID)

	if err := s.deleteRunner(ctx, id); err != nil {
		return nil, toStatusError(err)
	}
	return &runnersv1.DeleteRunnerResponse{}, nil
}

func (s *Server) ValidateServiceToken(ctx context.Context, req *runnersv1.ValidateServiceTokenRequest) (*runnersv1.ValidateServiceTokenResponse, error) {
	token := strings.TrimSpace(req.GetTokenHash())
	if token == "" {
		return nil, status.Error(codes.InvalidArgument, "token_hash must be provided")
	}
	tokenHash := hashServiceToken(token)
	runner, err := s.getRunnerByServiceTokenHash(ctx, tokenHash)
	if err != nil {
		return nil, toStatusError(err)
	}
	protoRunner, err := toProtoRunner(runner)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "convert runner: %v", err)
	}
	return &runnersv1.ValidateServiceTokenResponse{Runner: protoRunner}, nil
}

func (s *Server) EnrollRunner(ctx context.Context, req *runnersv1.EnrollRunnerRequest) (*runnersv1.EnrollRunnerResponse, error) {
	serviceToken := strings.TrimSpace(req.GetServiceToken())
	if serviceToken == "" {
		return nil, status.Error(codes.InvalidArgument, "service_token must be provided")
	}

	tokenHash := hashServiceToken(serviceToken)
	runner, err := s.getRunnerByServiceTokenHash(ctx, tokenHash)
	if err != nil {
		return nil, toStatusError(err)
	}

	zitiResp, err := s.zitiManagementClient.CreateRunnerIdentity(ctx, &zitimanagementv1.CreateRunnerIdentityRequest{
		RunnerId:       runner.Meta.ID.String(),
		RoleAttributes: []string{zitiRunnerRoleAttribute},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create ziti identity: %v", err)
	}

	if err := s.updateRunnerStatus(ctx, runner.Meta.ID, runnerStatusEnrolled); err != nil {
		return nil, toStatusError(err)
	}

	return &runnersv1.EnrollRunnerResponse{
		IdentityJson: zitiResp.GetIdentityJson(),
		ServiceName:  zitiResp.GetZitiServiceName(),
		IdentityId:   zitiResp.GetZitiIdentityId(),
	}, nil
}

func (s *Server) writeRunnerAuthorization(ctx context.Context, runnerID uuid.UUID, organizationID *uuid.UUID) error {
	tuple := runnerAuthorizationTuple(runnerID, organizationID)
	if _, err := s.authorizationClient.Write(ctx, &authorizationv1.WriteRequest{Writes: []*authorizationv1.TupleKey{tuple}}); err != nil {
		return status.Errorf(codes.Internal, "authorization write: %v", err)
	}
	return nil
}

func (s *Server) cleanupRunnerAuthorization(ctx context.Context, runnerID uuid.UUID, organizationID *uuid.UUID) {
	tuple := runnerAuthorizationTuple(runnerID, organizationID)
	_, _ = s.authorizationClient.Write(ctx, &authorizationv1.WriteRequest{Deletes: []*authorizationv1.TupleKey{tuple}})
}

func runnerAuthorizationTuple(runnerID uuid.UUID, organizationID *uuid.UUID) *authorizationv1.TupleKey {
	if organizationID != nil {
		return &authorizationv1.TupleKey{
			User:     identityObject(runnerID),
			Relation: organizationMemberRelation,
			Object:   organizationObject(*organizationID),
		}
	}
	return &authorizationv1.TupleKey{
		User:     identityObject(runnerID),
		Relation: clusterWriterRelation,
		Object:   clusterObject,
	}
}

func (s *Server) insertRunner(ctx context.Context, input runnerInsertInput) (runnerRecord, error) {
	labelsJSON, err := json.Marshal(input.Labels)
	if err != nil {
		return runnerRecord{}, err
	}
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`INSERT INTO runners (id, name, organization_id, identity_id, service_token_hash, status, labels)
	    VALUES ($1, $2, $3, $4, $5, $6, $7)
	    RETURNING %s`, runnerColumns),
		input.ID,
		input.Name,
		pgtype.UUID{Bytes: input.OrganizationIDBytes(), Valid: input.OrganizationID != nil},
		input.IdentityID,
		input.ServiceTokenHash,
		input.Status,
		labelsJSON,
	)
	runner, err := scanRunner(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return runnerRecord{}, AlreadyExists("runner")
		}
		return runnerRecord{}, err
	}
	return runner, nil
}

func (input runnerInsertInput) OrganizationIDBytes() uuid.UUID {
	if input.OrganizationID == nil {
		return uuid.UUID{}
	}
	return *input.OrganizationID
}

func (s *Server) getRunnerByID(ctx context.Context, id uuid.UUID) (runnerRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT %s FROM runners WHERE id = $1`, runnerColumns),
		id,
	)
	runner, err := scanRunner(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runnerRecord{}, NotFound("runner")
		}
		return runnerRecord{}, err
	}
	return runner, nil
}

func (s *Server) getRunnerByServiceTokenHash(ctx context.Context, tokenHash string) (runnerRecord, error) {
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT %s FROM runners WHERE service_token_hash = $1`, runnerColumns),
		tokenHash,
	)
	runner, err := scanRunner(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runnerRecord{}, NotFound("runner")
		}
		return runnerRecord{}, err
	}
	return runner, nil
}

func (s *Server) listRunners(ctx context.Context, organizationID *uuid.UUID, pageSize int32, pageToken string) ([]runnerRecord, string, error) {
	var (
		clauses []string
		args    []any
	)
	if organizationID != nil {
		clauses = append(clauses, fmt.Sprintf("(organization_id = $%d OR organization_id IS NULL)", len(args)+1))
		args = append(args, *organizationID)
	}
	return listWithPagination(ctx, s.pool, fmt.Sprintf("SELECT %s FROM runners", runnerColumns), clauses, args, pageSize, pageToken, scanRunner, func(record runnerRecord) uuid.UUID {
		return record.Meta.ID
	})
}

func (s *Server) deleteRunner(ctx context.Context, id uuid.UUID) error {
	result, err := s.pool.Exec(ctx, `DELETE FROM runners WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return NotFound("runner")
	}
	return nil
}

func (s *Server) updateRunner(ctx context.Context, input runnerUpdateInput) (runnerRecord, error) {
	labelsValue := pgtype.Text{Valid: false}
	if input.Labels != nil {
		labelsJSON, err := json.Marshal(*input.Labels)
		if err != nil {
			return runnerRecord{}, err
		}
		labelsValue = pgtype.Text{String: string(labelsJSON), Valid: true}
	}

	nameValue := pgtype.Text{Valid: false}
	if input.Name != nil {
		nameValue = pgtype.Text{String: *input.Name, Valid: true}
	}

	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`UPDATE runners SET name = COALESCE($1, name), labels = COALESCE($2::jsonb, labels), updated_at = NOW() WHERE id = $3 RETURNING %s`, runnerColumns),
		nameValue,
		labelsValue,
		input.ID,
	)
	runner, err := scanRunner(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runnerRecord{}, NotFound("runner")
		}
		return runnerRecord{}, err
	}
	return runner, nil
}

func (s *Server) updateRunnerStatus(ctx context.Context, id uuid.UUID, statusValue string) error {
	result, err := s.pool.Exec(ctx, `UPDATE runners SET status = $1, updated_at = NOW() WHERE id = $2`, statusValue, id)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return NotFound("runner")
	}
	return nil
}

func scanRunner(row pgx.Row) (runnerRecord, error) {
	var (
		runner     runnerRecord
		orgID      pgtype.UUID
		labelsData []byte
	)
	if err := row.Scan(
		&runner.Meta.ID,
		&runner.Name,
		&orgID,
		&runner.IdentityID,
		&runner.Status,
		&labelsData,
		&runner.Meta.CreatedAt,
		&runner.Meta.UpdatedAt,
	); err != nil {
		return runnerRecord{}, err
	}
	if err := json.Unmarshal(labelsData, &runner.Labels); err != nil {
		return runnerRecord{}, err
	}
	if runner.Labels == nil {
		runner.Labels = map[string]string{}
	}
	if orgID.Valid {
		value := uuid.UUID(orgID.Bytes)
		runner.OrganizationID = &value
	}
	return runner, nil
}

func toProtoRunner(record runnerRecord) (*runnersv1.Runner, error) {
	status, err := runnerStatusToProto(record.Status)
	if err != nil {
		return nil, err
	}
	runner := &runnersv1.Runner{
		Meta:       toProtoEntityMeta(record.Meta),
		Name:       record.Name,
		IdentityId: record.IdentityID.String(),
		Status:     status,
		Labels:     record.Labels,
	}
	if record.OrganizationID != nil {
		value := record.OrganizationID.String()
		runner.OrganizationId = &value
	}
	return runner, nil
}

func runnerStatusToProto(value string) (runnersv1.RunnerStatus, error) {
	switch value {
	case runnerStatusPending:
		return runnersv1.RunnerStatus_RUNNER_STATUS_PENDING, nil
	case runnerStatusEnrolled:
		return runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED, nil
	case runnerStatusOffline:
		return runnersv1.RunnerStatus_RUNNER_STATUS_OFFLINE, nil
	default:
		return runnersv1.RunnerStatus_RUNNER_STATUS_UNSPECIFIED, fmt.Errorf("invalid runner status: %s", value)
	}
}
