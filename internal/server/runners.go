package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	identityv1 "github.com/agynio/runners/.gen/go/agynio/api/identity/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
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

	runnerColumns = `id, name, organization_id, identity_id, status, created_at, updated_at`
)

type runnerRecord struct {
	Meta           entityMeta
	Name           string
	OrganizationID *uuid.UUID
	IdentityID     uuid.UUID
	Status         string
}

type runnerInsertInput struct {
	ID               uuid.UUID
	Name             string
	OrganizationID   *uuid.UUID
	IdentityID       uuid.UUID
	ServiceTokenHash string
	Status           string
}

func (s *Server) RegisterRunner(ctx context.Context, req *runnersv1.RegisterRunnerRequest) (*runnersv1.RegisterRunnerResponse, error) {
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name must be provided")
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

func (s *Server) writeRunnerAuthorization(ctx context.Context, runnerID uuid.UUID, organizationID *uuid.UUID) error {
	tuple := runnerAuthorizationTuple(runnerID, organizationID)
	if tuple == nil {
		return status.Error(codes.Internal, "authorization tuple unavailable")
	}
	if _, err := s.authorizationClient.Write(ctx, &authorizationv1.WriteRequest{Writes: []*authorizationv1.TupleKey{tuple}}); err != nil {
		return status.Errorf(codes.Internal, "authorization write: %v", err)
	}
	return nil
}

func (s *Server) cleanupRunnerAuthorization(ctx context.Context, runnerID uuid.UUID, organizationID *uuid.UUID) {
	tuple := runnerAuthorizationTuple(runnerID, organizationID)
	if tuple == nil {
		return
	}
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
	row := s.pool.QueryRow(ctx,
		fmt.Sprintf(`INSERT INTO runners (id, name, organization_id, identity_id, service_token_hash, status)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING %s`, runnerColumns),
		input.ID,
		input.Name,
		pgtype.UUID{Bytes: input.OrganizationIDBytes(), Valid: input.OrganizationID != nil},
		input.IdentityID,
		input.ServiceTokenHash,
		input.Status,
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
	limit := normalizePageSize(pageSize)

	var (
		clauses []string
		args    []any
	)
	if organizationID != nil {
		clauses = append(clauses, fmt.Sprintf("(organization_id = $%d OR organization_id IS NULL)", len(args)+1))
		args = append(args, *organizationID)
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
	query.WriteString(fmt.Sprintf("SELECT %s FROM runners", runnerColumns))
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

	runners := make([]runnerRecord, 0, limit)
	var (
		lastID  uuid.UUID
		hasMore bool
	)
	for rows.Next() {
		if int32(len(runners)) == limit {
			hasMore = true
			break
		}
		runner, err := scanRunner(rows)
		if err != nil {
			return nil, "", err
		}
		runners = append(runners, runner)
		lastID = runner.Meta.ID
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextToken := ""
	if hasMore {
		nextToken = encodePageToken(lastID)
	}
	return runners, nextToken, nil
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

func scanRunner(row pgx.Row) (runnerRecord, error) {
	var (
		runner runnerRecord
		orgID  pgtype.UUID
	)
	if err := row.Scan(
		&runner.Meta.ID,
		&runner.Name,
		&orgID,
		&runner.IdentityID,
		&runner.Status,
		&runner.Meta.CreatedAt,
		&runner.Meta.UpdatedAt,
	); err != nil {
		return runnerRecord{}, err
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
