package server

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	identityv1 "github.com/agynio/runners/.gen/go/agynio/api/identity/v1"
	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	zitimanagementv1 "github.com/agynio/runners/.gen/go/agynio/api/ziti_management/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultListPageSize int32 = 50
	maxListPageSize     int32 = 100

	clusterObject              = "cluster:global"
	clusterWriterRelation      = "writer"
	organizationMemberRelation = "member"
	identityObjectPrefix       = "identity:"
	organizationObjectPrefix   = "organization:"
)

// Server implements the RunnersService gRPC API.
type Server struct {
	runnersv1.UnimplementedRunnersServiceServer
	pool                 dbPool
	identityClient       identityv1.IdentityServiceClient
	authorizationClient  authorizationv1.AuthorizationServiceClient
	zitiManagementClient zitimanagementv1.ZitiManagementServiceClient
}

// Options defines required inputs for constructing a Server.
type Options struct {
	Pool                 dbPool
	IdentityClient       identityv1.IdentityServiceClient
	AuthorizationClient  authorizationv1.AuthorizationServiceClient
	ZitiManagementClient zitimanagementv1.ZitiManagementServiceClient
}

// New constructs a RunnersService server.
func New(options Options) *Server {
	return &Server{
		pool:                 options.Pool,
		identityClient:       options.IdentityClient,
		authorizationClient:  options.AuthorizationClient,
		zitiManagementClient: options.ZitiManagementClient,
	}
}

type entityMeta struct {
	ID        uuid.UUID
	CreatedAt time.Time
	UpdatedAt time.Time
}

func toProtoEntityMeta(meta entityMeta) *runnersv1.EntityMeta {
	return &runnersv1.EntityMeta{
		Id:        meta.ID.String(),
		CreatedAt: timestamppb.New(meta.CreatedAt),
		UpdatedAt: timestamppb.New(meta.UpdatedAt),
	}
}

func normalizePageSize(size int32) int32 {
	if size <= 0 {
		return defaultListPageSize
	}
	if size > maxListPageSize {
		return maxListPageSize
	}
	return size
}

func encodePageToken(id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id.String()))
}

func decodePageToken(token string) (uuid.UUID, error) {
	if token == "" {
		return uuid.UUID{}, errors.New("empty token")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("decode token: %w", err)
	}
	value, err := uuid.Parse(string(decoded))
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("parse token: %w", err)
	}
	return value, nil
}

func parseUUID(value string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return uuid.UUID{}, fmt.Errorf("value is empty")
	}
	id, err := uuid.Parse(trimmed)
	if err != nil {
		return uuid.UUID{}, err
	}
	return id, nil
}

func identityObject(id uuid.UUID) string {
	return identityObjectPrefix + id.String()
}

func organizationObject(id uuid.UUID) string {
	return organizationObjectPrefix + id.String()
}
