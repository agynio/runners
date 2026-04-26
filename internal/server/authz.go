package server

import (
	"context"
	"fmt"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func identityFromMetadata(ctx context.Context) (uuid.UUID, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return uuid.UUID{}, fmt.Errorf("metadata missing")
	}
	values := md.Get(identityMetadata)
	if len(values) != 1 {
		return uuid.UUID{}, fmt.Errorf("expected single value")
	}
	return parseUUID(values[0])
}

func (s *Server) requireClusterAdmin(ctx context.Context, identityID uuid.UUID) error {
	return s.requireRelation(ctx, identityID, clusterAdminRelation, clusterObject)
}

func (s *Server) requireOrgOwner(ctx context.Context, identityID uuid.UUID, organizationID uuid.UUID) error {
	return s.requireRelation(ctx, identityID, organizationOwnerRelation, organizationObject(organizationID))
}

func (s *Server) requireOrgMember(ctx context.Context, identityID uuid.UUID, organizationID uuid.UUID) error {
	return s.requireRelation(ctx, identityID, organizationMemberRelation, organizationObject(organizationID))
}

func (s *Server) orgRelationAllowed(ctx context.Context, identityID uuid.UUID, organizationID uuid.UUID, relation string) (bool, error) {
	return s.relationAllowed(ctx, identityID, relation, organizationObject(organizationID))
}

func (s *Server) relationAllowed(ctx context.Context, identityID uuid.UUID, relation string, object string) (bool, error) {
	resp, err := s.authorizationClient.Check(ctx, &authorizationv1.CheckRequest{
		TupleKey: &authorizationv1.TupleKey{
			User:     identityObject(identityID),
			Relation: relation,
			Object:   object,
		},
	})
	if err != nil {
		return false, status.Errorf(codes.Internal, "authorization check: %v", err)
	}
	return resp.GetAllowed(), nil
}

func (s *Server) requireRelation(ctx context.Context, identityID uuid.UUID, relation string, object string) error {
	allowed, err := s.relationAllowed(ctx, identityID, relation, object)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	return nil
}

func (s *Server) memberAllowed(ctx context.Context, identityID uuid.UUID, organizationID uuid.UUID, cache map[uuid.UUID]bool) (bool, error) {
	if allowed, ok := cache[organizationID]; ok {
		return allowed, nil
	}
	allowed, err := s.orgRelationAllowed(ctx, identityID, organizationID, organizationMemberRelation)
	if err != nil {
		return false, err
	}
	cache[organizationID] = allowed
	return allowed, nil
}
