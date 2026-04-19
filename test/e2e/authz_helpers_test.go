//go:build e2e

package e2e

import (
	"context"
	"testing"

	authorizationv1 "github.com/agynio/runners/.gen/go/agynio/api/authorization/v1"
)

const (
	authorizationIdentityPrefix     = "identity:"
	authorizationOrganizationPrefix = "organization:"
	authorizationMemberRelation     = "member"
)

func ensureOrganizationMember(t *testing.T, ctx context.Context, identityID string, organizationID string) {
	t.Helper()
	if authzClient == nil {
		t.Fatal("authorization client not initialized")
	}
	tuple := &authorizationv1.TupleKey{
		User:     authorizationIdentityPrefix + identityID,
		Relation: authorizationMemberRelation,
		Object:   authorizationOrganizationPrefix + organizationID,
	}
	if _, err := authzClient.Write(ctx, &authorizationv1.WriteRequest{Writes: []*authorizationv1.TupleKey{tuple}}); err != nil {
		t.Fatalf("authorization write failed: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), testTimeout)
		defer cleanupCancel()
		_, _ = authzClient.Write(cleanupCtx, &authorizationv1.WriteRequest{Deletes: []*authorizationv1.TupleKey{tuple}})
	})
}
