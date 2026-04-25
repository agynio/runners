package server

import (
	"context"
	"testing"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

func TestResolveAgentNamesForwardsMetadata(t *testing.T) {
	agentID := uuid.New()
	callerID := uuid.New()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(identityMetadata, callerID.String()))

	var gotMetadata metadata.MD
	agentsClient := fakeAgentsClient{getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
		md, _ := metadata.FromOutgoingContext(ctx)
		gotMetadata = md
		return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Name: "agent"}}, nil
	}}

	srv := New(Options{AgentsClient: agentsClient})
	_, err := srv.resolveAgentNames(ctx, []uuid.UUID{agentID})
	if err != nil {
		t.Fatalf("resolveAgentNames failed: %v", err)
	}
	if gotMetadata == nil {
		t.Fatal("expected outgoing metadata")
	}
	values := gotMetadata.Get(identityMetadata)
	if len(values) != 1 || values[0] != callerID.String() {
		t.Fatalf("expected %s to be forwarded, got %v", identityMetadata, values)
	}
}
