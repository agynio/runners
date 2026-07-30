package server

import (
	"context"
	"testing"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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

func TestResolveAgentNamesNotFoundUsesPlaceholder(t *testing.T) {
	agentID := uuid.New()
	agentsClient := fakeAgentsClient{getAgent: func(ctx context.Context, req *agentsv1.GetAgentRequest) (*agentsv1.GetAgentResponse, error) {
		return nil, status.Error(codes.NotFound, "agent not found")
	}}

	srv := New(Options{AgentsClient: agentsClient})
	resolved, err := srv.resolveAgentNames(context.Background(), []uuid.UUID{agentID})
	if err != nil {
		t.Fatalf("resolveAgentNames failed: %v", err)
	}
	name, ok := resolved[agentID]
	if !ok {
		t.Fatalf("expected agent %s to be resolved", agentID)
	}
	if name != missingNamePlaceholder {
		t.Fatalf("expected placeholder name %q, got %q", missingNamePlaceholder, name)
	}
}

func TestResolveSandboxNamesResolvesByID(t *testing.T) {
	sandboxID := uuid.New()
	sandboxName := "sandbox-name"
	var gotID string
	agentsClient := fakeAgentsClient{getSandbox: func(ctx context.Context, req *agentsv1.GetSandboxRequest) (*agentsv1.GetSandboxResponse, error) {
		gotID = req.GetId()
		return &agentsv1.GetSandboxResponse{Sandbox: &agentsv1.Sandbox{Name: sandboxName}}, nil
	}}

	srv := New(Options{AgentsClient: agentsClient})
	resolved, err := srv.resolveSandboxNames(context.Background(), []uuid.UUID{sandboxID})
	if err != nil {
		t.Fatalf("resolveSandboxNames failed: %v", err)
	}
	if gotID != sandboxID.String() {
		t.Fatalf("expected sandbox id %s, got %s", sandboxID, gotID)
	}
	if resolved[sandboxID] != sandboxName {
		t.Fatalf("expected sandbox name %q, got %q", sandboxName, resolved[sandboxID])
	}
}

func TestResolveSandboxNamesNotFoundUsesPlaceholder(t *testing.T) {
	sandboxID := uuid.New()
	agentsClient := fakeAgentsClient{getSandbox: func(ctx context.Context, req *agentsv1.GetSandboxRequest) (*agentsv1.GetSandboxResponse, error) {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}}

	srv := New(Options{AgentsClient: agentsClient})
	resolved, err := srv.resolveSandboxNames(context.Background(), []uuid.UUID{sandboxID})
	if err != nil {
		t.Fatalf("resolveSandboxNames failed: %v", err)
	}
	name, ok := resolved[sandboxID]
	if !ok {
		t.Fatalf("expected sandbox %s to be resolved", sandboxID)
	}
	if name != missingNamePlaceholder {
		t.Fatalf("expected placeholder name %q, got %q", missingNamePlaceholder, name)
	}
}

func TestResolveVolumeNamesNotFoundUsesPlaceholder(t *testing.T) {
	volumeID := uuid.New()
	agentsClient := fakeAgentsClient{getVolume: func(ctx context.Context, req *agentsv1.GetVolumeRequest) (*agentsv1.GetVolumeResponse, error) {
		return nil, status.Error(codes.NotFound, "volume not found")
	}}

	srv := New(Options{AgentsClient: agentsClient})
	resolved, err := srv.resolveVolumeNames(context.Background(), []uuid.UUID{volumeID})
	if err != nil {
		t.Fatalf("resolveVolumeNames failed: %v", err)
	}
	name, ok := resolved[volumeID]
	if !ok {
		t.Fatalf("expected volume %s to be resolved", volumeID)
	}
	if name != missingNamePlaceholder {
		t.Fatalf("expected placeholder name %q, got %q", missingNamePlaceholder, name)
	}
}
