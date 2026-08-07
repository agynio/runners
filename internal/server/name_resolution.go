package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const missingNamePlaceholder = "unknown"

func (s *Server) resolveAgentNames(ctx context.Context, agentIDs []uuid.UUID) (map[uuid.UUID]string, error) {
	if len(agentIDs) == 0 {
		return map[uuid.UUID]string{}, nil
	}
	if s.agentsClient == nil {
		return nil, errors.New("agents client not configured")
	}
	agentCtx := outgoingContext(ctx)
	resolved := make(map[uuid.UUID]string, len(agentIDs))
	for _, agentID := range agentIDs {
		resp, err := s.agentsClient.GetAgent(agentCtx, &agentsv1.GetAgentRequest{Id: agentID.String()})
		if err != nil {
			if isNotFoundGrpcError(err) {
				resolved[agentID] = missingNamePlaceholder
				continue
			}
			return nil, fmt.Errorf("get agent %s: %w", agentID, err)
		}
		agent := resp.GetAgent()
		if agent == nil {
			return nil, fmt.Errorf("agent %s not found", agentID)
		}
		resolved[agentID] = agent.GetName()
	}
	return resolved, nil
}

// resolveSandboxNames resolves display names for sandbox runtime owners. The
// Agents service exposes no batch sandbox lookup, so this mirrors
// resolveAgentNames and fetches one sandbox per id.
func (s *Server) resolveSandboxNames(ctx context.Context, sandboxIDs []uuid.UUID) (map[uuid.UUID]string, error) {
	if len(sandboxIDs) == 0 {
		return map[uuid.UUID]string{}, nil
	}
	if s.agentsClient == nil {
		return nil, errors.New("agents client not configured")
	}
	sandboxCtx := outgoingContext(ctx)
	resolved := make(map[uuid.UUID]string, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		resp, err := s.agentsClient.GetSandbox(sandboxCtx, &agentsv1.GetSandboxRequest{Ref: &agentsv1.GetSandboxRequest_Id{Id: sandboxID.String()}})
		if err != nil {
			if isNotFoundGrpcError(err) {
				resolved[sandboxID] = missingNamePlaceholder
				continue
			}
			return nil, fmt.Errorf("get sandbox %s: %w", sandboxID, err)
		}
		sandbox := resp.GetSandbox()
		if sandbox == nil {
			return nil, fmt.Errorf("sandbox %s not found", sandboxID)
		}
		resolved[sandboxID] = sandbox.GetName()
	}
	return resolved, nil
}

func (s *Server) resolveRunnerNames(ctx context.Context, runnerIDs []uuid.UUID) (map[uuid.UUID]string, error) {
	if len(runnerIDs) == 0 {
		return map[uuid.UUID]string{}, nil
	}
	rows, err := s.pool.Query(ctx, "SELECT id, name FROM runners WHERE id = ANY($1)", pgtype.FlatArray[uuid.UUID](runnerIDs))
	if err != nil {
		return nil, fmt.Errorf("list runner names: %w", err)
	}
	defer rows.Close()

	resolved := make(map[uuid.UUID]string, len(runnerIDs))
	for rows.Next() {
		var (
			id   uuid.UUID
			name string
		)
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("scan runner name: %w", err)
		}
		resolved[id] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list runner names: %w", err)
	}
	return resolved, nil
}

func (s *Server) resolveVolumeNames(ctx context.Context, volumeIDs []uuid.UUID) (map[uuid.UUID]string, error) {
	if len(volumeIDs) == 0 {
		return map[uuid.UUID]string{}, nil
	}
	if s.agentsClient == nil {
		return nil, errors.New("agents client not configured")
	}
	volumeCtx := outgoingContext(ctx)
	resolved := make(map[uuid.UUID]string, len(volumeIDs))
	for _, volumeID := range volumeIDs {
		resp, err := s.agentsClient.GetVolume(volumeCtx, &agentsv1.GetVolumeRequest{Id: volumeID.String()})
		if err != nil {
			if isNotFoundGrpcError(err) {
				resolved[volumeID] = missingNamePlaceholder
				continue
			}
			return nil, fmt.Errorf("get volume %s: %w", volumeID, err)
		}
		volume := resp.GetVolume()
		if volume == nil {
			return nil, fmt.Errorf("volume %s not found", volumeID)
		}
		// A volume carries a real name now; description is the deprecated field
		// it used to be labelled by, and the mount path the last resort.
		name := strings.TrimSpace(volume.GetName())
		if name == "" {
			name = strings.TrimSpace(volume.GetDescription())
		}
		if name == "" {
			name = strings.TrimSpace(volume.GetMountPath())
		}
		resolved[volumeID] = name
	}
	return resolved, nil
}

func isNotFoundGrpcError(err error) bool {
	statusErr, ok := status.FromError(err)
	return ok && statusErr.Code() == codes.NotFound
}

func (s *Server) resolveMcpName(ctx context.Context, mcpID uuid.UUID) (string, error) {
	if s.agentsClient == nil {
		return "", errors.New("agents client not configured")
	}
	resp, err := s.agentsClient.GetMcp(outgoingContext(ctx), &agentsv1.GetMcpRequest{Id: mcpID.String()})
	if err != nil {
		return "", fmt.Errorf("get mcp %s: %w", mcpID, err)
	}
	mcp := resp.GetMcp()
	if mcp == nil {
		return "", fmt.Errorf("mcp %s not found", mcpID)
	}
	return mcp.GetName(), nil
}
