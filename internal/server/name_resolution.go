package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	agentsv1 "github.com/agynio/runners/.gen/go/agynio/api/agents/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

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
			return nil, fmt.Errorf("get volume %s: %w", volumeID, err)
		}
		volume := resp.GetVolume()
		if volume == nil {
			return nil, fmt.Errorf("volume %s not found", volumeID)
		}
		name := strings.TrimSpace(volume.GetDescription())
		if name == "" {
			name = strings.TrimSpace(volume.GetMountPath())
		}
		resolved[volumeID] = name
	}
	return resolved, nil
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

func (s *Server) resolveHookName(ctx context.Context, hookID uuid.UUID) (string, error) {
	if s.agentsClient == nil {
		return "", errors.New("agents client not configured")
	}
	resp, err := s.agentsClient.GetHook(outgoingContext(ctx), &agentsv1.GetHookRequest{Id: hookID.String()})
	if err != nil {
		return "", fmt.Errorf("get hook %s: %w", hookID, err)
	}
	hook := resp.GetHook()
	if hook == nil {
		return "", fmt.Errorf("hook %s not found", hookID)
	}
	name := strings.TrimSpace(hook.GetDescription())
	if name == "" {
		name = strings.TrimSpace(hook.GetEvent())
	}
	return name, nil
}
