package server

import (
	"context"
	"fmt"
	"log"
	"time"
)

// RunAgentActivitySweep flips workloads from agent_state=processing to agent_state=idle
// when they have not received a keepalive within KEEPALIVE_GRACE.
//
// This is the sole writer of the processing -> idle transition.
func (s *Server) RunAgentActivitySweep(ctx context.Context, interval, keepaliveGrace time.Duration) {
	if interval <= 0 {
		log.Printf("runners: agent activity sweep disabled: interval=%s", interval)
		return
	}
	if keepaliveGrace <= 0 {
		log.Printf("runners: agent activity sweep disabled: keepalive_grace=%s", keepaliveGrace)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if err := s.agentActivitySweepOnce(ctx, keepaliveGrace); err != nil {
			log.Printf("runners: agent activity sweep: %v", err)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) agentActivitySweepOnce(ctx context.Context, keepaliveGrace time.Duration) error {
	cutoff := time.Now().Add(-keepaliveGrace)
	query := fmt.Sprintf(`
		UPDATE workloads
		SET agent_state = $1,
			updated_at = NOW()
		WHERE status = $2
			AND agent_state = $3
			AND removed_at IS NULL
			AND last_activity_at < $4
		RETURNING %s`, workloadColumns)

	rows, err := s.pool.Query(ctx, query, workloadAgentStateIdle, workloadStatusRunning, workloadAgentStateProcessing, cutoff)
	if err != nil {
		return err
	}
	defer rows.Close()

	var updated []workloadRecord
	for rows.Next() {
		record, err := scanWorkload(rows)
		if err != nil {
			return err
		}
		updated = append(updated, record)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if s.notificationsClient == nil {
		return nil
	}
	for _, workload := range updated {
		s.publishWorkloadUpdateNotifications(ctx, workload, false, false, false, true)
	}
	return nil
}
