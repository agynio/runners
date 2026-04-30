package server

import (
	"context"
	"fmt"
	"log"
	"time"
)

func (s *Server) RunWorkloadActivitySweep(ctx context.Context, interval, keepaliveGrace time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.sweepWorkloadActivity(ctx, time.Now().UTC(), keepaliveGrace); err != nil {
				log.Printf("runners: workload activity sweep: %v", err)
			}
		}
	}
}

func (s *Server) sweepWorkloadActivity(ctx context.Context, now time.Time, keepaliveGrace time.Duration) error {
	cutoff := now.Add(-keepaliveGrace)
	workloads, err := s.setIdleWorkloads(ctx, cutoff)
	if err != nil {
		return err
	}
	for _, workload := range workloads {
		s.publishWorkloadUpdateNotifications(ctx, workload, false, false, false, true)
	}
	return nil
}

func (s *Server) setIdleWorkloads(ctx context.Context, cutoff time.Time) ([]workloadRecord, error) {
	query := fmt.Sprintf("UPDATE workloads SET agent_state = $1, updated_at = NOW() WHERE status = $2 AND agent_state = $3 AND last_activity_at < $4 AND removed_at IS NULL RETURNING %s", workloadColumns)
	rows, err := s.pool.Query(ctx, query, workloadAgentStateIdle, workloadStatusRunning, workloadAgentStateProcessing, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	workloads := []workloadRecord{}
	for rows.Next() {
		workload, err := scanWorkload(rows)
		if err != nil {
			return nil, err
		}
		workloads = append(workloads, workload)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return workloads, nil
}
