package server

import (
	"fmt"
	"strings"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
)

type sampledAtEntry struct {
	ID        uuid.UUID
	SampledAt time.Time
}

func buildBatchSampledAtUpdateQuery(table string, entries []sampledAtEntry) (string, []any) {
	args := make([]any, 0, len(entries)*2)
	query := strings.Builder{}
	query.WriteString("UPDATE ")
	query.WriteString(table)
	query.WriteString(" AS target SET last_metering_sampled_at = v.sampled_at, updated_at = NOW() FROM (VALUES ")
	for i, entry := range entries {
		if i > 0 {
			query.WriteString(", ")
		}
		argPos := len(args) + 1
		fmt.Fprintf(&query, "($%d::uuid, $%d::timestamptz)", argPos, argPos+1)
		args = append(args, entry.ID, entry.SampledAt)
	}
	query.WriteString(") AS v(id, sampled_at) WHERE target.id = v.id")
	return query.String(), args
}

func parseSampledAtEntries(entries []*runnersv1.SampledAtEntry) ([]sampledAtEntry, error) {
	updates := make([]sampledAtEntry, 0, len(entries))
	for i, entry := range entries {
		if entry == nil {
			return nil, fmt.Errorf("entries[%d]: must be provided", i)
		}
		id, err := parseUUID(entry.GetId())
		if err != nil {
			return nil, fmt.Errorf("entries[%d].id: %v", i, err)
		}
		sampledAt := entry.GetSampledAt()
		if sampledAt == nil {
			return nil, fmt.Errorf("entries[%d].sampled_at: must be provided", i)
		}
		if err := sampledAt.CheckValid(); err != nil {
			return nil, fmt.Errorf("entries[%d].sampled_at: %v", i, err)
		}
		updates = append(updates, sampledAtEntry{ID: id, SampledAt: sampledAt.AsTime()})
	}
	return updates, nil
}
