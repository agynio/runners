package server

import (
	"fmt"
	"strings"
	"time"

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
		fmt.Fprintf(&query, "($%d, $%d)", argPos, argPos+1)
		args = append(args, entry.ID, entry.SampledAt)
	}
	query.WriteString(") AS v(id, sampled_at) WHERE target.id = v.id")
	return query.String(), args
}
