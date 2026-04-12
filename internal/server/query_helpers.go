package server

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

const pendingSampleClause = "(removed_at IS NULL OR last_metering_sampled_at IS NULL OR removed_at > last_metering_sampled_at)"

type listQueryInput struct {
	Table          string
	Columns        string
	Statuses       []string
	OrganizationID *uuid.UUID
	RunnerID       *uuid.UUID
	PendingSample  bool
	PageToken      string
	Limit          int32
}

func addUpdateClause(clauses *[]string, args *[]any, column string, value any) {
	*clauses = append(*clauses, fmt.Sprintf("%s = $%d", column, len(*args)+1))
	*args = append(*args, value)
}

func buildUpdateQuery(table, columns string, clauses []string, args []any, id uuid.UUID) (string, []any) {
	clauses = append(clauses, "updated_at = NOW()")
	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d RETURNING %s", table, strings.Join(clauses, ", "), len(args)+1, columns)
	args = append(args, id)
	return query, args
}

func buildListQuery(input listQueryInput) (string, []any, error) {
	var (
		clauses []string
		args    []any
	)
	if len(input.Statuses) > 0 {
		clauses = append(clauses, fmt.Sprintf("status = ANY($%d)", len(args)+1))
		args = append(args, pgtype.FlatArray[string](input.Statuses))
	}
	if input.OrganizationID != nil {
		clauses = append(clauses, fmt.Sprintf("organization_id = $%d", len(args)+1))
		args = append(args, *input.OrganizationID)
	}
	if input.RunnerID != nil {
		clauses = append(clauses, fmt.Sprintf("runner_id = $%d", len(args)+1))
		args = append(args, *input.RunnerID)
	}
	if input.PendingSample {
		clauses = append(clauses, pendingSampleClause)
	}
	if input.PageToken != "" {
		afterID, err := decodePageToken(input.PageToken)
		if err != nil {
			return "", nil, InvalidPageToken(err)
		}
		clauses = append(clauses, fmt.Sprintf("id > $%d", len(args)+1))
		args = append(args, afterID)
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT %s FROM %s", input.Columns, input.Table))
	if len(clauses) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(clauses, " AND "))
	}
	query.WriteString(fmt.Sprintf(" ORDER BY id ASC LIMIT $%d", len(args)+1))
	args = append(args, int(input.Limit)+1)

	return query.String(), args, nil
}
