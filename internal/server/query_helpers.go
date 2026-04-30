package server

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const pendingSampleClause = "(removed_at IS NULL OR last_metering_sampled_at IS NULL OR removed_at > last_metering_sampled_at)"

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
